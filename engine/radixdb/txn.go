package radixdb

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	iradix "github.com/hashicorp/go-immutable-radix"
)

const (
	id    = "id"
	table = "default"
)

var (
	// ErrNotFound is returned when the requested item is not found
	ErrNotFound = fmt.Errorf("not found")
)

// tableIndex is a tuple of (Table, Index) used for lookups
type tableIndex struct {
	Table string
	Index string
}

type dbItem struct {
	Key   []byte
	Value []byte
}

func KVFromObject(obj interface{}) ([]byte, []byte, error) {
	item, ok := obj.(*dbItem)
	if !ok {
		return nil, nil, errors.New("unknown data type")
	}
	return item.Key, item.Value, nil
}

// return the key from obj
func FromObject(obj interface{}) ([]byte, error) {
	item, ok := obj.(*dbItem)
	if !ok {
		return nil, errors.New("unknown data")
	}
	return item.Key, nil
}

func toIndexKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	key = append(key, '\x00')
	return key
}

// Txn is a transaction against a MemDB.
// This can be a read or write transaction.
type Txn struct {
	db      *MemDB
	write   bool
	rootTxn *iradix.Txn
	after   []func()

	// changes is used to track the changes performed during the transaction. If
	// it is nil at transaction start then changes are not tracked.
	changes Changes

	modified *iradix.Txn
}

// readableIndex returns a transaction usable for reading the given index in a
// table. If the transaction is a write transaction with modifications, a clone of the
// modified index will be returned.
func (txn *Txn) readableIndex(table, index string) *iradix.Txn {
	// Look for existing transaction
	if txn.write && txn.modified != nil {
		return txn.modified.Clone()
	}

	// Create a read transaction
	path := indexPath(table, index)
	raw, _ := txn.rootTxn.Get(path)
	indexTxn := raw.(*iradix.Tree).Txn()
	return indexTxn
}

// writableIndex returns a transaction usable for modifying the
// given index in a table.
func (txn *Txn) writableIndex(table, index string) *iradix.Txn {
	if txn.modified != nil {
		return txn.modified
	}

	// Start a new transaction
	path := indexPath(table, index)
	raw, _ := txn.rootTxn.Get(path)
	indexTxn := raw.(*iradix.Tree).Txn()

	// If we are the primary DB, enable mutation tracking. Snapshots should
	// not notify, otherwise we will trigger watches on the primary DB when
	// the writes will not be visible.
	indexTxn.TrackMutate(txn.db.primary)

	// Keep this open for the duration of the txn
	txn.modified = indexTxn
	return indexTxn
}

// Abort is used to cancel this transaction.
// This is a noop for read transactions.
func (txn *Txn) Abort() {
	// Noop for a read transaction
	if !txn.write {
		return
	}

	// Check if already aborted or committed
	if txn.rootTxn == nil {
		return
	}

	// Clear the txn
	txn.rootTxn = nil
	txn.modified = nil
	txn.changes = nil

	// Release the writer lock since this is invalid
	txn.db.writer.Unlock()
}

// Commit is used to finalize this transaction.
// This is a noop for read transactions.
func (txn *Txn) Commit() {
	// Noop for a read transaction
	if !txn.write {
		return
	}

	// Check if already aborted or committed
	if txn.rootTxn == nil {
		return
	}

	// Commit each sub-transaction scoped to (table, index)
	if txn.modified != nil {
		path := indexPath(table, id)
		final := txn.modified.CommitOnly()
		txn.rootTxn.Insert(path, final)
	}

	// Update the root of the DB
	newRoot := txn.rootTxn.CommitOnly()
	atomic.StorePointer(&txn.db.root, unsafe.Pointer(newRoot))

	// Now issue all of the mutation updates (this is safe to call
	// even if mutation tracking isn't enabled); we do this after
	// the root pointer is swapped so that waking responders will
	// see the new state.
	if txn.modified != nil {
		txn.modified.Notify()
	}
	txn.rootTxn.Notify()

	// Clear the txn
	txn.rootTxn = nil
	txn.modified = nil

	// Release the writer lock since this is invalid
	txn.db.writer.Unlock()

	// Run the deferred functions, if any
	for i := len(txn.after); i > 0; i-- {
		fn := txn.after[i-1]
		fn()
	}
}

// Insert is used to add or update an object into the given table
func (txn *Txn) Insert(key []byte, value []byte) error {
	if !txn.write {
		return fmt.Errorf("cannot insert in read-only transaction")
	}

	idVal := toIndexKey(key)
	// Lookup the object by ID first, to see if this is an update
	idTxn := txn.writableIndex(table, id)
	existing, _ := idTxn.Get(idVal)

	// Update the value of the index
	obj := &dbItem{Key: key, Value: value}
	idTxn.Insert(idVal, obj)
	if txn.changes != nil {
		txn.changes = append(txn.changes, Change{
			Table:      table,
			Before:     existing, // might be nil on a create
			After:      obj,
			primaryKey: idVal,
		})
	}
	return nil
}

// Delete is used to delete a single object from the given table
// This object must already exist in the table
func (txn *Txn) Delete(key []byte) error {
	if !txn.write {
		return fmt.Errorf("cannot delete in read-only transaction")
	}

	// Lookup the object by ID first, check fi we should continue
	idTxn := txn.writableIndex(table, id)
	idVal := toIndexKey(key)
	existing, ok := idTxn.Get(idVal)
	if !ok {
		return ErrNotFound
	}

	idTxn.Delete(idVal)
	if txn.changes != nil {
		txn.changes = append(txn.changes, Change{
			Table:      table,
			Before:     existing,
			After:      nil, // Now nil indicates deletion
			primaryKey: idVal,
		})
	}
	return nil
}

// DeletePrefix is used to delete an entire subtree based on a prefix.
// The given index must be a prefix index, and will be used to perform a scan and enumerate the set of objects to delete.
// These will be removed from all other indexes, and then a special prefix operation will delete the objects from the given index in an efficient subtree delete operation.
// This is useful when you have a very large number of objects indexed by the given index, along with a much smaller number of entries in the other indexes for those objects.
func (txn *Txn) DeletePrefix(prefix []byte) (bool, error) {
	if !txn.write {
		return false, fmt.Errorf("cannot delete in read-only transaction")
	}

	// Get an iterator over all of the keys with the given prefix.
	entries, err := txn.Get(prefix)
	if err != nil {
		return false, fmt.Errorf("failed kvs lookup: %s", err)
	}

	foundAny := false
	for entry := entries.Next(); entry != nil; entry = entries.Next() {
		if !foundAny {
			foundAny = true
		}
		// Get the primary ID of the object
		idVal, err := FromObject(entry)
		if err != nil {
			return false, fmt.Errorf("failed to build primary index: %v", err)
		}
		if txn.changes != nil {
			// Record the deletion
			idTxn := txn.writableIndex(table, id)
			existing, ok := idTxn.Get(idVal)
			if ok {
				txn.changes = append(txn.changes, Change{
					Table:      table,
					Before:     existing,
					After:      nil, // Now nil indicates deletion
					primaryKey: idVal,
				})
			}
		}
	}
	if foundAny {
		indexTxn := txn.writableIndex(table, id)
		ok := indexTxn.DeletePrefix(prefix)
		if !ok {
			panic(fmt.Errorf("prefix %v matched some entries but DeletePrefix did not delete any ", prefix))
		}
		return true, nil
	}
	return false, nil
}

// DeleteAll is used to delete all the objects in a given table
// matching the constraints on the index
func (txn *Txn) DeleteAll() (int, error) {
	if !txn.write {
		return 0, fmt.Errorf("cannot delete in read-only transaction")
	}

	// Get all the objects
	iter, err := txn.Get(nil)
	if err != nil {
		return 0, err
	}

	// Put them into a slice so there are no safety concerns while actually
	// performing the deletes
	var objs []interface{}
	for {
		obj := iter.Next()
		if obj == nil {
			break
		}

		objs = append(objs, obj)
	}

	// Do the deletes
	num := 0
	for _, obj := range objs {
		key, err := FromObject(obj)
		if err != nil {
			return num, err
		}
		// the key in object has no \x00
		if err := txn.Delete(key); err != nil {
			return num, err
		}
		num++
	}
	return num, nil
}

// FirstWatch is used to return the first matching object for
// the given constraints on the index along with the watch channel
func (txn *Txn) FirstWatch(key []byte) (<-chan struct{}, interface{}, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(table, id)

	key = toIndexKey(key)
	// Do an exact lookup
	if key != nil {
		watch, obj, ok := indexTxn.GetWatch(key)
		if !ok {
			return watch, nil, nil
		}
		return watch, obj, nil
	}

	// Handle non-unique index by using an iterator and getting the first value
	iter := indexTxn.Root().Iterator()
	watch := iter.SeekPrefixWatch(key)
	_, value, _ := iter.Next()
	return watch, value, nil
}

// LastWatch is used to return the last matching object for
// the given constraints on the index along with the watch channel
func (txn *Txn) LastWatch(key []byte) (<-chan struct{}, interface{}, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(table, id)

	key = toIndexKey(key)
	// Do an exact lookup
	if key != nil {
		watch, obj, ok := indexTxn.GetWatch(key)
		if !ok {
			return watch, nil, nil
		}
		return watch, obj, nil
	}

	// Handle non-unique index by using an iterator and getting the last value
	iter := indexTxn.Root().ReverseIterator()
	watch := iter.SeekPrefixWatch(key)
	_, value, _ := iter.Previous()
	return watch, value, nil
}

// First is used to return the first matching object for
// the given constraints on the index
func (txn *Txn) First(key []byte) (interface{}, error) {
	_, val, err := txn.FirstWatch(key)
	return val, err
}

// Last is used to return the last matching object for
// the given constraints on the index
func (txn *Txn) Last(key []byte) (interface{}, error) {
	_, val, err := txn.LastWatch(key)
	return val, err
}

// LongestPrefix is used to fetch the longest prefix match for the given
// constraints on the index. Note that this will not work with the memdb
// StringFieldIndex because it adds null terminators which prevent the
// algorithm from correctly finding a match (it will get to right before the
// null and fail to find a leaf node). This should only be used where the prefix
// given is capable of matching indexed entries directly, which typically only
// applies to a custom indexer. See the unit test for an example.
func (txn *Txn) LongestPrefix(key []byte) (interface{}, error) {
	// note prefix should use the key without the trailing \x00
	// Find the longest prefix match with the given index.
	indexTxn := txn.readableIndex(table, id)
	if _, value, ok := indexTxn.Root().LongestPrefix(key); ok {
		return value, nil
	}
	return nil, nil
}

// ResultIterator is used to iterate over a list of results from a query on a table.
//
// When a ResultIterator is created from a write transaction, the results from
// Next will reflect a snapshot of the table at the time the ResultIterator is
// created.
// This means that calling Insert or Delete on a transaction while iterating is
// allowed, but the changes made by Insert or Delete will not be observed in the
// results returned from subsequent calls to Next. For example if an item is deleted
// from the index used by the iterator it will still be returned by Next. If an
// item is inserted into the index used by the iterator, it will not be returned
// by Next. However, an iterator created after a call to Insert or Delete will
// reflect the modifications.
//
// When a ResultIterator is created from a write transaction, and there are already
// modifications to the index used by the iterator, the modification cache of the
// index will be invalidated. This may result in some additional allocations if
// the same node in the index is modified again.
type ResultIterator interface {
	WatchCh() <-chan struct{}
	// Next returns the next result from the iterator. If there are no more results
	// nil is returned.
	Next() interface{}
}

// Get is used to construct a ResultIterator over all the rows that match the
// given constraints of an index.
//
// See the documentation for ResultIterator to understand the behaviour of the
// returned ResultIterator.
func (txn *Txn) Get(key []byte) (ResultIterator, error) {
	indexIter, val, err := txn.getIndexIterator(key)
	if err != nil {
		return nil, err
	}

	// Seek the iterator to the appropriate sub-set
	watchCh := indexIter.SeekPrefixWatch(val)

	// Create an iterator
	iter := &radixIterator{
		iter:    indexIter,
		watchCh: watchCh,
	}
	return iter, nil
}

// GetReverse is used to construct a Reverse ResultIterator over all the
// rows that match the given constraints of an index.
// The returned ResultIterator's Next() will return the next Previous value.
//
// See the documentation for ResultIterator to understand the behaviour of the
// returned ResultIterator.
func (txn *Txn) GetReverse(key []byte) (ResultIterator, error) {
	indexIter, val, err := txn.getIndexIteratorReverse(key)
	if err != nil {
		return nil, err
	}

	// Seek the iterator to the appropriate sub-set
	watchCh := indexIter.SeekPrefixWatch(val)

	// Create an iterator
	iter := &radixReverseIterator{
		iter:    indexIter,
		watchCh: watchCh,
	}
	return iter, nil
}

// LowerBound is used to construct a ResultIterator over all the the range of
// rows that have an index value greater than or equal to the provide args.
// Calling this then iterating until the rows are larger than required allows
// range scans within an index. It is not possible to watch the resulting
// iterator since the radix tree doesn't efficiently allow watching on lower
// bound changes. The WatchCh returned will be nill and so will block forever.
//
// See the documentation for ResultIterator to understand the behaviour of the
// returned ResultIterator.
func (txn *Txn) LowerBound(key []byte) (ResultIterator, error) {
	indexIter, val, err := txn.getIndexIterator(key)
	if err != nil {
		return nil, err
	}

	// Seek the iterator to the appropriate sub-set
	indexIter.SeekLowerBound(val)

	// Create an iterator
	iter := &radixIterator{
		iter: indexIter,
	}
	return iter, nil
}

// ReverseLowerBound is used to construct a Reverse ResultIterator over all the
// the range of rows that have an index value less than or equal to the
// provide args.  Calling this then iterating until the rows are lower than
// required allows range scans within an index. It is not possible to watch the
// resulting iterator since the radix tree doesn't efficiently allow watching
// on lower bound changes. The WatchCh returned will be nill and so will block
// forever.
//
// See the documentation for ResultIterator to understand the behaviour of the
// returned ResultIterator.
func (txn *Txn) ReverseLowerBound(key []byte) (ResultIterator, error) {
	indexIter, val, err := txn.getIndexIteratorReverse(key)
	if err != nil {
		return nil, err
	}

	// Seek the iterator to the appropriate sub-set
	indexIter.SeekReverseLowerBound(val)

	// Create an iterator
	iter := &radixReverseIterator{
		iter: indexIter,
	}
	return iter, nil
}

// objectID is a tuple of table name and the raw internal id byte slice
// converted to a string. It's only converted to a string to make it comparable
// so this struct can be used as a map index.
type objectID struct {
	Table    string
	IndexVal string
}

// mutInfo stores metadata about mutations to allow collapsing multiple
// mutations to the same object into one.
type mutInfo struct {
	firstBefore interface{}
	lastIdx     int
}

// Changes returns the set of object changes that have been made in the
// transaction so far. If change tracking is not enabled it wil always return
// nil. It can be called before or after Commit. If it is before Commit it will
// return all changes made so far which may not be the same as the final
// Changes. After abort it will always return nil. As with other Txn methods
// it's not safe to call this from a different goroutine than the one making
// mutations or committing the transaction. Mutations will appear in the order
// they were performed in the transaction but multiple operations to the same
// object will be collapsed so only the effective overall change to that object
// is present. If transaction operations are dependent (e.g. copy object X to Y
// then delete X) this might mean the set of mutations is incomplete to verify
// history, but it is complete in that the net effect is preserved (Y got a new
// value, X got removed).
func (txn *Txn) Changes() Changes {
	if txn.changes == nil {
		return nil
	}

	// De-duplicate mutations by key so all take effect at the point of the last
	// write but we keep the mutations in order.
	dups := make(map[objectID]mutInfo)
	for i, m := range txn.changes {
		oid := objectID{
			Table:    m.Table,
			IndexVal: string(m.primaryKey),
		}
		// Store the latest mutation index for each key value
		mi, ok := dups[oid]
		if !ok {
			// First entry for key, store the before value
			mi.firstBefore = m.Before
		}
		mi.lastIdx = i
		dups[oid] = mi
	}
	if len(dups) == len(txn.changes) {
		// No duplicates found, fast path return it as is
		return txn.changes
	}

	// Need to remove the duplicates
	cs := make(Changes, 0, len(dups))
	for i, m := range txn.changes {
		oid := objectID{
			Table:    m.Table,
			IndexVal: string(m.primaryKey),
		}
		mi := dups[oid]
		if mi.lastIdx == i {
			// This was the latest value for this key copy it with the before value in
			// case it's different. Note that m is not a pointer so we are not
			// modifying the txn.changeSet here - it's already a copy.
			m.Before = mi.firstBefore

			// Edge case - if the object was inserted and then eventually deleted in
			// the same transaction, then the net affect on that key is a no-op. Don't
			// emit a mutation with nil for before and after as it's meaningless and
			// might violate expectations and cause a panic in code that assumes at
			// least one must be set.
			if m.Before == nil && m.After == nil {
				continue
			}
			cs = append(cs, m)
		}
	}
	// Store the de-duped version in case this is called again
	txn.changes = cs
	return cs
}

func (txn *Txn) getIndexIterator(key []byte) (*iradix.Iterator, []byte, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(table, id)
	indexRoot := indexTxn.Root()

	// Get an iterator over the index
	indexIter := indexRoot.Iterator()
	return indexIter, toIndexKey(key), nil
}

func (txn *Txn) getIndexIteratorReverse(key []byte) (*iradix.ReverseIterator, []byte, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(table, id)
	indexRoot := indexTxn.Root()

	// Get an interator over the index
	indexIter := indexRoot.ReverseIterator()
	return indexIter, toIndexKey(key), nil
}

// Defer is used to push a new arbitrary function onto a stack which
// gets called when a transaction is committed and finished. Deferred
// functions are called in LIFO order, and only invoked at the end of
// write transactions.
func (txn *Txn) Defer(fn func()) {
	txn.after = append(txn.after, fn)
}

// radixIterator is used to wrap an underlying iradix iterator.
// This is much more efficient than a sliceIterator as we are not
// materializing the entire view.
type radixIterator struct {
	iter    *iradix.Iterator
	watchCh <-chan struct{}
}

func (r *radixIterator) WatchCh() <-chan struct{} {
	return r.watchCh
}

func (r *radixIterator) Next() interface{} {
	_, value, ok := r.iter.Next()
	if !ok {
		return nil
	}
	return value
}

type radixReverseIterator struct {
	iter    *iradix.ReverseIterator
	watchCh <-chan struct{}
}

func (r *radixReverseIterator) Next() interface{} {
	_, value, ok := r.iter.Previous()
	if !ok {
		return nil
	}
	return value
}

func (r *radixReverseIterator) WatchCh() <-chan struct{} {
	return r.watchCh
}

// Snapshot creates a snapshot of the current state of the transaction.
// Returns a new read-only transaction or nil if the transaction is already
// aborted or committed.
func (txn *Txn) Snapshot() *Txn {
	if txn.rootTxn == nil {
		return nil
	}

	snapshot := &Txn{
		db:      txn.db,
		rootTxn: txn.rootTxn.Clone(),
	}

	// Commit sub-transactions into the snapshot
	if txn.modified != nil {
		path := indexPath(table, id)
		final := txn.modified.CommitOnly()
		snapshot.rootTxn.Insert(path, final)
	}

	return snapshot
}
