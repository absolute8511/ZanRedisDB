package radixdb

import (
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	iradix "github.com/hashicorp/go-immutable-radix"
)

const (
	id       = "id"
	defTable = "default"
)

var (
	// ErrNotFound is returned when the requested item is not found
	ErrNotFound = fmt.Errorf("not found")
)

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

func extractFromIndexKey(key []byte) []byte {
	if len(key) == 0 {
		return key
	}
	return key[:len(key)-1]
}

// Txn is a transaction against a MemDB.
// This can be a read or write transaction.
type Txn struct {
	db      *MemDB
	write   bool
	rootTxn *iradix.Txn
	after   []func()

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
		path := indexPath(defTable, id)
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
	idTxn := txn.writableIndex(defTable, id)
	// Update the value of the index
	// we use nil for Key, since we can get it from radix index, so we avoid save it in value
	obj := &dbItem{Key: nil, Value: value}
	idTxn.Insert(idVal, obj)
	return nil
}

// Delete is used to delete a single object from the given table
// This object must already exist in the table
func (txn *Txn) Delete(key []byte) error {
	if !txn.write {
		return fmt.Errorf("cannot delete in read-only transaction")
	}

	// Lookup the object by ID first, check fi we should continue
	idTxn := txn.writableIndex(defTable, id)
	idVal := toIndexKey(key)
	idTxn.Delete(idVal)

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
	indexTxn := txn.writableIndex(defTable, id)
	ok := indexTxn.DeletePrefix(prefix)
	return ok, nil
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
	var keys [][]byte
	for {
		k, _ := iter.Next()
		if k == nil {
			break
		}

		keys = append(keys, k)
	}

	// Do the deletes
	num := 0
	for _, key := range keys {
		// the key from ResultIterate.Next() has no \x00
		if err := txn.Delete(key); err != nil {
			return num, err
		}
		num++
	}
	return num, nil
}

// FirstWatch is used to return the first matching object for
// the given constraints on the index along with the watch channel
func (txn *Txn) FirstWatch(key []byte) (<-chan struct{}, []byte, interface{}, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(defTable, id)

	key = toIndexKey(key)
	// Do an exact lookup
	if key != nil {
		watch, obj, ok := indexTxn.GetWatch(key)
		if !ok {
			return watch, nil, nil, nil
		}
		return watch, nil, obj, nil
	}

	// Handle non-unique index by using an iterator and getting the first value
	iter := indexTxn.Root().Iterator()
	watch := iter.SeekPrefixWatch(key)
	k, value, _ := iter.Next()
	return watch, extractFromIndexKey(k), value, nil
}

// LastWatch is used to return the last matching object for
// the given constraints on the index along with the watch channel
func (txn *Txn) LastWatch(key []byte) (<-chan struct{}, []byte, interface{}, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(defTable, id)

	key = toIndexKey(key)
	// Do an exact lookup
	if key != nil {
		watch, obj, ok := indexTxn.GetWatch(key)
		if !ok {
			return watch, nil, nil, nil
		}
		return watch, nil, obj, nil
	}

	// Handle non-unique index by using an iterator and getting the last value
	iter := indexTxn.Root().ReverseIterator()
	watch := iter.SeekPrefixWatch(key)
	k, value, _ := iter.Previous()
	return watch, extractFromIndexKey(k), value, nil
}

// First is used to return the first matching object for
// the given constraints on the index
func (txn *Txn) First(key []byte) ([]byte, interface{}, error) {
	_, k, val, err := txn.FirstWatch(key)
	return k, val, err
}

// Last is used to return the last matching object for
// the given constraints on the index
func (txn *Txn) Last(key []byte) ([]byte, interface{}, error) {
	_, k, val, err := txn.LastWatch(key)
	return k, val, err
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
	indexTxn := txn.readableIndex(defTable, id)
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
	Next() ([]byte, interface{})
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

func (txn *Txn) getIndexIterator(key []byte) (*iradix.Iterator, []byte, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(defTable, id)
	indexRoot := indexTxn.Root()

	// Get an iterator over the index
	indexIter := indexRoot.Iterator()
	return indexIter, toIndexKey(key), nil
}

func (txn *Txn) getIndexIteratorReverse(key []byte) (*iradix.ReverseIterator, []byte, error) {
	// Get the index itself
	indexTxn := txn.readableIndex(defTable, id)
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

func (r *radixIterator) Next() ([]byte, interface{}) {
	k, value, ok := r.iter.Next()
	if !ok {
		return nil, nil
	}
	return extractFromIndexKey(k), value
}

type radixReverseIterator struct {
	iter    *iradix.ReverseIterator
	watchCh <-chan struct{}
}

func (r *radixReverseIterator) Next() ([]byte, interface{}) {
	k, value, ok := r.iter.Previous()
	if !ok {
		return nil, nil
	}
	return extractFromIndexKey(k), value
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
		path := indexPath(defTable, id)
		final := txn.modified.CommitOnly()
		snapshot.rootTxn.Insert(path, final)
	}

	return snapshot
}
