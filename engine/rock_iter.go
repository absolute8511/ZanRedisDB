package engine

import (
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

type rockIterator struct {
	*gorocksdb.Iterator
	snap         *gorocksdb.Snapshot
	ro           *gorocksdb.ReadOptions
	db           *gorocksdb.DB
	upperBound   *gorocksdb.IterBound
	lowerBound   *gorocksdb.IterBound
	removeTsType byte
}

// low_bound is inclusive
// upper bound is exclusive
func newRockIterator(db *gorocksdb.DB,
	prefixSame bool, opts IteratorOpts) (*rockIterator, error) {
	db.RLock()
	if !db.IsOpened() {
		db.RUnlock()
		return nil, common.ErrStopped
	}
	upperBound := opts.Max
	lowerBound := opts.Min
	if opts.Type&common.RangeROpen <= 0 && upperBound != nil {
		// range right not open, we need inclusive the max,
		// however upperBound is exclusive
		upperBound = append(upperBound, 0)
	}
	dbit := &rockIterator{
		db: db,
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	readOpts.SetFillCache(false)
	readOpts.SetVerifyChecksums(false)
	if prefixSame {
		readOpts.SetPrefixSameAsStart(true)
	}
	if lowerBound != nil {
		dbit.lowerBound = gorocksdb.NewIterBound(lowerBound)
		readOpts.SetIterLowerBound(dbit.lowerBound)
	}
	if upperBound != nil {
		dbit.upperBound = gorocksdb.NewIterBound(upperBound)
		readOpts.SetIterUpperBound(dbit.upperBound)
	}
	if opts.IgnoreDel {
		// may iterator some deleted keys still not compacted.
		readOpts.SetIgnoreRangeDeletions(true)
	}
	dbit.ro = readOpts
	var err error
	if opts.WithSnap {
		dbit.snap, err = db.NewSnapshot()
		if err != nil {
			dbit.Close()
			return nil, err
		}
		readOpts.SetSnapshot(dbit.snap)
	}
	dbit.Iterator, err = db.NewIterator(readOpts)
	if err != nil {
		dbit.Close()
		return nil, err
	}
	return dbit, nil
}

// the bytes returned will be freed after next
func (it *rockIterator) RefKey() []byte {
	return it.Iterator.Key().Data()
}

func (it *rockIterator) Key() []byte {
	return it.Iterator.Key().Bytes()
}

// the bytes returned will be freed after next
func (it *rockIterator) RefValue() []byte {
	v := it.Iterator.Value().Data()
	if (it.removeTsType == KVType || it.removeTsType == HashType) && len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v
}

func (it *rockIterator) Value() []byte {
	v := it.Iterator.Value().Bytes()
	if (it.removeTsType == KVType || it.removeTsType == HashType) && len(v) >= tsLen {
		v = v[:len(v)-tsLen]
	}
	return v
}

func (it *rockIterator) NoTimestamp(vt byte) {
	it.removeTsType = vt
}

func (it *rockIterator) Close() {
	if it.Iterator != nil {
		it.Iterator.Close()
	}
	if it.ro != nil {
		it.ro.Destroy()
	}
	if it.snap != nil {
		it.snap.Release()
	}
	if it.upperBound != nil {
		it.upperBound.Destroy()
	}
	if it.lowerBound != nil {
		it.lowerBound.Destroy()
	}
	it.db.RUnlock()
}
