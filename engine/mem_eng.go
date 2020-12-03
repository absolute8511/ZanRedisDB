package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/youzan/ZanRedisDB/common"
)

const (
	defBucket = "default"
)

var useSkiplist = true

type memRefSlice struct {
	b        []byte
	needCopy bool
}

func (rs *memRefSlice) Free() {
}

// ref data
func (rs *memRefSlice) Data() []byte {
	return rs.b
}

// copied data if need
func (rs *memRefSlice) Bytes() []byte {
	if !rs.needCopy || rs.b == nil {
		return rs.b
	}
	d := make([]byte, len(rs.b))
	copy(d, rs.b)
	return d
}

type sharedMemConfig struct {
}

func newSharedMemConfig(opt RockOptions) *sharedMemConfig {
	sc := &sharedMemConfig{}
	return sc
}

func (sc *sharedMemConfig) ChangeLimiter(bytesPerSec int64) {
}

func (sc *sharedMemConfig) Destroy() {
}

type memEng struct {
	rwmutex     sync.RWMutex
	cfg         *RockEngConfig
	eng         *btree
	slEng       *skipList
	engOpened   int32
	lastCompact int64
	deletedCnt  int64
	quit        chan struct{}
}

func NewMemEng(cfg *RockEngConfig) (*memEng, error) {
	if len(cfg.DataDir) == 0 {
		return nil, errors.New("config error")
	}

	err := os.MkdirAll(cfg.DataDir, common.DIR_PERM)
	if err != nil {
		return nil, err
	}

	if !cfg.DisableMergeCounter {
		if cfg.EnableTableCounter {
			// do merger
		}
	} else {
		cfg.EnableTableCounter = false
	}
	db := &memEng{
		cfg:  cfg,
		quit: make(chan struct{}),
	}
	if cfg.AutoCompacted {
		go db.compactLoop()
	}

	return db, nil
}

func (me *memEng) NewWriteBatch() WriteBatch {
	wb, err := newMemWriteBatch(me)
	if err != nil {
		return nil
	}
	return wb
}

func (me *memEng) DefaultWriteBatch() WriteBatch {
	return me.NewWriteBatch()
}

func (me *memEng) GetDataDir() string {
	return path.Join(me.cfg.DataDir, "mem")
}

func (me *memEng) SetCompactionFilter(ICompactFilter) {
}
func (me *memEng) SetMaxBackgroundOptions(maxCompact int, maxBackJobs int) error {
	return nil
}

func (me *memEng) compactLoop() {
}

func (me *memEng) CheckDBEngForRead(fullPath string) error {
	return nil
}

func (me *memEng) getDataFileName() string {
	return path.Join(me.GetDataDir(), "mem.dat")
}

func (me *memEng) OpenEng() error {
	if !me.IsClosed() {
		dbLog.Warningf("engine already opened: %v, should close it before reopen", me.GetDataDir())
		return errors.New("open failed since not closed")
	}
	me.rwmutex.Lock()
	defer me.rwmutex.Unlock()
	os.MkdirAll(me.GetDataDir(), common.DIR_PERM)
	if useSkiplist {
		sleng := NewSkipList()
		err := loadMemDBFromFile(me.getDataFileName(), func(key []byte, value []byte) error {
			return sleng.Set(key, value)
		})
		if err != nil {
			return err
		}
		me.slEng = sleng
	} else {
		eng := &btree{
			cmp: cmpItem,
		}
		err := loadMemDBFromFile(me.getDataFileName(), func(key []byte, value []byte) error {
			item := &kvitem{
				key:   make([]byte, len(key)),
				value: make([]byte, len(value)),
			}
			copy(item.key, key)
			copy(item.value, value)
			eng.Set(item)
			return nil
		})
		if err != nil {
			return err
		}
		me.eng = eng
	}

	atomic.StoreInt32(&me.engOpened, 1)
	dbLog.Infof("engine opened: %v", me.GetDataDir())
	return nil
}

func (me *memEng) Write(wb WriteBatch) error {
	return wb.Commit()
}

func (me *memEng) DeletedBeforeCompact() int64 {
	return atomic.LoadInt64(&me.deletedCnt)
}

func (me *memEng) AddDeletedCnt(c int64) {
	atomic.AddInt64(&me.deletedCnt, c)
}

func (me *memEng) LastCompactTime() int64 {
	return atomic.LoadInt64(&me.lastCompact)
}

func (me *memEng) CompactRange(rg CRange) {
}

func (me *memEng) CompactAllRange() {
	me.CompactRange(CRange{})
}

func (me *memEng) GetApproximateTotalKeyNum() int {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	if useSkiplist {
		return int(me.slEng.Len())
	}
	return me.eng.Len()
}

func (me *memEng) GetApproximateKeyNum(ranges []CRange) uint64 {
	return 0
}

func (me *memEng) SetOptsForLogStorage() {
	return
}

func (me *memEng) GetApproximateSizes(ranges []CRange, includeMem bool) []uint64 {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	sizeList := make([]uint64, len(ranges))
	if me.IsClosed() {
		return sizeList
	}
	// TODO: estimate the size
	return sizeList
}

func (me *memEng) IsClosed() bool {
	if atomic.LoadInt32(&me.engOpened) == 0 {
		return true
	}
	return false
}

func (me *memEng) CloseEng() bool {
	me.rwmutex.Lock()
	defer me.rwmutex.Unlock()
	if atomic.CompareAndSwapInt32(&me.engOpened, 1, 0) {
		if useSkiplist && me.slEng != nil {
			me.slEng.Destroy()
		} else if me.eng != nil {
			me.eng.Destroy()
		}
		dbLog.Infof("engine closed: %v", me.GetDataDir())
		return true
	}
	return false
}

func (me *memEng) CloseAll() {
	select {
	case <-me.quit:
	default:
		close(me.quit)
	}
	me.CloseEng()
}

func (me *memEng) GetStatistics() string {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	if me.IsClosed() {
		return ""
	}
	return ""
}

func (me *memEng) GetInternalStatus() map[string]interface{} {
	s := make(map[string]interface{})
	s["internal"] = me.GetStatistics()
	return s
}

func (me *memEng) GetInternalPropertyStatus(p string) string {
	return p
}

func (me *memEng) GetBytesNoLock(key []byte) ([]byte, error) {
	v, err := me.GetRefNoLock(key)
	if err != nil {
		return nil, err
	}
	if v != nil {
		value := v.Bytes()
		v.Free()
		return value, nil
	}
	return nil, nil
}

func (me *memEng) GetBytes(key []byte) ([]byte, error) {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	if me.IsClosed() {
		return nil, errDBEngClosed
	}
	return me.GetBytesNoLock(key)
}

func (me *memEng) MultiGetBytes(keyList [][]byte, values [][]byte, errs []error) {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	if me.IsClosed() {
		for i, _ := range errs {
			errs[i] = errDBEngClosed
		}
		return
	}
	for i, k := range keyList {
		values[i], errs[i] = me.GetBytesNoLock(k)
	}
}

func (me *memEng) Exist(key []byte) (bool, error) {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	if me.IsClosed() {
		return false, errDBEngClosed
	}
	return me.ExistNoLock(key)
}

func (me *memEng) ExistNoLock(key []byte) (bool, error) {
	v, err := me.GetRefNoLock(key)
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}
	ok := v.Data() != nil
	v.Free()
	return ok, nil
}

func (me *memEng) GetRefNoLock(key []byte) (RefSlice, error) {
	if useSkiplist {
		v, err := me.slEng.Get(key)
		if err != nil {
			return nil, err
		}
		return &memRefSlice{b: v, needCopy: false}, nil
	}

	bt := me.eng
	bi := bt.MakeIter()

	bi.SeekGE(&kvitem{key: key})
	if !bi.Valid() {
		return nil, nil
	}
	item := bi.Cur()
	if bytes.Equal(item.key, key) {
		return &memRefSlice{b: item.value, needCopy: true}, nil
	}
	return nil, nil
}

func (me *memEng) GetRef(key []byte) (RefSlice, error) {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	if me.IsClosed() {
		return nil, errDBEngClosed
	}
	return me.GetRefNoLock(key)
}

func (me *memEng) GetValueWithOp(key []byte,
	op func([]byte) error) error {
	me.rwmutex.RLock()
	defer me.rwmutex.RUnlock()
	if me.IsClosed() {
		return errDBEngClosed
	}

	return me.GetValueWithOpNoLock(key, op)
}

func (me *memEng) GetValueWithOpNoLock(key []byte,
	op func([]byte) error) error {
	val, err := me.GetRef(key)
	if err != nil {
		return err
	}
	if val != nil {
		defer val.Free()
		return op(val.Data())
	}
	return op(nil)
}

func (me *memEng) DeleteFilesInRange(rg CRange) {
	return
}

func (me *memEng) GetIterator(opts IteratorOpts) (Iterator, error) {
	mit, err := newMemIterator(me, opts)
	if err != nil {
		return nil, err
	}
	return mit, nil
}

func (me *memEng) NewCheckpoint() (KVCheckpoint, error) {
	return &memEngCheckpoint{
		me: me,
	}, nil
}

type memEngCheckpoint struct {
	me *memEng
}

func (pck *memEngCheckpoint) Save(cpath string, notify chan struct{}) error {
	tmpFile := path.Join(cpath, "mem.dat.tmp")
	err := os.Mkdir(cpath, common.DIR_PERM)
	if err != nil && !os.IsExist(err) {
		return err
	}
	it, err := pck.me.GetIterator(IteratorOpts{})
	if err != nil {
		return err
	}
	var dataNum int64
	if useSkiplist {
		dataNum = pck.me.slEng.Len()
	} else {
		dataNum = int64(pck.me.eng.Len())
	}

	it.SeekToFirst()
	if notify != nil {
		close(notify)
	}

	n, fs, err := saveMemDBToFile(it, tmpFile, dataNum)
	// release the lock early to avoid blocking while sync file
	it.Close()

	if err != nil {
		dbLog.Infof("save checkpoint to %v failed: %s", cpath, err.Error())
		return err
	}
	if fs != nil {
		err = fs.Sync()
		if err != nil {
			dbLog.Errorf("save checkpoint to %v sync failed: %v ", cpath, err.Error())
			return err
		}
		fs.Close()
	}
	err = os.Rename(tmpFile, path.Join(cpath, "mem.dat"))
	if err != nil {
		dbLog.Errorf("save checkpoint to %v failed: %v ", cpath, err.Error())
	} else {
		dbLog.Infof("save checkpoint to %v done: %v bytes", cpath, n)
	}
	return err
}

func loadMemDBFromFile(fileName string, loader func([]byte, []byte) error) error {
	// read from checkpoint file
	fs, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	header := make([]byte, 22)
	_, err = fs.Read(header)
	if err != nil {
		return err
	}
	lenBuf := make([]byte, 8)
	dataKeyBuf := make([]byte, 0, 1024)
	dataValueBuf := make([]byte, 0, 1024)
	for {
		_, err := fs.Read(lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		vl := binary.BigEndian.Uint64(lenBuf)
		if uint64(len(dataKeyBuf)) < vl {
			dataKeyBuf = make([]byte, vl)
		}
		_, err = fs.Read(dataKeyBuf[:vl])
		if err != nil {
			return err
		}
		key := dataKeyBuf[:vl]
		_, err = fs.Read(lenBuf)
		if err != nil {
			return err
		}
		vl = binary.BigEndian.Uint64(lenBuf)
		if uint64(len(dataValueBuf)) < vl {
			dataValueBuf = make([]byte, vl)
		}
		_, err = fs.Read(dataValueBuf[:vl])
		if err != nil {
			return err
		}
		value := dataValueBuf[:vl]
		err = loader(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func saveMemDBToFile(it Iterator, fileName string, dataNum int64) (int64, *os.File, error) {
	fs, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, common.FILE_PERM)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if err != nil {
			fs.Close()
		}
	}()
	total := int64(0)
	n := int(0)
	n, err = fs.Write([]byte("v001\n"))
	if err != nil {
		return total, nil, err
	}
	total += int64(n)
	n, err = fs.Write([]byte(fmt.Sprintf("%016d\n", dataNum)))
	if err != nil {
		return total, nil, err
	}
	total += int64(n)
	buf := make([]byte, 8)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.RefKey()
		// save item to path
		vlen := uint64(len(k))
		binary.BigEndian.PutUint64(buf, vlen)
		n, err = fs.Write(buf[:8])
		if err != nil {
			return total, nil, err
		}
		total += int64(n)
		n, err = fs.Write(k)
		if err != nil {
			return total, nil, err
		}
		total += int64(n)
		v := it.RefValue()
		vlen = uint64(len(v))
		binary.BigEndian.PutUint64(buf, vlen)
		n, err = fs.Write(buf[:8])
		if err != nil {
			return total, nil, err
		}
		total += int64(n)
		n, err = fs.Write(v)
		if err != nil {
			return total, nil, err
		}
		total += int64(n)
	}
	return total, fs, nil
}
