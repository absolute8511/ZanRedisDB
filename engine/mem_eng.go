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

type memRefSlice struct {
	b []byte
}

func (rs *memRefSlice) Free() {
}

func (rs *memRefSlice) Data() []byte {
	return rs.b
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
	writerMutex sync.Mutex
	cfg         *RockEngConfig
	eng         *btree
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

func (pe *memEng) NewWriteBatch() WriteBatch {
	wb, err := newMemWriteBatch(pe)
	if err != nil {
		return nil
	}
	return wb
}

func (pe *memEng) DefaultWriteBatch() WriteBatch {
	return pe.NewWriteBatch()
}

func (pe *memEng) GetDataDir() string {
	return path.Join(pe.cfg.DataDir, "mem")
}

func (pe *memEng) SetMaxBackgroundOptions(maxCompact int, maxBackJobs int) error {
	return nil
}

func (pe *memEng) compactLoop() {
}

func (pe *memEng) CheckDBEngForRead(fullPath string) error {
	return nil
}

func (pe *memEng) OpenEng() error {
	if !pe.IsClosed() {
		dbLog.Warningf("engine already opened: %v, should close it before reopen", pe.GetDataDir())
		return errors.New("open failed since not closed")
	}
	pe.rwmutex.Lock()
	defer pe.rwmutex.Unlock()
	eng := &btree{
		cmp: cmpItem,
	}
	err := LoadBtreeFromFile(eng, pe.GetDataDir())
	if err != nil {
		return err
	}
	pe.eng = eng
	atomic.StoreInt32(&pe.engOpened, 1)
	dbLog.Infof("engine opened: %v", pe.GetDataDir())
	return nil
}

func (pe *memEng) Write(wb WriteBatch) error {
	return wb.Commit()
}

func (pe *memEng) DeletedBeforeCompact() int64 {
	return atomic.LoadInt64(&pe.deletedCnt)
}

func (pe *memEng) AddDeletedCnt(c int64) {
	atomic.AddInt64(&pe.deletedCnt, c)
}

func (pe *memEng) LastCompactTime() int64 {
	return atomic.LoadInt64(&pe.lastCompact)
}

func (pe *memEng) CompactRange(rg CRange) {
}

func (pe *memEng) CompactAllRange() {
	pe.CompactRange(CRange{})
}

func (pe *memEng) GetApproximateTotalKeyNum() int {
	return 0
}

func (pe *memEng) GetApproximateKeyNum(ranges []CRange) uint64 {
	return 0
}

func (pe *memEng) SetOptsForLogStorage() {
	return
}

func (pe *memEng) GetApproximateSizes(ranges []CRange, includeMem bool) []uint64 {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	sizeList := make([]uint64, len(ranges))
	if pe.IsClosed() {
		return sizeList
	}
	// TODO: estimate the size
	return sizeList
}

func (pe *memEng) IsClosed() bool {
	if atomic.LoadInt32(&pe.engOpened) == 0 {
		return true
	}
	return false
}

func (pe *memEng) CloseEng() bool {
	pe.rwmutex.Lock()
	defer pe.rwmutex.Unlock()
	if pe.eng != nil {
		if atomic.CompareAndSwapInt32(&pe.engOpened, 1, 0) {
			pe.eng.Reset()
			dbLog.Infof("engine closed: %v", pe.GetDataDir())
			return true
		}
	}
	return false
}

func (pe *memEng) CloseAll() {
	select {
	case <-pe.quit:
	default:
		close(pe.quit)
	}
	pe.CloseEng()
}

func (pe *memEng) GetStatistics() string {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return ""
	}
	return ""
}

func (pe *memEng) GetInternalStatus() map[string]interface{} {
	s := make(map[string]interface{})
	s["internal"] = pe.GetStatistics()
	return s
}

func (pe *memEng) GetInternalPropertyStatus(p string) string {
	return p
}

func (pe *memEng) GetBytesNoLock(key []byte) ([]byte, error) {
	v, err := pe.GetRefNoLock(key)
	if err != nil {
		return nil, err
	}
	if v != nil {
		value := make([]byte, len(v.Data()))
		copy(value, v.Data())
		v.Free()
		return value, nil
	}
	return nil, nil
}

func (pe *memEng) GetBytes(key []byte) ([]byte, error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return nil, errDBEngClosed
	}
	return pe.GetBytesNoLock(key)
}

func (pe *memEng) MultiGetBytes(keyList [][]byte, values [][]byte, errs []error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		for i, _ := range errs {
			errs[i] = errDBEngClosed
		}
		return
	}
	for i, k := range keyList {
		values[i], errs[i] = pe.GetBytesNoLock(k)
	}
}

func (pe *memEng) Exist(key []byte) (bool, error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return false, errDBEngClosed
	}
	return pe.ExistNoLock(key)
}

func (pe *memEng) ExistNoLock(key []byte) (bool, error) {
	v, err := pe.GetRefNoLock(key)
	if err != nil {
		return false, err
	}
	if v != nil {
		v.Free()
		return true, nil
	}
	return false, nil
}

func (pe *memEng) GetRefNoLock(key []byte) (RefSlice, error) {
	bt := pe.eng.Clone()
	defer bt.Reset()
	bi := bt.MakeIter()

	bi.SeekGE(&kvitem{key: key})
	if !bi.Valid() {
		return nil, nil
	}
	item := bi.Cur()
	if bytes.Equal(item.key, key) {
		return &memRefSlice{b: item.value}, nil
	}
	return nil, nil
}

func (pe *memEng) GetRef(key []byte) (RefSlice, error) {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return nil, errDBEngClosed
	}
	return pe.GetRefNoLock(key)
}

func (pe *memEng) GetValueWithOp(key []byte,
	op func([]byte) error) error {
	pe.rwmutex.RLock()
	defer pe.rwmutex.RUnlock()
	if pe.IsClosed() {
		return errDBEngClosed
	}

	return pe.GetValueWithOpNoLock(key, op)
}

func (pe *memEng) GetValueWithOpNoLock(key []byte,
	op func([]byte) error) error {
	val, err := pe.GetRef(key)
	if err != nil {
		return err
	}
	if val != nil {
		defer val.Free()
		return op(val.Data())
	}
	return op(nil)
}

func (pe *memEng) DeleteFilesInRange(rg CRange) {
	return
}

func (pe *memEng) GetIterator(opts IteratorOpts) (Iterator, error) {
	mit, err := newMemIterator(pe, opts)
	if err != nil {
		return nil, err
	}
	return mit, nil
}

func (pe *memEng) NewCheckpoint() (KVCheckpoint, error) {
	return &memEngCheckpoint{
		pe: pe,
	}, nil
}

type memEngCheckpoint struct {
	pe *memEng
}

func (pck *memEngCheckpoint) Save(path string, notify chan struct{}) error {
	pck.pe.rwmutex.RLock()
	defer pck.pe.rwmutex.RUnlock()
	if pck.pe.IsClosed() {
		return errDBEngClosed
	}
	cbt := pck.pe.eng.Clone()
	defer cbt.Reset()
	// cloned btree will be immutable, so we notify we can continue do write
	if notify != nil {
		close(notify)
	}
	n, err := saveBtreeToFile(&cbt, path)
	if err != nil {
		dbLog.Infof("save checkpoint to %v failed: %s", path, err.Error())
		return err
	}
	dbLog.Infof("save checkpoint to %v done: %v bytes", path, n)
	return nil
}

func LoadBtreeFromFile(eng *btree, dir string) error {
	// read from checkpoint file
	fs, err := os.Open(path.Join(dir, "btree.dat"))
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
	dataBuf := make([]byte, 0, 1024)
	for {
		_, err := fs.Read(lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		vl := binary.BigEndian.Uint64(lenBuf)
		if uint64(len(dataBuf)) < vl {
			dataBuf = make([]byte, vl)
		}
		_, err = fs.Read(dataBuf[:vl])
		if err != nil {
			return err
		}
		item := &kvitem{
			key: make([]byte, vl),
		}
		copy(item.key, dataBuf[:vl])
		_, err = fs.Read(lenBuf)
		if err != nil {
			return err
		}
		vl = binary.BigEndian.Uint64(lenBuf)
		if uint64(len(dataBuf)) < vl {
			dataBuf = make([]byte, vl)
		}
		_, err = fs.Read(dataBuf[:vl])
		if err != nil {
			return err
		}
		item.value = make([]byte, vl)
		copy(item.value, dataBuf[:vl])
		eng.Set(item)
	}
	return nil
}

func saveBtreeToFile(cbt *btree, dir string) (int64, error) {
	err := os.Mkdir(dir, common.DIR_PERM)
	if err != nil && !os.IsExist(err) {
		return 0, err
	}
	fs, err := os.OpenFile(path.Join(dir, "btree.dat"), os.O_CREATE|os.O_WRONLY, common.FILE_PERM)
	if err != nil {
		return 0, err
	}
	total := int64(0)
	n, err := fs.Write([]byte("v001\n"))
	if err != nil {
		return total, err
	}
	total += int64(n)
	n, err = fs.Write([]byte(fmt.Sprintf("%016d\n", cbt.Len())))
	if err != nil {
		return total, err
	}
	total += int64(n)
	bi := cbt.MakeIter()
	buf := make([]byte, 8)
	for bi.First(); bi.Valid(); bi.Next() {
		item := bi.Cur()
		// save item to path
		vlen := uint64(len(item.key))
		binary.BigEndian.PutUint64(buf, vlen)
		n, err = fs.Write(buf[:8])
		if err != nil {
			return total, err
		}
		total += int64(n)
		n, err = fs.Write(item.key)
		if err != nil {
			return total, err
		}
		total += int64(n)
		vlen = uint64(len(item.value))
		binary.BigEndian.PutUint64(buf, vlen)
		n, err = fs.Write(buf[:8])
		if err != nil {
			return total, err
		}
		total += int64(n)
		n, err = fs.Write(item.value)
		if err != nil {
			return total, err
		}
		total += int64(n)
	}
	return total, nil
}
