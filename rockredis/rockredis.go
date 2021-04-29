package rockredis

import (
	"bytes"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/murmur3"

	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/ZanRedisDB/engine"
	"github.com/youzan/ZanRedisDB/metric"
)

const (
	MaxCheckpointNum       = 10
	MaxRemoteCheckpointNum = 3
	HLLReadCacheSize       = 1024
	HLLWriteCacheSize      = 32
	writeTmpSize           = 1024 * 512
	minExpiredPossible     = 1500000000
)

var (
	lazyCleanExpired = time.Hour * 48
	timeUpdateFreq   = 10000
)

var dbLog = common.NewLevelLogger(common.LOG_INFO, common.NewLogger())

func SetLogLevel(level int32) {
	dbLog.SetLevel(level)
}

func SetLogger(level int32, logger common.Logger) {
	dbLog.SetLevel(level)
	dbLog.Logger = logger
}

func GetCheckpointDir(term uint64, index uint64) string {
	return fmt.Sprintf("%016x-%016x", term, index)
}

var batchableCmds map[string]bool

type RockRedisDBConfig struct {
	engine.RockEngConfig
	KeepBackup int
	// this will ignore all update and non-exist delete
	EstimateTableCounter bool
	ExpirationPolicy     common.ExpirationPolicy
	DataVersion          common.DataVersionT
}

func NewRockRedisDBConfig() *RockRedisDBConfig {
	c := &RockRedisDBConfig{
		EstimateTableCounter: false,
	}
	c.RockEngConfig = *engine.NewRockConfig()
	return c
}

type CheckpointSortNames []string

func (self CheckpointSortNames) Len() int {
	return len(self)
}

func (self CheckpointSortNames) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self CheckpointSortNames) Less(i, j int) bool {
	left := path.Base(self[i])
	right := path.Base(self[j])
	lsplit := strings.SplitN(left, "-", 2)
	rsplit := strings.SplitN(right, "-", 2)
	if len(lsplit) != 2 || len(rsplit) != 2 {
		dbLog.Panicf("the checkpoint name is not valid: %v, %v", left, right)
	}
	lterm, err := strconv.ParseUint(lsplit[0], 16, 64)
	if err != nil {
		dbLog.Panicf("the checkpoint name is not valid: %v, %v, %v", left, right, err)
	}
	lindex, _ := strconv.ParseUint(lsplit[1], 16, 64)
	rterm, _ := strconv.ParseUint(rsplit[0], 16, 64)
	rindex, _ := strconv.ParseUint(rsplit[1], 16, 64)
	if lterm == rterm {
		return lindex < rindex
	}
	return lterm < rterm
}

func GetLatestCheckpoint(checkpointDir string, skipN int, matchFunc func(string) bool) string {
	checkpointList, err := filepath.Glob(path.Join(checkpointDir, "*-*"))
	if err != nil {
		return ""
	}
	if len(checkpointList) <= skipN {
		return ""
	}

	sortedNameList := CheckpointSortNames(checkpointList)
	sort.Sort(sortedNameList)
	startIndex := len(sortedNameList) - 1
	for i := startIndex; i >= 0; i-- {
		curDir := sortedNameList[i]
		if matchFunc(curDir) {
			if skipN > 0 {
				skipN--
				continue
			}
			return curDir
		}
	}
	return ""
}

func purgeOldCheckpoint(keepNum int, checkpointDir string, latestSnapIndex uint64) {
	defer func() {
		if e := recover(); e != nil {
			dbLog.Infof("purge old checkpoint failed: %v", e)
		}
	}()
	checkpointList, err := filepath.Glob(path.Join(checkpointDir, "*-*"))
	if err != nil {
		return
	}
	if len(checkpointList) > keepNum {
		sortedNameList := CheckpointSortNames(checkpointList)
		sort.Sort(sortedNameList)
		for i := 0; i < len(sortedNameList)-keepNum; i++ {
			fn := path.Base(sortedNameList[i+keepNum])
			subs := strings.Split(fn, "-")
			if len(subs) != 2 {
				continue
			}
			sindex, err := strconv.ParseUint(subs[1], 16, 64)
			if err != nil {
				dbLog.Infof("checkpoint name index invalid: %v, %v", subs, err.Error())
				continue
			}
			if sindex >= latestSnapIndex {
				break
			}
			os.RemoveAll(sortedNameList[i])
			dbLog.Infof("clean checkpoint : %v", sortedNameList[i])
		}
	}
}

type rockCompactFilter struct {
	rdb           *RockDB
	checkedCnt    int64
	cachedTimeSec int64
	metric.CompactFilterStats
}

func (cf *rockCompactFilter) Name() string {
	return "rockredis.compactfilter"
}

func (cf *rockCompactFilter) Stats() metric.CompactFilterStats {
	var s metric.CompactFilterStats
	s.ExpiredCleanCnt = atomic.LoadInt64(&cf.ExpiredCleanCnt)
	s.VersionCleanCnt = atomic.LoadInt64(&cf.VersionCleanCnt)
	s.DelCleanCnt = atomic.LoadInt64(&cf.DelCleanCnt)
	return s
}

func (cf *rockCompactFilter) lazyExpireCheck(h headerMetaValue, curCnt int64) bool {
	if h.ExpireAt == 0 {
		return false
	}
	if h.ExpireAt <= minExpiredPossible {
		dbLog.Infof("db %s key %v has invalid small expired timestamp: %v", cf.rdb.GetDataDir(), h.UserData, h.ExpireAt)
		return false
	}
	// avoid call now() too much, we cache the time for a while
	ts := atomic.LoadInt64(&cf.cachedTimeSec)
	if curCnt > int64(timeUpdateFreq) || ts <= 0 {
		ts = time.Now().Unix()
		atomic.StoreInt64(&cf.cachedTimeSec, ts)
		atomic.StoreInt64(&cf.checkedCnt, 0)
	}
	if int64(h.ExpireAt)+lazyCleanExpired.Nanoseconds()/int64(time.Second) < ts {
		//dbLog.Debugf("db %s key %v clean since expired timestamp: %v, %v", cf.rdb.GetDataDir(), h.UserData, h.ExpireAt, ts)
		atomic.AddInt64(&cf.ExpiredCleanCnt, 1)
		return true
	}
	return false
}

func (cf *rockCompactFilter) Filter(level int, key, value []byte) (bool, []byte) {
	// dbLog.Debugf("db %v level %v compacting: %v, %s", cf.rdb.GetDataDir(), level, key, value)
	// check key type
	if len(key) < 1 {
		return false, nil
	}
	newCnt := atomic.AddInt64(&cf.checkedCnt, 1)
	switch key[0] {
	case KVType, HSizeType, LMetaType, SSizeType, ZSizeType, BitmapMetaType:
		var h headerMetaValue
		_, err := h.decode(value)
		if err != nil {
			return false, nil
		}
		return cf.lazyExpireCheck(h, newCnt), nil
	case HashType, ListType, SetType, ZSetType, ZScoreType, BitmapType:
		dt, rawKey, ver, err := convertCollDBKeyToRawKey(key)
		if err != nil {
			return false, nil
		}
		if ver == 0 {
			return false, nil
		}
		// the version is timestamp in nano, check lazy expire
		ts := atomic.LoadInt64(&cf.cachedTimeSec)
		if int64(ver)+lazyCleanExpired.Nanoseconds() >= ts*int64(time.Second) {
			return false, nil
		}
		metak, err := encodeMetaKey(dt, rawKey)
		if err != nil {
			return false, nil
		}
		// maybe cache the meta value if the collection has too many subkeys
		metav, err := cf.rdb.GetBytesNoLock(metak)
		if err != nil {
			return false, nil
		}
		if metav == nil {
			// collection meta not found, it means the whole collection is deleted
			//dbLog.Debugf("db %s key %v clean since meta not exist: %v, %v, %v", cf.rdb.GetDataDir(), key, metak, rawKey, ver)
			atomic.AddInt64(&cf.DelCleanCnt, 1)
			return true, nil
		}
		var h headerMetaValue
		_, err = h.decode(metav)
		if err != nil {
			return false, nil
		}
		if h.ValueVersion == 0 {
			// maybe no version?
			return false, nil
		}
		// how to lazy clean for version mismatch?
		if h.ValueVersion != ver {
			//dbLog.Debugf("db %s key %v clean since mismatch version: %v, %v", cf.rdb.GetDataDir(), key, h, ver)
			atomic.AddInt64(&cf.VersionCleanCnt, 1)
			return true, nil
		}
		return cf.lazyExpireCheck(h, newCnt), nil
	}
	return false, nil
}

type RockDB struct {
	expiration
	cfg               *RockRedisDBConfig
	rockEng           engine.KVEngine
	wb                engine.WriteBatch
	writeTmpBuf       []byte
	quit              chan struct{}
	wg                sync.WaitGroup
	backupC           chan *BackupInfo
	indexMgr          *IndexMgr
	isBatching        int32
	checkpointDirLock sync.RWMutex
	hasher64          hash.Hash64
	hllCache          *hllCache
	stopping          int32
	engOpened         int32
	latestSnapIndex   uint64
	topLargeCollKeys  *metric.CollSizeHeap
	compactFilter     *rockCompactFilter
}

func OpenRockDB(cfg *RockRedisDBConfig) (*RockDB, error) {
	eng, err := engine.NewKVEng(&cfg.RockEngConfig)
	if err != nil {
		return nil, err
	}

	db := &RockDB{
		cfg:              cfg,
		rockEng:          eng,
		writeTmpBuf:      make([]byte, writeTmpSize),
		backupC:          make(chan *BackupInfo),
		quit:             make(chan struct{}),
		hasher64:         murmur3.New64(),
		topLargeCollKeys: metric.NewCollSizeHeap(metric.DefaultHeapCapacity),
	}

	switch cfg.ExpirationPolicy {
	case common.LocalDeletion:
		db.expiration = newLocalExpiration(db)
	case common.WaitCompact:
		db.expiration = newCompactExpiration(db)
		// register the compact callback to lazy clean the expired data
		db.RegisterCompactCallback()
	default:
		return nil, errors.New("unsupported ExpirationPolicy")
	}

	err = db.reOpenEng()
	if err != nil {
		return nil, err
	}

	if !cfg.ReadOnly {
		os.MkdirAll(db.GetBackupDir(), common.DIR_PERM)
	}

	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		db.backupLoop()
	}()

	return db, nil
}

func GetBackupDir(base string) string {
	return path.Join(base, "rocksdb_backup")
}

func GetBackupDirForRemote(base string) string {
	return path.Join(base, "rocksdb_backup", "remote")
}

func (r *RockDB) SetLatestSnapIndex(i uint64) {
	atomic.StoreUint64(&r.latestSnapIndex, i)
}

func (r *RockDB) GetBackupBase() string {
	return r.cfg.DataDir
}

func (r *RockDB) GetBackupDirForRemote() string {
	return GetBackupDirForRemote(r.cfg.DataDir)
}

func (r *RockDB) GetBackupDir() string {
	return GetBackupDir(r.cfg.DataDir)
}

func (r *RockDB) GetDataDir() string {
	return r.rockEng.GetDataDir()
}

func (r *RockDB) GetCompactFilterStats() metric.CompactFilterStats {
	if r.compactFilter != nil {
		return r.compactFilter.Stats()
	}
	return metric.CompactFilterStats{}
}

func (r *RockDB) RegisterCompactCallback() {
	filter := &rockCompactFilter{
		rdb: r,
	}
	r.compactFilter = filter
	r.rockEng.SetCompactionFilter(filter)
}

func (r *RockDB) reOpenEng() error {
	var err error
	hcache, err := newHLLCache(HLLReadCacheSize, HLLWriteCacheSize, r)
	if err != nil {
		return err
	}
	r.hllCache = hcache
	r.indexMgr = NewIndexMgr()

	err = r.rockEng.OpenEng()
	if err != nil {
		return err
	}
	r.wb = r.rockEng.DefaultWriteBatch()

	err = r.indexMgr.LoadIndexes(r)
	if err != nil {
		dbLog.Warningf("rocksdb %v load index failed: %v", r.GetDataDir(), err)
		r.rockEng.CloseEng()
		return err
	}
	if r.expiration != nil && !r.cfg.ReadOnly {
		r.expiration.Start()
	}
	atomic.StoreInt32(&r.engOpened, 1)
	return nil
}

func (r *RockDB) getIndexer() *IndexMgr {
	e := r.indexMgr
	return e
}

func (r *RockDB) SetMaxBackgroundOptions(maxCompact int, maxBackJobs int) error {
	return r.rockEng.SetMaxBackgroundOptions(maxCompact, maxBackJobs)
}

// interrupt the manual compact to avoid stall too long?
func (r *RockDB) DisableManualCompact(disable bool) {
	r.rockEng.DisableManualCompact(disable)
}

func (r *RockDB) CompactAllRange() {
	r.rockEng.CompactAllRange()
}

func (r *RockDB) CompactOldExpireData() {
	now := time.Now().Unix()
	minKey := expEncodeTimeKey(NoneType, nil, 0)
	maxKey := expEncodeTimeKey(maxDataType, nil, now)
	r.CompactRange(minKey, maxKey)
}

func (r *RockDB) CompactRange(minKey []byte, maxKey []byte) {
	var rg engine.CRange
	rg.Start = minKey
	rg.Limit = maxKey
	dbLog.Infof("compacting range: %v, %v", minKey, maxKey)
	r.rockEng.CompactRange(rg)
}

func (r *RockDB) closeEng() {
	if atomic.CompareAndSwapInt32(&r.engOpened, 1, 0) {
		if r.hllCache != nil {
			r.hllCache.Flush()
		}
		if r.indexMgr != nil {
			r.indexMgr.Close()
		}
		if r.expiration != nil {
			r.expiration.Stop()
		}
		if r.rockEng != nil {
			r.rockEng.CloseEng()
		}
	}
}

func (r *RockDB) Close() {
	if !atomic.CompareAndSwapInt32(&r.stopping, 0, 1) {
		return
	}
	close(r.quit)
	r.wg.Wait()
	r.closeEng()
	if r.expiration != nil {
		r.expiration.Destroy()
		r.expiration = nil
	}
	if r.rockEng != nil {
		r.rockEng.CloseAll()
	}
	dbLog.Infof("rocksdb %v closed", r.cfg.DataDir)
}

func (r *RockDB) GetInternalStatus() map[string]interface{} {
	return r.rockEng.GetInternalStatus()
}

func (r *RockDB) GetInternalPropertyStatus(p string) string {
	return r.rockEng.GetInternalPropertyStatus(p)
}

func (r *RockDB) GetStatistics() string {
	return r.rockEng.GetStatistics()
}

func (r *RockDB) GetTopLargeKeys() []metric.TopNInfo {
	return r.topLargeCollKeys.TopKeys()
}

func (r *RockDB) GetBytesNoLock(key []byte) ([]byte, error) {
	return r.rockEng.GetBytesNoLock(key)
}

func (r *RockDB) GetBytes(key []byte) ([]byte, error) {
	return r.rockEng.GetBytes(key)
}

func (r *RockDB) MultiGetBytes(keyList [][]byte, values [][]byte, errs []error) {
	r.rockEng.MultiGetBytes(keyList, values, errs)
}

func (r *RockDB) Exist(key []byte) (bool, error) {
	return r.rockEng.Exist(key)
}

func (r *RockDB) ExistNoLock(key []byte) (bool, error) {
	return r.rockEng.ExistNoLock(key)
}

// make sure close all the iterator before do any write on engine, since it may have lock on read/iterator
func (r *RockDB) NewDBRangeIterator(min []byte, max []byte, rtype uint8,
	reverse bool) (*engine.RangeLimitedIterator, error) {
	opts := engine.IteratorOpts{
		Reverse: reverse,
	}
	opts.Max = max
	opts.Min = min
	opts.Type = rtype
	return engine.NewDBRangeIteratorWithOpts(r.rockEng, opts)
}

func (r *RockDB) NewDBRangeLimitIterator(min []byte, max []byte, rtype uint8,
	offset int, count int, reverse bool) (*engine.RangeLimitedIterator, error) {
	opts := engine.IteratorOpts{
		Reverse: reverse,
	}
	opts.Max = max
	opts.Min = min
	opts.Type = rtype
	opts.Offset = offset
	opts.Count = count
	return engine.NewDBRangeLimitIteratorWithOpts(r.rockEng, opts)
}

func (r *RockDB) NewDBRangeIteratorWithOpts(opts engine.IteratorOpts) (*engine.RangeLimitedIterator, error) {
	return engine.NewDBRangeIteratorWithOpts(r.rockEng, opts)
}

func (r *RockDB) NewDBRangeLimitIteratorWithOpts(opts engine.IteratorOpts) (*engine.RangeLimitedIterator, error) {
	return engine.NewDBRangeLimitIteratorWithOpts(r.rockEng, opts)
}

// [start, end)
func (r *RockDB) CompactTableRange(table string) {
	dts := []byte{KVType, HashType, ListType, SetType, ZSetType}
	dtsMeta := []byte{KVType, HSizeType, LMetaType, SSizeType, ZSizeType}
	for i, dt := range dts {
		rgs, err := getTableDataRange(dt, []byte(table), nil, nil)
		if err != nil {
			dbLog.Infof("failed to build dt %v data range: %v", dt, err)
			continue
		}
		// compact data range
		dbLog.Infof("compacting dt %v data range: %v", dt, rgs)
		for _, rg := range rgs {
			r.rockEng.CompactRange(rg)
		}
		// compact meta range
		minKey, maxKey, err := getTableMetaRange(dtsMeta[i], []byte(table), nil, nil)
		if err != nil {
			dbLog.Infof("failed to get table %v data range: %v", table, err)
			continue
		}
		var rg engine.CRange
		rg.Start = minKey
		rg.Limit = maxKey
		dbLog.Infof("compacting dt %v meta range: %v, %v", dt, minKey, maxKey)
		r.rockEng.CompactRange(rg)
	}
}

func getTableDataRange(dt byte, table []byte, start, end []byte) ([]engine.CRange, error) {
	minKey, err := encodeFullScanMinKey(dt, table, start, nil)
	if err != nil {
		dbLog.Infof("failed to build dt %v range: %v", dt, err)
		return nil, err
	}
	var maxKey []byte
	if end == nil {
		maxKey = encodeDataTableEnd(dt, table)
	} else {
		maxKey, err = encodeFullScanMinKey(dt, table, end, nil)
	}
	if err != nil {
		dbLog.Infof("failed to build dt %v range: %v", dt, err)
		return nil, err
	}
	rgs := make([]engine.CRange, 0, 2)
	rgs = append(rgs, engine.CRange{Start: minKey, Limit: maxKey})
	if dt == ZSetType {
		// zset has key-score-member data except the key-member data
		zminKey := zEncodeStartKey(table, start)
		var zmaxKey []byte
		if end == nil {
			zmaxKey = encodeDataTableEnd(ZScoreType, []byte(table))
		} else {
			zmaxKey = zEncodeStopKey(table, end)
		}
		rgs = append(rgs, engine.CRange{Start: zminKey, Limit: zmaxKey})
	}
	dbLog.Debugf("table dt %v data range: %v", dt, rgs)
	return rgs, nil
}

func getTableMetaRange(dt byte, table []byte, start, end []byte) ([]byte, []byte, error) {
	tableStart := append(table, tableStartSep)
	tableStart = append(tableStart, start...)
	minMetaKey, err := encodeScanKey(dt, tableStart)
	if err != nil {
		return nil, nil, err
	}
	tableStart = tableStart[:0]
	if end == nil {
		tableStart = append(table, tableStartSep+1)
	} else {
		tableStart = append(table, tableStartSep)
		tableStart = append(tableStart, end...)
	}
	maxMetaKey, err := encodeScanKey(dt, tableStart)
	if err != nil {
		return nil, nil, err
	}
	dbLog.Debugf("table dt %v meta range: %v, %v", dt, minMetaKey, maxMetaKey)
	return minMetaKey, maxMetaKey, nil
}

// [start, end)
func (r *RockDB) DeleteTableRange(dryrun bool, table string, start []byte, end []byte) error {
	// TODO: need handle index and meta data, since index need scan if we delete part
	// range of table, we can only allow delete whole table if it has index.
	// fixme: how to handle the table key number counter, scan to count the deleted number is too slow

	tidx := r.indexMgr.GetTableIndexes(table)
	if tidx != nil {
		return errors.New("drop table with any index is not supported currently")
	}
	wb := r.rockEng.NewWriteBatch()
	defer wb.Destroy()
	// kv, hash, set, list, zset
	dts := []byte{KVType, HashType, ListType, SetType, ZSetType}
	dtsMeta := []byte{KVType, HSizeType, LMetaType, SSizeType, ZSizeType}
	for i, dt := range dts {
		// delete meta and data
		rgs, err := getTableDataRange(dt, []byte(table), start, end)
		if err != nil {
			dbLog.Infof("failed to build dt %v range: %v", dt, err)
			continue
		}
		dbLog.Infof("delete dt %v data range: %v", dt, rgs)
		// delete meta
		minMetaKey, maxMetaKey, err := getTableMetaRange(dtsMeta[i], []byte(table), start, end)
		if err != nil {
			continue
		}
		dbLog.Infof("deleting dt %v meta range: %v, %v, %v, %v", dt,
			minMetaKey, maxMetaKey, string(minMetaKey), string(maxMetaKey))

		if dryrun {
			continue
		}
		for _, rg := range rgs {
			r.rockEng.DeleteFilesInRange(rg)
			wb.DeleteRange(rg.Start, rg.Limit)
		}
		wb.DeleteRange(minMetaKey, maxMetaKey)
		if start == nil && end == nil {
			// delete table counter
			r.DelTableKeyCount([]byte(table), wb)
		}
	}
	if dryrun {
		return nil
	}
	err := r.rockEng.Write(wb)
	if err != nil {
		dbLog.Infof("failed to delete table %v range: %v", table, err)
	}
	return nil
}

func (r *RockDB) GetBTablesSizes(tables [][]byte) []int64 {
	// try all data types for each table
	tableTotals := make([]int64, 0, len(tables))
	for _, table := range tables {
		ss := r.GetTableSizeInRange(string(table), nil, nil)
		tableTotals = append(tableTotals, ss)
	}
	return tableTotals
}

// [start, end)
func (r *RockDB) GetTablesSizes(tables []string) []int64 {
	// try all data types for each table
	tableTotals := make([]int64, 0, len(tables))
	for _, table := range tables {
		ss := r.GetTableSizeInRange(table, nil, nil)
		tableTotals = append(tableTotals, ss)
	}

	return tableTotals
}

// [start, end)
func (r *RockDB) GetTableSizeInRange(table string, start []byte, end []byte) int64 {
	dts := []byte{KVType, HashType, ListType, SetType, ZSetType}
	dtsMeta := []byte{KVType, HSizeType, LMetaType, SSizeType, ZSizeType}
	rgs := make([]engine.CRange, 0, len(dts))
	for i, dt := range dts {
		// data range
		drgs, err := getTableDataRange(dt, []byte(table), start, end)
		if err != nil {
			dbLog.Infof("failed to build dt %v range: %v", dt, err)
			continue
		}
		rgs = append(rgs, drgs...)
		// meta range
		minMetaKey, maxMetaKey, err := getTableMetaRange(dtsMeta[i], []byte(table), start, end)
		if err != nil {
			dbLog.Infof("failed to build dt %v meta range: %v", dt, err)
			continue
		}
		var rgMeta engine.CRange
		rgMeta.Start = minMetaKey
		rgMeta.Limit = maxMetaKey
		rgs = append(rgs, rgMeta)
	}
	sList := r.rockEng.GetApproximateSizes(rgs, true)
	dbLog.Debugf("range %v sizes: %v", rgs, sList)
	total := uint64(0)
	for _, ss := range sList {
		total += ss
	}
	return int64(total)
}

func (r *RockDB) GetApproximateTotalNum() int64 {
	return int64(r.rockEng.GetApproximateTotalKeyNum())
}

// [start, end)
func (r *RockDB) GetTableApproximateNumInRange(table string, start []byte, end []byte) int64 {
	num := r.rockEng.GetApproximateTotalKeyNum()
	if num <= 0 {
		return 0
	}
	dts := []byte{KVType, HashType, ListType, SetType, ZSetType}
	dtsMeta := []byte{KVType, HSizeType, LMetaType, SSizeType, ZSizeType}
	rgs := make([]engine.CRange, 0, len(dts))
	for i, dt := range dts {
		// meta range
		minMetaKey, maxMetaKey, err := getTableMetaRange(dtsMeta[i], []byte(table), start, end)
		if err != nil {
			dbLog.Infof("failed to build dt %v meta range: %v", dt, err)
			continue
		}
		var rgMeta engine.CRange
		rgMeta.Start = minMetaKey
		rgMeta.Limit = maxMetaKey
		rgs = append(rgs, rgMeta)
	}
	filteredRgs := make([]engine.CRange, 0, len(dts))
	sList := r.rockEng.GetApproximateSizes(rgs, true)
	for i, s := range sList {
		if s > 0 {
			filteredRgs = append(filteredRgs, rgs[i])
		}
	}
	keyNum := int64(r.rockEng.GetApproximateKeyNum(filteredRgs))
	dbLog.Debugf("total db key num: %v, table %s key num %v, %v", num, table, keyNum, sList)
	// use GetApproximateSizes and estimate-keys-num in property
	// refer: https://github.com/facebook/mysql-5.6/commit/4ca34d2498e8d16ede73a7955d1ab101a91f102f
	// range records = estimate-keys-num * GetApproximateSizes(range) / GetApproximateSizes (total)
	// use GetPropertiesOfTablesInRange to get number of keys in sst
	return int64(keyNum)
}

type BackupInfo struct {
	backupDir string
	started   chan struct{}
	done      chan struct{}
	rsp       []byte
	err       error
}

func newBackupInfo(dir string) *BackupInfo {
	return &BackupInfo{
		backupDir: dir,
		started:   make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (self *BackupInfo) WaitReady() {
	select {
	case <-self.started:
	case <-self.done:
	}
}

func (self *BackupInfo) GetResult() ([]byte, error) {
	select {
	case <-self.done:
	}
	return self.rsp, self.err
}

func (r *RockDB) backupLoop() {
	for {
		select {
		case rsp, ok := <-r.backupC:
			if !ok {
				return
			}

			func() {
				// before close rsp.done or rsp.started, the raft loop will block,
				// after the chan closed, the raft loop continue, so we need make sure
				// the db engine will not be closed while doing checkpoint, we need hold read lock
				// before closing the chan.
				defer close(rsp.done)
				dbLog.Infof("begin backup to:%v \n", rsp.backupDir)
				start := time.Now()
				ck, err := r.rockEng.NewCheckpoint(false)
				if err != nil {
					dbLog.Infof("init checkpoint failed: %v", err)
					rsp.err = err
					return
				}

				r.checkpointDirLock.Lock()
				_, err = os.Stat(rsp.backupDir)
				if !os.IsNotExist(err) {
					dbLog.Infof("checkpoint exist: %v, remove it", rsp.backupDir)
					os.RemoveAll(rsp.backupDir)
				}
				rsp.rsp = []byte(rsp.backupDir)
				err = ck.Save(rsp.backupDir, rsp.started)
				r.checkpointDirLock.Unlock()
				if err != nil {
					dbLog.Infof("save checkpoint failed: %v", err)
					rsp.err = err
					return
				}
				cost := time.Now().Sub(start)
				dbLog.Infof("backup done (cost %v), check point to: %v\n", cost.String(), rsp.backupDir)
			}()
			// purge some old checkpoint
			r.checkpointDirLock.Lock()
			keepNum := MaxCheckpointNum
			if r.cfg.KeepBackup > 0 {
				keepNum = r.cfg.KeepBackup
			}
			// avoid purge the checkpoint in the raft snapshot
			purgeOldCheckpoint(keepNum, r.GetBackupDir(), atomic.LoadUint64(&r.latestSnapIndex))
			purgeOldCheckpoint(MaxRemoteCheckpointNum, r.GetBackupDirForRemote(), math.MaxUint64-1)
			r.checkpointDirLock.Unlock()
		case <-r.quit:
			return
		}
	}
}

func (r *RockDB) Backup(term uint64, index uint64) *BackupInfo {
	fname := GetCheckpointDir(term, index)
	checkpointDir := path.Join(r.GetBackupDir(), fname)
	bi := newBackupInfo(checkpointDir)
	r.hllCache.Flush()
	select {
	case r.backupC <- bi:
	default:
		return nil
	}
	return bi
}

func (r *RockDB) IsLocalBackupOK(term uint64, index uint64) (bool, error) {
	r.checkpointDirLock.RLock()
	defer r.checkpointDirLock.RUnlock()
	return r.isBackupOKInPath(r.GetBackupDir(), term, index)
}

func (r *RockDB) isBackupOKInPath(backupDir string, term uint64, index uint64) (bool, error) {
	checkpointDir := GetCheckpointDir(term, index)
	fullPath := path.Join(backupDir, checkpointDir)
	_, err := os.Stat(fullPath)
	if os.IsNotExist(err) {
		dbLog.Infof("checkpoint not exist: %v", fullPath)
		return false, err
	}
	if r.rockEng == nil {
		return false, errDBClosed
	}
	dbLog.Infof("begin check local checkpoint : %v", fullPath)
	defer dbLog.Infof("check local checkpoint : %v done", fullPath)
	err = r.rockEng.CheckDBEngForRead(fullPath)
	if err != nil {
		dbLog.Infof("checkpoint open failed: %v", err)
		return false, err
	}
	return true, nil
}

func copyFile(src, dst string, override bool) error {
	sfi, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !sfi.Mode().IsRegular() {
		return fmt.Errorf("copyfile: non-regular source file %v (%v)", sfi.Name(), sfi.Mode().String())
	}
	_, err = os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		if !override {
			return nil
		}
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	// we remove dst to avoid override the hard link file content which may affect the origin linked file
	err = os.Remove(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	err = out.Sync()
	return err
}

func (r *RockDB) RestoreFromRemoteBackup(term uint64, index uint64) error {
	// check if there is the same term-index backup on local
	// if not, we can just rename remote snap to this name.
	// if already exist, we need handle rename
	checkpointDir := GetCheckpointDir(term, index)
	remotePath := path.Join(r.GetBackupDirForRemote(), checkpointDir)
	_, err := os.Stat(remotePath)
	if err != nil {
		dbLog.Infof("apply remote snap failed since backup data error: %v", err)
		return err
	}
	err = r.restoreFromPath(r.GetBackupDirForRemote(), term, index)
	return err
}

func (r *RockDB) Restore(term uint64, index uint64) error {
	backupDir := r.GetBackupDir()
	return r.restoreFromPath(backupDir, term, index)
}
func isSameSSTFile(f1 string, f2 string) error {
	stat1, err1 := os.Stat(f1)
	stat2, err2 := os.Stat(f2)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("sst files not match err: %v, %v", err1, err2)
	}
	if stat1.Size() != stat2.Size() {
		return fmt.Errorf("sst files mismatch size: %v, %v", stat1, stat2)
	}
	// sst meta is stored at the footer of file
	// we check 256KB is enough for footer
	rbytes := int64(256 * 1024)
	roffset := stat1.Size() - rbytes
	if roffset < 0 {
		roffset = 0
		rbytes = stat1.Size()
	}
	fs1, err1 := os.Open(f1)
	fs2, err2 := os.Open(f2)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("sst files not match err: %v, %v", err1, err2)
	}
	b1 := make([]byte, rbytes)
	n1, err1 := fs1.ReadAt(b1, roffset)
	if err1 != nil {
		if err1 != io.EOF {
			return fmt.Errorf("read file err: %v", err1)
		}
	}
	b2 := make([]byte, rbytes)
	n2, err2 := fs2.ReadAt(b2, roffset)
	if err1 != nil {
		if err1 != io.EOF {
			return fmt.Errorf("read file err: %v", err1)
		}
	}
	if n2 != n1 {
		return fmt.Errorf("sst file footer not match")
	}
	// TODO: maybe add more check on header and middle of file
	if bytes.Equal(b1[:n1], b2[:n2]) {
		return nil
	}
	return fmt.Errorf("sst file footer not match")
}

func (r *RockDB) restoreFromPath(backupDir string, term uint64, index uint64) error {
	// write meta (snap term and index) and check the meta data in the backup
	r.checkpointDirLock.RLock()
	defer r.checkpointDirLock.RUnlock()
	hasBackup, _ := r.isBackupOKInPath(backupDir, term, index)
	if !hasBackup {
		return errors.New("no backup for restore")
	}

	checkpointDir := GetCheckpointDir(term, index)
	start := time.Now()
	dbLog.Infof("begin restore from checkpoint: %v-%v\n", backupDir, checkpointDir)
	r.closeEng()
	select {
	case <-r.quit:
		return errors.New("db is quiting")
	default:
	}
	// 1. remove all files in current db except sst files
	// 2. get the list of sst in checkpoint
	// 3. remove all the sst files not in the checkpoint list
	// 4. copy all files from checkpoint to current db and do not override sst
	matchName := path.Join(r.GetDataDir(), "*")
	nameList, err := filepath.Glob(matchName)
	if err != nil {
		dbLog.Infof("list files failed:  %v\n", err)
		return err
	}
	ckNameList, err := filepath.Glob(path.Join(backupDir, checkpointDir, "*"))
	if err != nil {
		dbLog.Infof("list checkpoint files failed:  %v\n", err)
		return err
	}
	ckSstNameMap := make(map[string]string)
	for _, fn := range ckNameList {
		if strings.HasSuffix(fn, ".sst") {
			ckSstNameMap[path.Base(fn)] = fn
		}
	}

	for _, fn := range nameList {
		shortName := path.Base(fn)
		if strings.HasPrefix(shortName, "LOG") {
			continue
		}

		if strings.HasSuffix(shortName, ".sst") {
			if fullName, ok := ckSstNameMap[shortName]; ok {
				err = isSameSSTFile(fullName, fn)
				if err == nil {
					dbLog.Infof("keeping sst file: %v", fn)
					continue
				} else {
					dbLog.Infof("no keeping sst file %v for not same: %v", fn, err)
				}
			}
		}
		dbLog.Infof("removing: %v", fn)
		os.RemoveAll(fn)
	}
	for _, fn := range ckNameList {
		if strings.HasPrefix(path.Base(fn), "LOG") {
			dbLog.Infof("ignore copy LOG file: %v", fn)
			continue
		}
		dst := path.Join(r.GetDataDir(), path.Base(fn))
		err := common.CopyFileForHardLink(fn, dst)
		//err := copyFile(fn, dst, false)
		if err != nil {
			dbLog.Infof("copy %v to %v failed: %v", fn, dst, err)
			return err
		} else {
			dbLog.Infof("copy %v to %v done", fn, dst)
		}
	}

	err = r.reOpenEng()
	dbLog.Infof("restore done, cost: %v\n", time.Now().Sub(start))
	if err != nil {
		dbLog.Infof("reopen the restored db failed:  %v\n", err)
	} else {
		keepNum := MaxCheckpointNum
		if r.cfg.KeepBackup > 0 {
			keepNum = r.cfg.KeepBackup
		}
		purgeOldCheckpoint(keepNum, r.GetBackupDir(), atomic.LoadUint64(&r.latestSnapIndex))
		purgeOldCheckpoint(MaxRemoteCheckpointNum, r.GetBackupDirForRemote(), math.MaxUint64-1)
	}
	return err
}

func (r *RockDB) GetIndexSchema(table string) (*common.IndexSchema, error) {
	return r.indexMgr.GetIndexSchemaInfo(r, table)
}

func (r *RockDB) GetAllIndexSchema() (map[string]*common.IndexSchema, error) {
	return r.indexMgr.GetAllIndexSchemaInfo(r)
}

func (r *RockDB) AddHsetIndex(table string, hindex *common.HsetIndexSchema) error {
	indexInfo := HsetIndexInfo{
		Name:       []byte(hindex.Name),
		IndexField: []byte(hindex.IndexField),
		PrefixLen:  hindex.PrefixLen,
		Unique:     hindex.Unique,
		ValueType:  IndexPropertyDType(hindex.ValueType),
		State:      IndexState(hindex.State),
	}
	index := &HsetIndex{
		Table:         []byte(table),
		HsetIndexInfo: indexInfo,
	}
	return r.indexMgr.AddHsetIndex(r, index)
}

func (r *RockDB) UpdateHsetIndexState(table string, hindex *common.HsetIndexSchema) error {
	return r.indexMgr.UpdateHsetIndexState(r, table, hindex.IndexField, IndexState(hindex.State))
}

func (r *RockDB) BeginBatchWrite() error {
	if atomic.CompareAndSwapInt32(&r.isBatching, 0, 1) {
		return nil
	}
	return errors.New("another batching is waiting")
}

func (r *RockDB) MaybeCommitBatch() error {
	if atomic.LoadInt32(&r.isBatching) == 1 {
		return nil
	}
	err := r.rockEng.Write(r.wb)
	r.wb.Clear()
	return err
}

func (r *RockDB) CommitBatchWrite() error {
	err := r.rockEng.Write(r.wb)
	if err != nil {
		dbLog.Infof("commit write error: %v", err)
	}
	r.wb.Clear()
	atomic.StoreInt32(&r.isBatching, 0)
	return err
}

func (r *RockDB) AbortBatch() {
	r.wb.Clear()
	atomic.StoreInt32(&r.isBatching, 0)
}

func IsNeedAbortError(err error) bool {
	// for the error which will not touch write batch no need abort
	// since it will not affect the write buffer in batch
	if err == errTooMuchBatchSize {
		return false
	}
	return true
}

func IsBatchableWrite(cmd string) bool {
	_, ok := batchableCmds[cmd]
	return ok
}

func init() {
	batchableCmds = make(map[string]bool)
	// command need response value (not just error or ok) can not be batched
	// batched command may cause the table count not-exactly.
	// should use MaybeCommitBatch and MaybeClearBatch in command handler
	batchableCmds["set"] = true
	batchableCmds["setex"] = true
	batchableCmds["del"] = true
	batchableCmds["hmset"] = true
}
