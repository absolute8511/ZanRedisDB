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

	"github.com/spaolacci/murmur3"

	"github.com/shirou/gopsutil/mem"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

const (
	MaxCheckpointNum       = 10
	MaxRemoteCheckpointNum = 3
	HLLCacheSize           = 512
)

var dbLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("db"))

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

type RockOptions struct {
	VerifyReadChecksum             bool   `json:"verify_read_checksum"`
	BlockSize                      int    `json:"block_size"`
	BlockCache                     int64  `json:"block_cache"`
	CacheIndexAndFilterBlocks      bool   `json:"cache_index_and_filter_blocks"`
	WriteBufferSize                int    `json:"write_buffer_size"`
	MaxWriteBufferNumber           int    `json:"max_write_buffer_number"`
	MinWriteBufferNumberToMerge    int    `json:"min_write_buffer_number_to_merge"`
	Level0FileNumCompactionTrigger int    `json:"level0_file_num_compaction_trigger"`
	MaxBytesForLevelBase           uint64 `json:"max_bytes_for_level_base"`
	TargetFileSizeBase             uint64 `json:"target_file_size_base"`
	MaxBackgroundFlushes           int    `json:"max_background_flushes"`
	MaxBackgroundCompactions       int    `json:"max_background_compactions"`
	MinLevelToCompress             int    `json:"min_level_to_compress"`
	MaxMainifestFileSize           uint64 `json:"max_mainifest_file_size"`
	RateBytesPerSec                int64  `json:"rate_bytes_per_sec"`
	BackgroundHighThread           int    `json:"background_high_thread,omitempty"`
	BackgroundLowThread            int    `json:"background_low_thread,omitempty"`
	AdjustThreadPool               bool   `json:"adjust_thread_pool,omitempty"`
	UseSharedCache                 bool   `json:"use_shared_cache,omitempty"`
	UseSharedRateLimiter           bool   `json:"use_shared_rate_limiter,omitempty"`
	DisableWAL                     bool   `json:"disable_wal,omitempty"`
	DisableMergeCounter            bool   `json:"disable_merge_counter,omitempty"`
}

func FillDefaultOptions(opts *RockOptions) {
	// use large block to reduce index block size for hdd
	// if using ssd, should use the default value
	if opts.BlockSize <= 0 {
		// for hdd use 64KB and above
		// for ssd use 32KB and below
		opts.BlockSize = 1024 * 32
	}
	// should about 20% less than host RAM
	// http://smalldatum.blogspot.com/2016/09/tuning-rocksdb-block-cache.html
	if opts.BlockCache <= 0 {
		v, err := mem.VirtualMemory()
		if err != nil {
			opts.BlockCache = 1024 * 1024 * 128
		} else {
			opts.BlockCache = int64(v.Total / 100)
			if opts.UseSharedCache {
				opts.BlockCache *= 10
			} else {
				if opts.BlockCache < 1024*1024*64 {
					opts.BlockCache = 1024 * 1024 * 64
				} else if opts.BlockCache > 1024*1024*1024*8 {
					opts.BlockCache = 1024 * 1024 * 1024 * 8
				}
			}
		}
	}
	// keep level0_file_num_compaction_trigger * write_buffer_size * min_write_buffer_number_tomerge = max_bytes_for_level_base to minimize write amplification
	if opts.WriteBufferSize <= 0 {
		opts.WriteBufferSize = 1024 * 1024 * 64
	}
	if opts.MaxWriteBufferNumber <= 0 {
		opts.MaxWriteBufferNumber = 6
	}
	if opts.MinWriteBufferNumberToMerge <= 0 {
		opts.MinWriteBufferNumberToMerge = 2
	}
	if opts.Level0FileNumCompactionTrigger <= 0 {
		opts.Level0FileNumCompactionTrigger = 2
	}
	if opts.MaxBytesForLevelBase <= 0 {
		opts.MaxBytesForLevelBase = 1024 * 1024 * 256
	}
	if opts.TargetFileSizeBase <= 0 {
		opts.TargetFileSizeBase = 1024 * 1024 * 64
	}
	if opts.MaxBackgroundFlushes <= 0 {
		opts.MaxBackgroundFlushes = 2
	}
	if opts.MaxBackgroundCompactions <= 0 {
		opts.MaxBackgroundCompactions = 4
	}
	if opts.MinLevelToCompress <= 0 {
		opts.MinLevelToCompress = 3
	}
	if opts.MaxMainifestFileSize <= 0 {
		opts.MaxMainifestFileSize = 1024 * 1024 * 32
	}
	if opts.AdjustThreadPool {
		if opts.BackgroundHighThread <= 0 {
			opts.BackgroundHighThread = 2
		}
		if opts.BackgroundLowThread <= 0 {
			opts.BackgroundLowThread = 4
		}
	}
}

type SharedRockConfig struct {
	SharedCache       *gorocksdb.Cache
	SharedEnv         *gorocksdb.Env
	SharedRateLimiter *gorocksdb.RateLimiter
}
type RockConfig struct {
	DataDir            string
	KeepBackup         int
	EnableTableCounter bool
	// this will ignore all update and non-exist delete
	EstimateTableCounter bool
	ExpirationPolicy     common.ExpirationPolicy
	DefaultReadOpts      *gorocksdb.ReadOptions
	DefaultWriteOpts     *gorocksdb.WriteOptions
	SharedConfig         *SharedRockConfig
	RockOptions
}

func NewRockConfig() *RockConfig {
	c := &RockConfig{
		DefaultReadOpts:      gorocksdb.NewDefaultReadOptions(),
		DefaultWriteOpts:     gorocksdb.NewDefaultWriteOptions(),
		EnableTableCounter:   true,
		EstimateTableCounter: false,
	}
	c.DefaultReadOpts.SetVerifyChecksums(false)
	FillDefaultOptions(&c.RockOptions)
	return c
}

func NewSharedRockConfig(opt RockOptions) *SharedRockConfig {
	rc := &SharedRockConfig{}
	if opt.UseSharedCache {
		if opt.BlockCache <= 0 {
			v, err := mem.VirtualMemory()
			if err != nil {
				opt.BlockCache = 1024 * 1024 * 128 * 10
			} else {
				opt.BlockCache = int64(v.Total / 10)
			}
		}
		rc.SharedCache = gorocksdb.NewLRUCache(opt.BlockCache)
	}
	if opt.AdjustThreadPool {
		rc.SharedEnv = gorocksdb.NewDefaultEnv()
		if opt.BackgroundHighThread <= 0 {
			opt.BackgroundHighThread = 3
		}
		if opt.BackgroundLowThread <= 0 {
			opt.BackgroundLowThread = 6
		}
		rc.SharedEnv.SetBackgroundThreads(opt.BackgroundLowThread)
		rc.SharedEnv.SetHighPriorityBackgroundThreads(opt.BackgroundHighThread)
	}
	if opt.UseSharedRateLimiter && opt.RateBytesPerSec > 0 {
		rc.SharedRateLimiter = gorocksdb.NewGenericRateLimiter(opt.RateBytesPerSec, 100*1000, 10)
	}
	return rc
}

func (src *SharedRockConfig) Destroy() {
	if src.SharedCache != nil {
		src.SharedCache.Destroy()
	}
	if src.SharedEnv != nil {
		src.SharedEnv.Destroy()
	}
	if src.SharedRateLimiter != nil {
		src.SharedRateLimiter.Destroy()
	}
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

func purgeOldCheckpoint(keepNum int, checkpointDir string) {
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
			os.RemoveAll(sortedNameList[i])
			dbLog.Infof("clean checkpoint : %v", sortedNameList[i])
		}
	}
}

type RockDB struct {
	expiration
	cfg               *RockConfig
	eng               *gorocksdb.DB
	dbOpts            *gorocksdb.Options
	defaultWriteOpts  *gorocksdb.WriteOptions
	defaultReadOpts   *gorocksdb.ReadOptions
	wb                *gorocksdb.WriteBatch
	lruCache          *gorocksdb.Cache
	rl                *gorocksdb.RateLimiter
	quit              chan struct{}
	wg                sync.WaitGroup
	backupC           chan *BackupInfo
	engOpened         int32
	indexMgr          *IndexMgr
	isBatching        int32
	checkpointDirLock sync.RWMutex
	hasher64          hash.Hash64
	hllCache          *hllCache
	stopping          int32
}

func OpenRockDB(cfg *RockConfig) (*RockDB, error) {
	if len(cfg.DataDir) == 0 {
		return nil, errors.New("config error")
	}

	if cfg.DisableWAL {
		cfg.DefaultWriteOpts.DisableWAL(true)
	}
	os.MkdirAll(cfg.DataDir, common.DIR_PERM)
	// options need be adjust due to using hdd or sdd, please reference
	// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	// use large block to reduce index block size for hdd
	// if using ssd, should use the default value
	bbto.SetBlockSize(cfg.BlockSize)
	// should about 20% less than host RAM
	// http://smalldatum.blogspot.com/2016/09/tuning-rocksdb-block-cache.html
	var lru *gorocksdb.Cache
	if cfg.RockOptions.UseSharedCache {
		if cfg.SharedConfig == nil || cfg.SharedConfig.SharedCache == nil {
			return nil, errors.New("missing shared cache instance")
		}
		bbto.SetBlockCache(cfg.SharedConfig.SharedCache)
		dbLog.Infof("use shared cache: %v", cfg.SharedConfig.SharedCache)
	} else {
		lru = gorocksdb.NewLRUCache(cfg.BlockCache)
		bbto.SetBlockCache(lru)
	}
	// cache index and filter blocks can save some memory,
	// if not cache, the index and filter will be pre-loaded in memory
	bbto.SetCacheIndexAndFilterBlocks(cfg.CacheIndexAndFilterBlocks)
	// /* filter should not block_based, use sst based to reduce cpu */
	filter := gorocksdb.NewBloomFilter(10, false)
	bbto.SetFilterPolicy(filter)
	opts := gorocksdb.NewDefaultOptions()
	// optimize filter for hit, use less memory since last level will has no bloom filter
	// opts.OptimizeFilterForHits(true)
	opts.SetBlockBasedTableFactory(bbto)
	if cfg.RockOptions.AdjustThreadPool {
		if cfg.SharedConfig == nil || cfg.SharedConfig.SharedEnv == nil {
			return nil, errors.New("missing shared env instance")
		}
		opts.SetEnv(cfg.SharedConfig.SharedEnv)
		dbLog.Infof("use shared env: %v", cfg.SharedConfig.SharedEnv)
	}

	var rl *gorocksdb.RateLimiter
	if cfg.RateBytesPerSec > 0 {
		if cfg.UseSharedRateLimiter {
			if cfg.SharedConfig == nil {
				return nil, errors.New("missing shared instance")
			}
			opts.SetRateLimiter(cfg.SharedConfig.SharedRateLimiter)
			dbLog.Infof("use shared rate limiter: %v", cfg.SharedConfig.SharedRateLimiter)
		} else {
			rl = gorocksdb.NewGenericRateLimiter(cfg.RateBytesPerSec, 100*1000, 10)
			opts.SetRateLimiter(rl)
		}
	}

	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(-1)
	// keep level0_file_num_compaction_trigger * write_buffer_size * min_write_buffer_number_tomerge = max_bytes_for_level_base to minimize write amplification
	opts.SetWriteBufferSize(cfg.WriteBufferSize)
	opts.SetMaxWriteBufferNumber(cfg.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(cfg.MinWriteBufferNumberToMerge)
	opts.SetLevel0FileNumCompactionTrigger(cfg.Level0FileNumCompactionTrigger)
	opts.SetMaxBytesForLevelBase(cfg.MaxBytesForLevelBase)
	opts.SetTargetFileSizeBase(cfg.TargetFileSizeBase)
	opts.SetMaxBackgroundFlushes(cfg.MaxBackgroundFlushes)
	opts.SetMaxBackgroundCompactions(cfg.MaxBackgroundCompactions)
	opts.SetMinLevelToCompress(cfg.MinLevelToCompress)
	// we use table, so we use prefix seek feature
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))
	opts.SetMemtablePrefixBloomSizeRatio(0.1)
	opts.EnableStatistics()
	opts.SetMaxLogFileSize(1024 * 1024 * 32)
	opts.SetLogFileTimeToRoll(3600 * 24 * 3)
	opts.SetMaxManifestFileSize(cfg.MaxMainifestFileSize)
	opts.SetMaxSuccessiveMerges(1000)
	// https://github.com/facebook/mysql-5.6/wiki/my.cnf-tuning
	// rate limiter need to reduce the compaction io
	if !cfg.DisableMergeCounter {
		if cfg.EnableTableCounter {
			opts.SetUint64AddMergeOperator()
		}
	} else {
		cfg.EnableTableCounter = false
	}

	db := &RockDB{
		cfg:              cfg,
		dbOpts:           opts,
		lruCache:         lru,
		rl:               rl,
		defaultReadOpts:  cfg.DefaultReadOpts,
		defaultWriteOpts: cfg.DefaultWriteOpts,
		wb:               gorocksdb.NewWriteBatch(),
		backupC:          make(chan *BackupInfo),
		quit:             make(chan struct{}),
		hasher64:         murmur3.New64(),
	}

	switch cfg.ExpirationPolicy {
	case common.ConsistencyDeletion:
		db.expiration = newConsistencyExpiration(db)

	case common.LocalDeletion:
		db.expiration = newLocalExpiration(db)

	//TODO
	//case common.PeriodicalRotation:
	default:
		return nil, errors.New("unsupported ExpirationPolicy")
	}

	err := db.reOpenEng()
	if err != nil {
		return nil, err
	}

	os.MkdirAll(db.GetBackupDir(), common.DIR_PERM)
	dbLog.Infof("rocksdb opened: %v", db.GetDataDir())

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

func (r *RockDB) CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	if r.cfg.ExpirationPolicy != common.ConsistencyDeletion {
		return fmt.Errorf("can not check expired data at the expiration-policy:%d", r.cfg.ExpirationPolicy)
	}
	return r.expiration.check(buffer, stop)
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

func GetDataDirFromBase(base string) string {
	return path.Join(base, "rocksdb")
}

func (r *RockDB) GetDataDir() string {
	return path.Join(r.cfg.DataDir, "rocksdb")
}

func (r *RockDB) reOpenEng() error {
	var err error
	hcache, err := newHLLCache(HLLCacheSize, r)
	if err != nil {
		return err
	}
	r.hllCache = hcache

	r.eng, err = gorocksdb.OpenDb(r.dbOpts, r.GetDataDir())
	r.indexMgr = NewIndexMgr()
	if err != nil {
		return err
	}
	err = r.indexMgr.LoadIndexes(r)
	if err != nil {
		dbLog.Infof("rocksdb %v load index failed: %v", r.GetDataDir(), err)
		r.eng.Close()
		return err
	}

	r.expiration.Start()
	atomic.StoreInt32(&r.engOpened, 1)
	dbLog.Infof("rocksdb reopened: %v", r.GetDataDir())
	return nil
}

func (r *RockDB) getDBEng() *gorocksdb.DB {
	e := r.eng
	return e
}

func (r *RockDB) getIndexer() *IndexMgr {
	e := r.indexMgr
	return e
}

func (r *RockDB) CompactRange() {
	var rg gorocksdb.Range
	r.eng.CompactRange(rg)
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
			r.eng.CompactRange(rg)
		}
		// compact meta range
		minKey, maxKey, err := getTableMetaRange(dtsMeta[i], []byte(table), nil, nil)
		if err != nil {
			dbLog.Infof("failed to get table %v data range: %v", table, err)
			continue
		}
		var rg gorocksdb.Range
		rg.Start = minKey
		rg.Limit = maxKey
		dbLog.Infof("compacting dt %v meta range: %v, %v", dt, minKey, maxKey)
		r.eng.CompactRange(rg)
	}
}

func (r *RockDB) closeEng() {
	if r.eng != nil {
		if atomic.CompareAndSwapInt32(&r.engOpened, 1, 0) {
			r.hllCache.Flush()
			r.indexMgr.Close()
			r.expiration.Stop()
			r.eng.Close()
			dbLog.Infof("rocksdb engine closed: %v", r.GetDataDir())
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
	if r.defaultReadOpts != nil {
		r.defaultReadOpts.Destroy()
		r.defaultReadOpts = nil
	}
	if r.defaultWriteOpts != nil {
		r.defaultWriteOpts.Destroy()
	}
	if r.wb != nil {
		r.wb.Destroy()
	}
	if r.dbOpts != nil {
		r.dbOpts.Destroy()
		r.dbOpts = nil
	}
	if r.lruCache != nil {
		r.lruCache.Destroy()
		r.lruCache = nil
	}
	if r.rl != nil {
		r.rl.Destroy()
		r.rl = nil
	}
	dbLog.Infof("rocksdb %v closed", r.cfg.DataDir)
}

func (r *RockDB) GetStatistics() string {
	return r.dbOpts.GetStatistics()
}

func getTableDataRange(dt byte, table []byte, start, end []byte) ([]gorocksdb.Range, error) {
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
	rgs := make([]gorocksdb.Range, 0, 2)
	rgs = append(rgs, gorocksdb.Range{Start: minKey, Limit: maxKey})
	if dt == ZSetType {
		// zset has key-score-member data except the key-member data
		zminKey := zEncodeStartKey(table, start)
		var zmaxKey []byte
		if end == nil {
			zmaxKey = encodeDataTableEnd(ZScoreType, []byte(table))
		} else {
			zmaxKey = zEncodeStopKey(table, end)
		}
		rgs = append(rgs, gorocksdb.Range{Start: zminKey, Limit: zmaxKey})
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
	wb := gorocksdb.NewWriteBatch()
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
	err := r.eng.Write(r.defaultWriteOpts, wb)
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
	rgs := make([]gorocksdb.Range, 0, len(dts))
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
		var rgMeta gorocksdb.Range
		rgMeta.Start = minMetaKey
		rgMeta.Limit = maxMetaKey
		rgs = append(rgs, rgMeta)
	}
	sList := r.eng.GetApproximateSizes(rgs, true)
	dbLog.Debugf("range %v sizes: %v", rgs, sList)
	total := uint64(0)
	for _, ss := range sList {
		total += ss
	}
	return int64(total)
}

// [start, end)
func (r *RockDB) GetTableApproximateNumInRange(table string, start []byte, end []byte) int64 {
	numStr := r.eng.GetProperty("rocksdb.estimate-num-keys")
	num, err := strconv.Atoi(numStr)
	if err != nil {
		dbLog.Infof("total keys num error: %v, %v", numStr, err)
		return 0
	}
	if num <= 0 {
		dbLog.Debugf("total keys num zero: %v", numStr)
		return 0
	}
	dts := []byte{KVType, HashType, ListType, SetType, ZSetType}
	dtsMeta := []byte{KVType, HSizeType, LMetaType, SSizeType, ZSizeType}
	rgs := make([]gorocksdb.Range, 0, len(dts))
	for i, dt := range dts {
		// meta range
		minMetaKey, maxMetaKey, err := getTableMetaRange(dtsMeta[i], []byte(table), start, end)
		if err != nil {
			dbLog.Infof("failed to build dt %v meta range: %v", dt, err)
			continue
		}
		var rgMeta gorocksdb.Range
		rgMeta.Start = minMetaKey
		rgMeta.Limit = maxMetaKey
		rgs = append(rgs, rgMeta)
	}
	filteredRgs := make([]gorocksdb.Range, 0, len(dts))
	sList := r.eng.GetApproximateSizes(rgs, true)
	for i, s := range sList {
		if s > 0 {
			filteredRgs = append(filteredRgs, rgs[i])
		}
	}
	keyNum := int64(r.eng.GetApproximateKeyNum(filteredRgs))
	dbLog.Debugf("total db key num: %v, table key num %v, %v", num, keyNum, sList)
	// use GetApproximateSizes and estimate-keys-num in property
	// refer: https://github.com/facebook/mysql-5.6/commit/4ca34d2498e8d16ede73a7955d1ab101a91f102f
	// range records = estimate-keys-num * GetApproximateSizes(range) / GetApproximateSizes (total)
	// use GetPropertiesOfTablesInRange to get number of keys in sst
	return int64(keyNum)
}

func (r *RockDB) GetInternalStatus() map[string]interface{} {
	status := make(map[string]interface{})
	bbt := r.dbOpts.GetBlockBasedTableFactory()
	if bbt != nil {
		bc := bbt.GetBlockCache()
		if bc != nil {
			status["block-cache-usage"] = bc.GetUsage()
			status["block-cache-pinned-usage"] = bc.GetPinnedUsage()
		}
	}

	memStr := r.eng.GetProperty("rocksdb.estimate-table-readers-mem")
	status["estimate-table-readers-mem"] = memStr
	memStr = r.eng.GetProperty("rocksdb.cur-size-all-mem-tables")
	status["cur-size-all-mem-tables"] = memStr
	memStr = r.eng.GetProperty("rocksdb.cur-size-active-mem-table")
	status["cur-size-active-mem-tables"] = memStr
	return status
}

func (r *RockDB) GetInternalPropertyStatus(p string) string {
	return r.eng.GetProperty(p)
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
				ck, err := gorocksdb.NewCheckpoint(r.eng)
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
				r.eng.RLock()
				if r.eng.IsOpened() {
					time.AfterFunc(time.Millisecond*10, func() {
						close(rsp.started)
					})
					err = ck.Save(rsp.backupDir, math.MaxUint64)
				} else {
					err = errors.New("db engine closed")
				}
				r.eng.RUnlock()
				r.checkpointDirLock.Unlock()
				if err != nil {
					dbLog.Infof("save checkpoint failed: %v", err)
					rsp.err = err
					return
				}
				cost := time.Now().Sub(start)
				dbLog.Infof("backup done (cost %v), check point to: %v\n", cost.String(), rsp.backupDir)
				// purge some old checkpoint
				r.checkpointDirLock.Lock()
				keepNum := MaxCheckpointNum
				if r.cfg.KeepBackup > 0 {
					keepNum = r.cfg.KeepBackup
				}
				purgeOldCheckpoint(keepNum, r.GetBackupDir())
				purgeOldCheckpoint(MaxRemoteCheckpointNum, r.GetBackupDirForRemote())
				r.checkpointDirLock.Unlock()
			}()
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
	dbLog.Infof("begin check local checkpoint : %v", fullPath)
	defer dbLog.Infof("check local checkpoint : %v done", fullPath)
	ro := *r.dbOpts
	ro.SetCreateIfMissing(false)
	db, err := gorocksdb.OpenDbForReadOnly(&ro, fullPath, false)
	if err != nil {
		dbLog.Infof("checkpoint open failed: %v", err)
		return false, err
	}
	db.Close()
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
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err != nil {
		out.Close()
		return err
	}
	err = out.Sync()
	if err != nil {
		out.Close()
		return err
	}
	return out.Close()
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
					dbLog.Infof("no keeping sst file %v for not same: %v, %v", fn, err)
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
		err := copyFile(fn, dst, false)
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
		purgeOldCheckpoint(keepNum, r.GetBackupDir())
		purgeOldCheckpoint(MaxRemoteCheckpointNum, r.GetBackupDirForRemote())
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
		r.wb.Clear()
		return nil
	}
	return errors.New("another batching is waiting")
}

func (r *RockDB) MaybeClearBatch() {
	if atomic.LoadInt32(&r.isBatching) == 1 {
		return
	}
	r.wb.Clear()
}

func (r *RockDB) MaybeCommitBatch() error {
	if atomic.LoadInt32(&r.isBatching) == 1 {
		return nil
	}
	return r.eng.Write(r.defaultWriteOpts, r.wb)
}

func (r *RockDB) CommitBatchWrite() error {
	err := r.eng.Write(r.defaultWriteOpts, r.wb)
	if err != nil {
		dbLog.Infof("commit write error: %v", err)
	}
	atomic.StoreInt32(&r.isBatching, 0)
	return err
}

func IsBatchableWrite(cmd string) bool {
	_, ok := batchableCmds[cmd]
	return ok
}

func SetPerfLevel(level int) {
	if level <= 0 || level > 4 {
		DisablePerfLevel()
		return
	}
	gorocksdb.SetPerfLevel(gorocksdb.PerfLevel(level))
}

func IsPerfEnabledLevel(lv int) bool {
	if lv <= 0 || lv > 4 {
		return false
	}
	return lv != gorocksdb.PerfDisable
}

func DisablePerfLevel() {
	gorocksdb.SetPerfLevel(gorocksdb.PerfDisable)
}

func init() {
	batchableCmds = make(map[string]bool)
	// command need response value (not just error or ok) can not be batched
	// batched command may cause the table count not-exactly.
	batchableCmds["set"] = true
	batchableCmds["setex"] = true
	batchableCmds["del"] = true
	batchableCmds["hmset"] = true
}
