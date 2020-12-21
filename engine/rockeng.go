package engine

import (
	"errors"
	"math"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/mem"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

type rockRefSlice struct {
	v *gorocksdb.Slice
}

func (rs *rockRefSlice) Free() {
	rs.v.Free()
}

func (rs *rockRefSlice) Bytes() []byte {
	return rs.v.Bytes()
}

func (rs *rockRefSlice) Data() []byte {
	return rs.v.Data()
}

type sharedRockConfig struct {
	SharedCache       *gorocksdb.Cache
	SharedEnv         *gorocksdb.Env
	SharedRateLimiter *gorocksdb.RateLimiter
}

func newSharedRockConfig(opt RockOptions) *sharedRockConfig {
	rc := &sharedRockConfig{}
	if opt.UseSharedCache {
		if opt.BlockCache <= 0 {
			v, err := mem.VirtualMemory()
			if err != nil {
				opt.BlockCache = 1024 * 1024 * 128 * 10
			} else {
				opt.BlockCache = int64(v.Total / 10)
				if opt.CacheIndexAndFilterBlocks || opt.EnablePartitionedIndexFilter {
					opt.BlockCache *= 2
				}
			}
		}
		rc.SharedCache = gorocksdb.NewLRUCache(opt.BlockCache)
	}
	if opt.AdjustThreadPool {
		rc.SharedEnv = gorocksdb.NewDefaultEnv()
		if opt.BackgroundHighThread <= 0 {
			opt.BackgroundHighThread = 2
		}
		if opt.BackgroundLowThread <= 0 {
			opt.BackgroundLowThread = 16
		}
		rc.SharedEnv.SetBackgroundThreads(opt.BackgroundLowThread)
		rc.SharedEnv.SetHighPriorityBackgroundThreads(opt.BackgroundHighThread)
	}
	if opt.UseSharedRateLimiter && opt.RateBytesPerSec > 0 {
		rc.SharedRateLimiter = gorocksdb.NewGenericRateLimiter(opt.RateBytesPerSec, 100*1000, 10)
	}
	return rc
}

func (src *sharedRockConfig) ChangeLimiter(bytesPerSec int64) {
	limiter := src.SharedRateLimiter
	if limiter == nil {
		return
	}
	limiter.SetBytesPerSecond(bytesPerSec)
}

func (src *sharedRockConfig) Destroy() {
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

type RockEng struct {
	cfg              *RockEngConfig
	eng              *gorocksdb.DB
	dbOpts           *gorocksdb.Options
	defaultWriteOpts *gorocksdb.WriteOptions
	defaultReadOpts  *gorocksdb.ReadOptions
	wb               *rocksWriteBatch
	lruCache         *gorocksdb.Cache
	rl               *gorocksdb.RateLimiter
	engOpened        int32
	lastCompact      int64
	deletedCnt       int64
	quit             chan struct{}
}

func NewRockEng(cfg *RockEngConfig) (*RockEng, error) {
	if len(cfg.DataDir) == 0 {
		return nil, errors.New("config error")
	}

	//if cfg.DisableWAL {
	//	cfg.DefaultWriteOpts.DisableWAL(true)
	//}
	// options need be adjust due to using hdd or sdd, please reference
	// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	// use large block to reduce index block size for hdd
	// if using ssd, should use the default value
	bbto.SetBlockSize(cfg.BlockSize)
	// should about 20% less than host RAM
	// http://smalldatum.blogspot.com/2016/09/tuning-rocksdb-block-cache.html
	var lru *gorocksdb.Cache
	sharedConfig, _ := cfg.SharedConfig.(*sharedRockConfig)
	if cfg.RockOptions.UseSharedCache {
		if sharedConfig == nil || sharedConfig.SharedCache == nil {
			return nil, errors.New("missing shared cache instance")
		}
		bbto.SetBlockCache(sharedConfig.SharedCache)
		dbLog.Infof("use shared cache: %v", sharedConfig.SharedCache)
	} else {
		lru = gorocksdb.NewLRUCache(cfg.BlockCache)
		bbto.SetBlockCache(lru)
	}
	// cache index and filter blocks can save some memory,
	// if not cache, the index and filter will be pre-loaded in memory
	bbto.SetCacheIndexAndFilterBlocks(cfg.CacheIndexAndFilterBlocks)
	// set pin_l0_filter_and_index_blocks_in_cache = true if cache index is true to improve performance on read
	// and see this https://github.com/facebook/rocksdb/pull/3692 if partitioned filter is on
	// TODO: no need set after 6.15
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)

	if cfg.EnablePartitionedIndexFilter {
		//enable partitioned indexes and partitioned filters
		bbto.SetCacheIndexAndFilterBlocksWithHighPriority(true)
		bbto.SetIndexType(gorocksdb.IndexTypeTwoLevelIndexSearch)
		bbto.SetPartitionFilters(true)
		bbto.SetMetaDataBlockSize(1024 * 8)
		bbto.SetCacheIndexAndFilterBlocks(true)
	}
	// TODO: set to 5 for version after 6.6
	bbto.SetFormatVersion(4)
	bbto.SetIndexBlockRestartInterval(16)

	// /* filter should not block_based, use sst based to reduce cpu */
	filter := gorocksdb.NewBloomFilter(10, false)
	bbto.SetFilterPolicy(filter)
	opts := gorocksdb.NewDefaultOptions()
	// optimize filter for hit, use less memory since last level will has no bloom filter
	// If you're certain that Get() will mostly find a key you're looking for, you can set options.optimize_filters_for_hits = true
	// to save memory usage for bloom filters
	if cfg.OptimizeFiltersForHits {
		opts.OptimizeFilterForHits(true)
	}
	opts.SetBlockBasedTableFactory(bbto)
	if cfg.RockOptions.AdjustThreadPool {
		if cfg.SharedConfig == nil || sharedConfig.SharedEnv == nil {
			return nil, errors.New("missing shared env instance")
		}
		opts.SetEnv(sharedConfig.SharedEnv)
		dbLog.Infof("use shared env: %v", sharedConfig.SharedEnv)
	}

	var rl *gorocksdb.RateLimiter
	if cfg.RateBytesPerSec > 0 {
		if cfg.UseSharedRateLimiter {
			if cfg.SharedConfig == nil {
				return nil, errors.New("missing shared instance")
			}
			opts.SetRateLimiter(sharedConfig.SharedRateLimiter)
			dbLog.Infof("use shared rate limiter: %v", sharedConfig.SharedRateLimiter)
		} else {
			rl = gorocksdb.NewGenericRateLimiter(cfg.RateBytesPerSec, 100*1000, 10)
			opts.SetRateLimiter(rl)
		}
	}

	if cfg.InsertHintFixedLen > 0 {
		opts.SetMemtableInsertWithHintFixedLengthPrefixExtractor(cfg.InsertHintFixedLen)
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
	// TODO: add avoid_unnecessary_blocking_io option for db after 6.14

	// See http://smalldatum.blogspot.com/2018/09/5-things-to-set-when-configuring.html
	// level_compaction_dynamic_level_bytes
	if cfg.LevelCompactionDynamicLevelBytes {
		opts.SetLevelCompactionDynamicLevelBytes(true)
	}

	if !cfg.ReadOnly {
		err := os.MkdirAll(cfg.DataDir, common.DIR_PERM)
		if err != nil {
			return nil, err
		}
	}
	db := &RockEng{
		cfg:              cfg,
		dbOpts:           opts,
		lruCache:         lru,
		rl:               rl,
		defaultWriteOpts: gorocksdb.NewDefaultWriteOptions(),
		defaultReadOpts:  gorocksdb.NewDefaultReadOptions(),
		quit:             make(chan struct{}),
	}
	db.defaultReadOpts.SetVerifyChecksums(false)
	if cfg.DisableWAL {
		db.defaultWriteOpts.DisableWAL(true)
	}
	if cfg.AutoCompacted {
		go db.compactLoop()
	}
	return db, nil
}

func (r *RockEng) SetCompactionFilter(filter ICompactFilter) {
	r.dbOpts.SetCompactionFilter(filter)
}

func (r *RockEng) SetMaxBackgroundOptions(maxCompact int, maxBackJobs int) error {
	/*
			all options we can change is in MutableDBOptions
		  struct MutableDBOptions {
		  int max_background_jobs;
		  int base_background_compactions;
		  int max_background_compactions;
		  bool avoid_flush_during_shutdown;
		  size_t writable_file_max_buffer_size;
		  uint64_t delayed_write_rate;
		  uint64_t max_total_wal_size;
		  uint64_t delete_obsolete_files_period_micros;
		  unsigned int stats_dump_period_sec;
		  int max_open_files;
		  uint64_t bytes_per_sync;
		  uint64_t wal_bytes_per_sync;
		  size_t compaction_readahead_size;
		};
	*/
	keys := []string{}
	values := []string{}
	if maxCompact > 0 {
		r.dbOpts.SetMaxBackgroundCompactions(maxCompact)
		keys = append(keys, "max_background_compactions")
		values = append(values, strconv.Itoa(maxCompact))
	}
	if maxBackJobs > 0 {
		r.dbOpts.SetMaxBackgroundFlushes(maxBackJobs)
		keys = append(keys, "max_background_jobs")
		values = append(values, strconv.Itoa(maxBackJobs))
	}
	if len(keys) == 0 {
		return nil
	}
	return r.eng.SetDBOptions(keys, values)
}

func (r *RockEng) compactLoop() {
	ticker := time.NewTicker(time.Hour)
	interval := (time.Hour / time.Second).Nanoseconds()
	dbLog.Infof("start auto compact loop : %v", interval)
	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			if (r.DeletedBeforeCompact() > compactThreshold) &&
				(time.Now().Unix()-r.LastCompactTime()) > interval {
				dbLog.Infof("auto compact : %v, %v", r.DeletedBeforeCompact(), r.LastCompactTime())
				r.CompactAllRange()
			}
		}
	}
}

// NewWriteBatch init a new write batch for write, should only call this after the engine opened
func (r *RockEng) NewWriteBatch() WriteBatch {
	if r.eng == nil {
		panic("nil engine, should only get write batch after db opened")
	}
	return newRocksWriteBatch(r.eng, r.defaultWriteOpts)
}

// DefaultWriteBatch return the internal default write batch for write, should only call this after the engine opened
// and can not be used concurrently
func (r *RockEng) DefaultWriteBatch() WriteBatch {
	if r.wb == nil {
		panic("nil default write batch, should only get write batch after db opened")
	}
	return r.wb
}

func (r *RockEng) SetOptsForLogStorage() {
	r.defaultReadOpts.SetVerifyChecksums(false)
	r.defaultReadOpts.SetFillCache(false)
	// read raft always hit non-deleted range
	r.defaultReadOpts.SetIgnoreRangeDeletions(true)
	r.defaultWriteOpts.DisableWAL(true)
}

func (r *RockEng) GetOpts() *gorocksdb.Options {
	return r.dbOpts
}

func (r *RockEng) GetDataDir() string {
	return path.Join(r.cfg.DataDir, "rocksdb")
}

func (r *RockEng) CheckDBEngForRead(fullPath string) error {
	ro := *(r.GetOpts())
	ro.SetCreateIfMissing(false)
	db, err := gorocksdb.OpenDbForReadOnly(&ro, fullPath, false)
	if err != nil {
		return err
	}
	db.Close()
	return nil
}

func (r *RockEng) OpenEng() error {
	if !r.IsClosed() {
		dbLog.Warningf("rocksdb engine already opened: %v, should close it before reopen", r.GetDataDir())
		return errors.New("rocksdb open failed since not closed")
	}
	if r.cfg.ReadOnly {
		ro := *(r.GetOpts())
		ro.SetCreateIfMissing(false)
		dfile := r.GetDataDir()
		if r.cfg.DataTool {
			_, err := os.Stat(dfile)
			if os.IsNotExist(err) {
				dfile = r.cfg.DataDir
			}
		}
		dbLog.Infof("rocksdb engine open %v as read only", dfile)
		eng, err := gorocksdb.OpenDbForReadOnly(&ro, dfile, false)
		if err != nil {
			return err
		}
		r.eng = eng
	} else {
		eng, err := gorocksdb.OpenDb(r.dbOpts, r.GetDataDir())
		if err != nil {
			return err
		}
		r.eng = eng
	}
	r.wb = newRocksWriteBatch(r.eng, r.defaultWriteOpts)
	atomic.StoreInt32(&r.engOpened, 1)
	dbLog.Infof("rocksdb engine opened: %v", r.GetDataDir())
	return nil
}

func (r *RockEng) Write(wb WriteBatch) error {
	return wb.Commit()
}

func (r *RockEng) DeletedBeforeCompact() int64 {
	return atomic.LoadInt64(&r.deletedCnt)
}

func (r *RockEng) AddDeletedCnt(c int64) {
	atomic.AddInt64(&r.deletedCnt, c)
}

func (r *RockEng) LastCompactTime() int64 {
	return atomic.LoadInt64(&r.lastCompact)
}

func (r *RockEng) CompactRange(rg CRange) {
	atomic.StoreInt64(&r.lastCompact, time.Now().Unix())
	dbLog.Infof("compact rocksdb %v begin: %v", r.GetDataDir(), rg)
	defer dbLog.Infof("compact rocksdb %v done", r.GetDataDir())
	var rrg gorocksdb.Range
	rrg.Start = rg.Start
	rrg.Limit = rg.Limit
	r.eng.CompactRange(rrg)
}

func (r *RockEng) CompactAllRange() {
	atomic.StoreInt64(&r.deletedCnt, 0)
	r.CompactRange(CRange{})
}

func (r *RockEng) DisableManualCompact(disable bool) {
	//TODO: rocksdb 6.5 will support the disable and enable manual compaction
	if disable {
		//r.eng.DisableManualCompact(disable)
	} else {
		//r.eng.EnableManualCompact(disable)
	}
}

func (r *RockEng) GetApproximateTotalKeyNum() int {
	numStr := r.eng.GetProperty("rocksdb.estimate-num-keys")
	num, err := strconv.Atoi(numStr)
	if err != nil {
		dbLog.Infof("total keys num error: %v, %v", numStr, err)
		return 0
	}
	return num
}

func (r *RockEng) GetApproximateKeyNum(ranges []CRange) uint64 {
	rgs := make([]gorocksdb.Range, 0, len(ranges))
	for _, r := range ranges {
		rgs = append(rgs, gorocksdb.Range{Start: r.Start, Limit: r.Limit})
	}
	return r.eng.GetApproximateKeyNum(rgs)
}

func (r *RockEng) GetApproximateSizes(ranges []CRange, includeMem bool) []uint64 {
	rgs := make([]gorocksdb.Range, 0, len(ranges))
	for _, r := range ranges {
		rgs = append(rgs, gorocksdb.Range{Start: r.Start, Limit: r.Limit})
	}
	return r.eng.GetApproximateSizes(rgs, includeMem)
}

func (r *RockEng) IsClosed() bool {
	if atomic.LoadInt32(&r.engOpened) == 0 {
		return true
	}
	return false
}

func (r *RockEng) CloseEng() bool {
	if r.eng != nil {
		if atomic.CompareAndSwapInt32(&r.engOpened, 1, 0) {
			if r.wb != nil {
				r.wb.Destroy()
			}
			r.eng.PreShutdown()
			r.eng.Close()
			dbLog.Infof("rocksdb engine closed: %v", r.GetDataDir())
			return true
		}
	}
	return false
}

func (r *RockEng) CloseAll() {
	select {
	case <-r.quit:
	default:
		close(r.quit)
	}
	r.CloseEng()
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

	if r.defaultWriteOpts != nil {
		r.defaultWriteOpts.Destroy()
	}
	if r.defaultReadOpts != nil {
		r.defaultReadOpts.Destroy()
	}
}

func (r *RockEng) GetStatistics() string {
	return r.dbOpts.GetStatistics()
}

func (r *RockEng) GetInternalStatus() map[string]interface{} {
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

func (r *RockEng) GetInternalPropertyStatus(p string) string {
	return r.eng.GetProperty(p)
}

func (r *RockEng) GetBytesNoLock(key []byte) ([]byte, error) {
	return r.eng.GetBytesNoLock(r.defaultReadOpts, key)
}

func (r *RockEng) GetBytes(key []byte) ([]byte, error) {
	return r.eng.GetBytes(r.defaultReadOpts, key)
}

func (r *RockEng) MultiGetBytes(keyList [][]byte, values [][]byte, errs []error) {
	r.eng.MultiGetBytes(r.defaultReadOpts, keyList, values, errs)
}

func (r *RockEng) Exist(key []byte) (bool, error) {
	return r.eng.Exist(r.defaultReadOpts, key)
}

func (r *RockEng) ExistNoLock(key []byte) (bool, error) {
	return r.eng.ExistNoLock(r.defaultReadOpts, key)
}

func (r *RockEng) GetRefNoLock(key []byte) (RefSlice, error) {
	v, err := r.eng.GetNoLock(r.defaultReadOpts, key)
	if err != nil {
		return nil, err
	}
	return &rockRefSlice{v: v}, nil
}

func (r *RockEng) GetRef(key []byte) (RefSlice, error) {
	v, err := r.eng.Get(r.defaultReadOpts, key)
	if err != nil {
		return nil, err
	}
	return &rockRefSlice{v: v}, nil
}

func (r *RockEng) GetValueWithOp(key []byte,
	op func([]byte) error) error {
	val, err := r.eng.Get(r.defaultReadOpts, key)
	if err != nil {
		return err
	}
	defer val.Free()
	return op(val.Data())
}

func (r *RockEng) GetValueWithOpNoLock(key []byte,
	op func([]byte) error) error {
	val, err := r.eng.GetNoLock(r.defaultReadOpts, key)
	if err != nil {
		return err
	}
	defer val.Free()
	return op(val.Data())
}

func (r *RockEng) GetIterator(opts IteratorOpts) (Iterator, error) {
	dbit, err := newRockIterator(r.eng, true, opts)
	if err != nil {
		return nil, err
	}
	return dbit, nil
}

func (r *RockEng) DeleteFilesInRange(rg CRange) {
	var rrg gorocksdb.Range
	rrg.Start = rg.Start
	rrg.Limit = rg.Limit
	r.eng.DeleteFilesInRange(rrg)
}

func (r *RockEng) NewCheckpoint() (KVCheckpoint, error) {
	ck, err := gorocksdb.NewCheckpoint(r.eng)
	if err != nil {
		return nil, err
	}
	return &rockEngCheckpoint{
		ck:  ck,
		eng: r.eng,
	}, nil
}

type rockEngCheckpoint struct {
	ck  *gorocksdb.Checkpoint
	eng *gorocksdb.DB
}

func (rck *rockEngCheckpoint) Save(path string, notify chan struct{}) error {
	rck.eng.RLock()
	defer rck.eng.RUnlock()
	if rck.eng.IsOpened() {
		if notify != nil {
			time.AfterFunc(time.Millisecond*20, func() {
				close(notify)
			})
		}
		return rck.ck.Save(path, math.MaxUint64)
	}
	return errDBEngClosed
}
