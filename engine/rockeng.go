package engine

import (
	"errors"
	"os"
	"path"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/mem"
	"github.com/youzan/ZanRedisDB/common"
	"github.com/youzan/gorocksdb"
)

const (
	compactThreshold = 500000
)

var dbLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("rocksdb_eng"))

func SetLogLevel(level int32) {
	dbLog.SetLevel(level)
}

func SetLogger(level int32, logger common.Logger) {
	dbLog.SetLevel(level)
	dbLog.Logger = logger
}

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
	OptimizeFiltersForHits         bool   `json:"optimize_filters_for_hits,omitempty"`
	InsertHintFixedLen             int    `json:"insert_hint_fixed_len"`
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

type RockEngConfig struct {
	DataDir            string
	SharedConfig       *SharedRockConfig
	EnableTableCounter bool
	AutoCompacted      bool
	RockOptions
}

func NewRockConfig() *RockEngConfig {
	c := &RockEngConfig{
		EnableTableCounter: true,
	}
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

type RockEng struct {
	cfg         *RockEngConfig
	eng         *gorocksdb.DB
	dbOpts      *gorocksdb.Options
	lruCache    *gorocksdb.Cache
	rl          *gorocksdb.RateLimiter
	engOpened   int32
	lastCompact int64
	deletedCnt  int64
	quit        chan struct{}
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
	// set pin_l0_filter_and_index_blocks_in_cache = true if cache index is true to improve performance on read
	// and see this https://github.com/facebook/rocksdb/pull/3692 if partitioned filter is on
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)

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

	err := os.MkdirAll(cfg.DataDir, common.DIR_PERM)
	if err != nil {
		return nil, err
	}
	db := &RockEng{
		cfg:      cfg,
		dbOpts:   opts,
		lruCache: lru,
		rl:       rl,
		quit:     make(chan struct{}),
	}
	if cfg.AutoCompacted {
		go db.compactLoop()
	}
	return db, nil
}

func (r *RockEng) compactLoop() {
	ticker := time.NewTicker(time.Hour)
	interval := (time.Hour / time.Second).Nanoseconds()
	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			if r.DeletedBeforeCompact() > compactThreshold && (time.Now().Unix()-r.LastCompactTime()) > interval {
				r.CompactRange()
			}
		}
	}
}

func (r *RockEng) GetOpts() *gorocksdb.Options {
	return r.dbOpts
}

func (r *RockEng) GetDataDir() string {
	return path.Join(r.cfg.DataDir, "rocksdb")
}

func (r *RockEng) OpenEng() error {
	if atomic.LoadInt32(&r.engOpened) == 1 {
		dbLog.Warningf("rocksdb engine already opened: %v, should close it before reopen", r.GetDataDir())
		return errors.New("rocksdb open failed since not closed")
	}
	eng, err := gorocksdb.OpenDb(r.dbOpts, r.GetDataDir())
	if err != nil {
		return err
	}
	r.eng = eng
	atomic.StoreInt32(&r.engOpened, 1)
	dbLog.Infof("rocksdb engine opened: %v", r.GetDataDir())
	return nil
}

func (r *RockEng) Eng() *gorocksdb.DB {
	e := r.eng
	return e
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

func (r *RockEng) CompactRange() {
	atomic.StoreInt64(&r.lastCompact, time.Now().Unix())
	atomic.StoreInt64(&r.deletedCnt, 0)
	var rg gorocksdb.Range
	r.eng.CompactRange(rg)
	dbLog.Infof("compact rocksdb %v done", r.GetDataDir())
}

func (r *RockEng) CloseEng() bool {
	if r.eng != nil {
		if atomic.CompareAndSwapInt32(&r.engOpened, 1, 0) {
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
