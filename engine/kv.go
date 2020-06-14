package engine

import (
	"github.com/shirou/gopsutil/mem"
	"github.com/youzan/ZanRedisDB/common"
)

type RefSlice interface {
	Data() []byte
	Free()
}

const (
	compactThreshold = 5000000
)

var dbLog = common.NewLevelLogger(common.LOG_INFO, common.NewGLogger())

func SetLogLevel(level int32) {
	dbLog.SetLevel(level)
}

func SetLogger(level int32, logger common.Logger) {
	dbLog.SetLevel(level)
	dbLog.Logger = logger
}

type CRange struct {
	Start []byte
	Limit []byte
}

type SharedRockConfig interface {
	Destroy()
	ChangeLimiter(bytesPerSec int64)
}

type RockOptions struct {
	VerifyReadChecksum             bool   `json:"verify_read_checksum"`
	BlockSize                      int    `json:"block_size"`
	BlockCache                     int64  `json:"block_cache"`
	CacheIndexAndFilterBlocks      bool   `json:"cache_index_and_filter_blocks"`
	EnablePartitionedIndexFilter   bool   `json:"enable_partitioned_index_filter"`
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
		opts.BlockSize = 1024 * 8
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
		opts.MaxBackgroundCompactions = 8
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
			opts.BackgroundLowThread = 16
		}
	}
}

type KVCheckpoint interface {
	Save(path string, notify chan struct{}) error
}
