package rockredis

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
	"github.com/shirou/gopsutil/mem"
)

const (
	MAX_CHECKPOINT_NUM = 10
)

var dbLog = common.NewLevelLogger(common.LOG_INFO, common.NewDefaultLogger("db"))

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
}

func FillDefaultOptions(opts *RockOptions) {
	// use large block to reduce index block size for hdd
	// if using ssd, should use the default value
	if opts.BlockSize <= 0 {
		opts.BlockSize = 1024 * 64
	}
	// should about 20% less than host RAM
	// http://smalldatum.blogspot.com/2016/09/tuning-rocksdb-block-cache.html
	if opts.BlockCache <= 0 {
		v, err := mem.VirtualMemory()
		if err != nil {
			opts.BlockCache = 1024 * 1024 * 128
		} else {
			opts.BlockCache = int64(v.Total / 100)
			if opts.BlockCache < 1024*1024*64 {
				opts.BlockCache = 1024 * 1024 * 64
			} else if opts.BlockCache > 1024*1024*1024*8 {
				opts.BlockCache = 1024 * 1024 * 1024 * 8
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
}

type RockConfig struct {
	DataDir            string
	EnableTableCounter bool
	// this will ignore all update and non-exist delete
	EstimateTableCounter bool
	ExpirationPolicy     common.ExpirationPolicy
	DefaultReadOpts      *gorocksdb.ReadOptions
	DefaultWriteOpts     *gorocksdb.WriteOptions
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
	cfg              *RockConfig
	eng              *gorocksdb.DB
	dbOpts           *gorocksdb.Options
	defaultWriteOpts *gorocksdb.WriteOptions
	defaultReadOpts  *gorocksdb.ReadOptions
	wb               *gorocksdb.WriteBatch
	lruCache         *gorocksdb.Cache
	quit             chan struct{}
	wg               sync.WaitGroup
	backupC          chan *BackupInfo
	engOpened        int32
	isBatching       int32
}

func OpenRockDB(cfg *RockConfig) (*RockDB, error) {
	if len(cfg.DataDir) == 0 {
		return nil, errors.New("config error")
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
	lru := gorocksdb.NewLRUCache(cfg.BlockCache)
	bbto.SetBlockCache(lru)
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

	if cfg.RateBytesPerSec > 0 {
		rateLimiter := gorocksdb.NewGenericRateLimiter(cfg.RateBytesPerSec)
		opts.SetRateLimiter(rateLimiter)
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
	// https://github.com/facebook/mysql-5.6/wiki/my.cnf-tuning
	// rate limiter need to reduce the compaction io
	opts.SetUint64AddMergeOperator()

	db := &RockDB{
		cfg:              cfg,
		dbOpts:           opts,
		lruCache:         lru,
		defaultReadOpts:  cfg.DefaultReadOpts,
		defaultWriteOpts: cfg.DefaultWriteOpts,
		wb:               gorocksdb.NewWriteBatch(),
		backupC:          make(chan *BackupInfo),
		quit:             make(chan struct{}),
	}
	eng, err := gorocksdb.OpenDb(opts, db.GetDataDir())
	if err != nil {
		return nil, err
	}
	db.eng = eng
	atomic.StoreInt32(&db.engOpened, 1)
	os.MkdirAll(db.GetBackupDir(), common.DIR_PERM)
	dbLog.Infof("rocksdb opened: %v", db.GetDataDir())

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

	db.expiration.Start()

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

func (r *RockDB) CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	if r.cfg.ExpirationPolicy != common.ConsistencyDeletion {
		return fmt.Errorf("can not check expired data at the expiration-policy:%d", r.cfg.ExpirationPolicy)
	}
	return r.expiration.check(buffer, stop)
}

func (r *RockDB) GetBackupBase() string {
	return r.cfg.DataDir
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

func (r *RockDB) TheSameDBWithOtherWriteBatch(wb *gorocksdb.WriteBatch) *RockDB {
	return &RockDB{
		expiration:       r.expiration,
		cfg:              r.cfg,
		eng:              r.eng,
		dbOpts:           r.dbOpts,
		defaultWriteOpts: r.defaultWriteOpts,
		defaultReadOpts:  r.defaultReadOpts,
		wb:               wb,
	}
}

func (r *RockDB) reOpenEng() error {
	var err error
	r.eng, err = gorocksdb.OpenDb(r.dbOpts, r.GetDataDir())
	if err == nil {
		atomic.StoreInt32(&r.engOpened, 1)
		dbLog.Infof("rocksdb opened: %v", r.GetDataDir())
	}
	return err
}

func (r *RockDB) CompactRange() {
	var rg gorocksdb.Range
	r.eng.CompactRange(rg)
}

func (r *RockDB) closeEng() {
	if r.eng != nil {
		if atomic.CompareAndSwapInt32(&r.engOpened, 1, 0) {
			r.eng.Close()
			dbLog.Infof("rocksdb closed: %v", r.GetDataDir())
		}
	}
}

func (r *RockDB) Close() {
	select {
	case <-r.quit:
		// already closed
		return
	default:
	}
	close(r.quit)
	r.expiration.Stop()
	r.wg.Wait()
	r.closeEng()
	if r.defaultReadOpts != nil {
		r.defaultReadOpts.Destroy()
		r.defaultReadOpts = nil
	}
	if r.defaultWriteOpts != nil {
		r.defaultWriteOpts.Destroy()
	}
	if r.dbOpts != nil {
		r.dbOpts.Destroy()
		r.dbOpts = nil
	}
	if r.lruCache != nil {
		r.lruCache.Destroy()
		r.lruCache = nil
	}
	dbLog.Infof("rocksdb %v closed", r.cfg.DataDir)
}

func (r *RockDB) SetPerfLevel(level int) {
	// TODO:
}

func (r *RockDB) GetStatistics() string {
	return r.dbOpts.GetStatistics()
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
				defer close(rsp.done)
				dbLog.Infof("begin backup to:%v \n", rsp.backupDir)
				start := time.Now()
				ck, err := gorocksdb.NewCheckpoint(r.eng)
				if err != nil {
					dbLog.Infof("init checkpoint failed: %v", err)
					rsp.err = err
					return
				}
				_, err = os.Stat(rsp.backupDir)
				if !os.IsNotExist(err) {
					dbLog.Infof("checkpoint exist: %v, remove it", rsp.backupDir)
					os.RemoveAll(rsp.backupDir)
				}
				time.AfterFunc(time.Second*2, func() {
					close(rsp.started)
				})
				err = ck.Save(rsp.backupDir)
				if err != nil {
					dbLog.Infof("save checkpoint failed: %v", err)
					rsp.err = err
					return
				}
				cost := time.Now().Sub(start)
				dbLog.Infof("backup done (cost %v), check point to: %v\n", cost.String(), rsp.backupDir)
				// purge some old checkpoint
				rsp.rsp = []byte(rsp.backupDir)
				purgeOldCheckpoint(MAX_CHECKPOINT_NUM, r.GetBackupDir())
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
	select {
	case r.backupC <- bi:
	default:
		return nil
	}
	return bi
}

func (r *RockDB) IsLocalBackupOK(term uint64, index uint64) (bool, error) {
	backupDir := r.GetBackupDir()
	checkpointDir := GetCheckpointDir(term, index)
	ro := *r.dbOpts
	ro.SetCreateIfMissing(false)
	db, err := gorocksdb.OpenDbForReadOnly(&ro, path.Join(backupDir, checkpointDir), false)
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

func (r *RockDB) Restore(term uint64, index uint64) error {
	// write meta (snap term and index) and check the meta data in the backup
	backupDir := r.GetBackupDir()
	hasBackup, _ := r.IsLocalBackupOK(term, index)
	if !hasBackup {
		return errors.New("no backup for restore")
	}

	checkpointDir := GetCheckpointDir(term, index)
	start := time.Now()
	dbLog.Infof("begin restore from checkpoint: %v\n", checkpointDir)
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
				stat1, err1 := os.Stat(fullName)
				stat2, err2 := os.Stat(fn)
				if err1 == nil && err2 == nil {
					if stat1.Size() == stat2.Size() {
						dbLog.Infof("keeping sst file: %v", fn)
						continue
					} else {
						dbLog.Infof("no keeping sst file %v for mismatch size: %v, %v", fn, stat1, stat2)
					}
				} else {
					dbLog.Infof("no keeping sst file %v for err: %v, %v", fn, err1, err2)
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
	}
	return err
}

func (r *RockDB) ClearBackup(term uint64, index uint64) error {
	backupDir := r.GetBackupDir()
	checkpointDir := GetCheckpointDir(term, index)
	return os.RemoveAll(path.Join(backupDir, checkpointDir))
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

func init() {
	batchableCmds = make(map[string]bool)
	// command need response value (not just error or ok) can not be batched
	//batchableCmds["incr"] = true
	batchableCmds["set"] = true
	batchableCmds["mset"] = true
	//batchableCmds["setnx"] = true
	batchableCmds["setex"] = true
	//batchableCmds["setrange"] = true
	//batchableCmds["append"] = true
	//batchableCmds["persist"] = true
	batchableCmds["del"] = true
}
