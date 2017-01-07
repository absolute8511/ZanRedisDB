package rockredis

import (
	"errors"
	"fmt"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func GetCheckpointDir(term uint64, index uint64) string {
	return fmt.Sprintf("%016x-%016x", term, index)
}

type RockConfig struct {
	DataDir          string
	DefaultReadOpts  *gorocksdb.ReadOptions
	DefaultWriteOpts *gorocksdb.WriteOptions
}

func NewRockConfig() *RockConfig {
	c := &RockConfig{
		DefaultReadOpts:  gorocksdb.NewDefaultReadOptions(),
		DefaultWriteOpts: gorocksdb.NewDefaultWriteOptions(),
	}
	c.DefaultReadOpts.SetVerifyChecksums(false)
	return c
}

type RockDB struct {
	cfg              *RockConfig
	eng              *gorocksdb.DB
	dbOpts           *gorocksdb.Options
	defaultWriteOpts *gorocksdb.WriteOptions
	defaultReadOpts  *gorocksdb.ReadOptions
	wb               *gorocksdb.WriteBatch
	quit             chan struct{}
	wg               sync.WaitGroup
	backupC          chan *BackupInfo
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
	bbto.SetBlockSize(1024 * 16)
	// should about 20% less than host RAM
	// http://smalldatum.blogspot.com/2016/09/tuning-rocksdb-block-cache.html
	bbto.SetBlockCache(gorocksdb.NewLRUCache(1024 * 1024 * 1024))
	// for hdd , we nee cache index and filter blocks
	bbto.SetCacheIndexAndFilterBlocks(true)
	filter := gorocksdb.NewBloomFilter(10)
	bbto.SetFilterPolicy(filter)
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetMaxOpenFiles(-1)
	// keep level0_file_num_compaction_trigger * write_buffer_size = max_bytes_for_level_base to minimize write amplification
	opts.SetWriteBufferSize(1024 * 1024 * 128)
	opts.SetMaxWriteBufferNumber(8)
	opts.SetLevel0FileNumCompactionTrigger(4)
	opts.SetMaxBytesForLevelBase(1024 * 1024 * 1024 * 2)
	opts.SetMinWriteBufferNumberToMerge(2)
	opts.SetTargetFileSizeBase(1024 * 1024 * 128)
	opts.SetMaxBackgroundFlushes(2)
	opts.SetMaxBackgroundCompactions(4)
	opts.SetMinLevelToCompress(3)
	// we use table, so we use prefix seek feature
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))
	opts.SetMemtablePrefixBloomSizeRatio(0.1)
	opts.EnableStatistics()
	opts.SetMaxLogFileSize(1024 * 1024 * 32)
	opts.SetLogFileTimeToRoll(3600 * 24 * 3)
	// https://github.com/facebook/mysql-5.6/wiki/my.cnf-tuning
	// rate limiter need to reduce the compaction io

	db := &RockDB{
		cfg:              cfg,
		dbOpts:           opts,
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
	os.MkdirAll(db.GetBackupDir(), common.DIR_PERM)

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

func (r *RockDB) GetBackupBase() string {
	return r.cfg.DataDir
}

func (r *RockDB) GetBackupDir() string {
	return GetBackupDir(r.cfg.DataDir)
}

func (r *RockDB) GetDataDir() string {
	return path.Join(r.cfg.DataDir, "rocksdb")
}

func (r *RockDB) reOpen() error {
	var err error
	r.eng, err = gorocksdb.OpenDb(r.dbOpts, r.GetDataDir())
	return err
}

func (r *RockDB) CompactRange() {
	var rg gorocksdb.Range
	r.eng.CompactRange(rg)
}

func (r *RockDB) Close() {
	close(r.quit)
	r.wg.Wait()
	if r.defaultReadOpts != nil {
		r.defaultReadOpts.Destroy()
	}
	if r.defaultWriteOpts != nil {
		r.defaultWriteOpts.Destroy()
	}
	if r.eng != nil {
		r.eng.Close()
	}
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

func (r *RockDB) ReadRange(sKey, eKey []byte, maxNum int) chan common.KVRecord {
	retChan := make(chan common.KVRecord, 32)
	go func() {
		it := NewDBRangeLimitIterator(r.eng, sKey, eKey, RangeClose, 0, maxNum, false)
		defer it.Close()
		for it = it; it.Valid(); it.Next() {
			key := it.Key()
			value := it.Value()
			retChan <- common.KVRecord{Key: key, Value: value}
		}
		close(retChan)
	}()
	return retChan
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
				log.Printf("begin backup to:%v \n", rsp.backupDir)
				start := time.Now()
				ck, err := gorocksdb.NewCheckpoint(r.eng)
				if err != nil {
					log.Printf("init checkpoint failed: %v", err)
					rsp.err = err
					return
				}
				_, err = os.Stat(rsp.backupDir)
				if !os.IsNotExist(err) {
					log.Printf("checkpoint exist: %v, remove it", rsp.backupDir)
					os.RemoveAll(rsp.backupDir)
				}
				time.AfterFunc(time.Second*2, func() {
					close(rsp.started)
				})
				err = ck.Save(rsp.backupDir)
				if err != nil {
					log.Printf("save checkpoint failed: %v", err)
					rsp.err = err
					return
				}
				cost := time.Now().Sub(start)
				log.Printf("backup done (cost %v), check point to: %v\n", cost.String(), rsp.backupDir)
				// TODO: purge some old checkpoint
				rsp.rsp = []byte(rsp.backupDir)
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
		log.Printf("checkpoint open failed: %v", err)
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
	// TODO: maybe write meta (snap term and index) and check the meta data in the backup
	backupDir := r.GetBackupDir()
	hasBackup, _ := r.IsLocalBackupOK(term, index)
	if !hasBackup {
		return errors.New("no backup for restore")
	}

	checkpointDir := GetCheckpointDir(term, index)
	start := time.Now()
	log.Printf("begin restore from checkpoint: %v\n", checkpointDir)
	r.eng.Close()
	// 1. remove all files in current db except sst files
	// 2. get the list of sst in checkpoint
	// 3. remove all the sst files not in the checkpoint list
	// 4. copy all files from checkpoint to current db and do not override sst
	matchName := path.Join(r.GetDataDir(), "*")
	nameList, err := filepath.Glob(matchName)
	if err != nil {
		log.Printf("list files failed:  %v\n", err)
		return err
	}
	ckNameList, err := filepath.Glob(path.Join(backupDir, checkpointDir, "*"))
	if err != nil {
		log.Printf("list checkpoint files failed:  %v\n", err)
		return err
	}
	ckSstNameMap := make(map[string]bool)
	for _, fn := range ckNameList {
		if strings.HasSuffix(fn, ".sst") {
			ckSstNameMap[fn] = true
		}
	}

	for _, fn := range nameList {
		shortName := path.Base(fn)
		if strings.HasPrefix(shortName, "LOG") {
			continue
		}
		if strings.HasSuffix(fn, ".sst") {
			if _, ok := ckSstNameMap[fn]; ok {
				log.Printf("keeping sst file: %v", fn)
				continue
			}
		}
		log.Printf("removing: %v", fn)
		os.RemoveAll(fn)
	}
	for _, fn := range ckNameList {
		dst := path.Join(r.GetDataDir(), path.Base(fn))
		err := copyFile(fn, dst, false)
		if err != nil {
			log.Printf("copy %v to %v failed: %v", fn, dst, err)
			return err
		} else {
			log.Printf("copy %v to %v done", fn, dst)
		}
	}

	err = r.reOpen()
	log.Printf("restore done, cost: %v\n", time.Now().Sub(start))
	if err != nil {
		log.Printf("reopen the restored db failed:  %v\n", err)
	}
	return err
}

func (r *RockDB) ClearBackup(term uint64, index uint64) error {
	backupDir := r.GetBackupDir()
	checkpointDir := GetCheckpointDir(term, index)
	return os.RemoveAll(path.Join(backupDir, checkpointDir))
}
