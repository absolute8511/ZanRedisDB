package rockredis

import (
	"encoding/json"
	"errors"
	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/gorocksdb"
	"log"
	"os"
	"sync"
	"time"
)

type RockConfig struct {
	DataDir          string
	DefaultReadOpts  *gorocksdb.ReadOptions
	DefaultWriteOpts *gorocksdb.WriteOptions
}

func NewRockConfig() *RockConfig {
	return &RockConfig{
		DefaultReadOpts:  gorocksdb.NewDefaultReadOptions(),
		DefaultWriteOpts: gorocksdb.NewDefaultWriteOptions(),
	}
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
}

func OpenRockDB(cfg *RockConfig) (*RockDB, error) {
	if len(cfg.DataDir) == 0 {
		return nil, errors.New("config error")
	}

	os.MkdirAll(cfg.DataDir, 0755)
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
	opts.SetMaxBytesForLevelBase(1024 * 1024 * 1024)
	opts.SetMinWriteBufferNumberToMerge(2)
	opts.SetTargetFileSizeBase(1024 * 1024 * 64)
	opts.SetMaxBackgroundFlushes(2)
	opts.SetMaxBackgroundCompactions(4)
	// we use table, so we use prefix seek feature
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))
	//opts.SetMemtablePrefixBloomSizeRatio(0.1)
	opts.EnableStatistics()
	// https://github.com/facebook/mysql-5.6/wiki/my.cnf-tuning
	// rate limiter need to reduce the compaction io

	eng, err := gorocksdb.OpenDb(opts, cfg.DataDir)
	if err != nil {
		return nil, err
	}

	db := &RockDB{
		cfg:              cfg,
		dbOpts:           opts,
		eng:              eng,
		defaultReadOpts:  cfg.DefaultReadOpts,
		defaultWriteOpts: cfg.DefaultWriteOpts,
		wb:               gorocksdb.NewWriteBatch(),
	}

	db.quit = make(chan struct{})
	return db, nil
}

func (r *RockDB) reOpen() error {
	var err error
	r.eng, err = gorocksdb.OpenDb(r.dbOpts, r.cfg.DataDir)
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

func (r *RockDB) GetStatistics() string {
	return r.dbOpts.GetStatistics()
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

func (r *RockDB) Backup(backupDir string) ([]byte, error) {
	opts := gorocksdb.NewDefaultOptions()
	log.Printf("begin backup \n")
	start := time.Now()
	be, err := gorocksdb.OpenBackupEngine(opts, backupDir)
	if err != nil {
		log.Printf("backup engine failed: %v", err)
		return nil, err
	}
	err = be.CreateNewBackup(r.eng)
	if err != nil {
		log.Printf("backup failed: %v", err)
		return nil, err
	}
	cost := time.Now().Sub(start)
	beInfo := be.GetInfo()

	log.Printf("backup done (cost %v), total backup : %v\n", cost.String(), beInfo.GetCount())

	lastID := beInfo.GetBackupId(beInfo.GetCount() - 1)
	for i := 0; i < beInfo.GetCount(); i++ {
		id := beInfo.GetBackupId(i)
		log.Printf("backup data :%v, timestamp: %v, files: %v, size: %v", id, beInfo.GetTimestamp(i), beInfo.GetNumFiles(i),
			beInfo.GetSize(i))
	}
	beInfo.Destroy()
	be.PurgeOldBackups(r.eng, 3)
	be.Close()
	d, _ := json.Marshal(lastID)
	return d, nil
}

func (r *RockDB) IsLocalBackupOK(backupDir string, metaData []byte) (bool, error) {
	var backupID int64
	err := json.Unmarshal(metaData, &backupID)
	if err != nil {
		return false, err
	}
	opts := gorocksdb.NewDefaultOptions()
	be, err := gorocksdb.OpenBackupEngine(opts, backupDir)
	if err != nil {
		log.Printf("backup engine open failed: %v", err)
		return false, err
	}
	beInfo := be.GetInfo()
	lastID := int64(0)
	if beInfo.GetCount() > 0 {
		lastID = beInfo.GetBackupId(beInfo.GetCount() - 1)
	}
	log.Printf("local total backup : %v, last: %v\n", beInfo.GetCount(), lastID)
	hasBackup := false
	for i := 0; i < beInfo.GetCount(); i++ {
		id := beInfo.GetBackupId(i)
		log.Printf("backup data :%v, timestamp: %v, files: %v, size: %v", id, beInfo.GetTimestamp(i), beInfo.GetNumFiles(i),
			beInfo.GetSize(i))
		if id == backupID {
			hasBackup = true
			break
		}
	}
	return hasBackup, nil
}

func (r *RockDB) Restore(backupDir string, metaData []byte) error {
	hasBackup, _ := r.IsLocalBackupOK(backupDir, metaData)
	if !hasBackup {
		return errors.New("no backup for restore")
	}
	var backupID int64
	err := json.Unmarshal(metaData, &backupID)
	if err != nil {
		return err
	}

	opts := gorocksdb.NewDefaultOptions()
	be, err := gorocksdb.OpenBackupEngine(opts, backupDir)
	if err != nil {
		log.Printf("backup engine open failed: %v", err)
		return err
	}
	beInfo := be.GetInfo()
	lastID := int64(0)
	if beInfo.GetCount() > 0 {
		lastID = beInfo.GetBackupId(beInfo.GetCount() - 1)
	}
	log.Printf("after sync total backup : %v, last: %v\n", beInfo.GetCount(), lastID)
	if lastID < backupID {
		return errors.New("no backup for restore")
	}
	if lastID > backupID {
		for i := 0; i < beInfo.GetCount(); i++ {
			id := beInfo.GetBackupId(i)
			log.Printf("backup data :%v, timestamp: %v, files: %v, size: %v", id, beInfo.GetTimestamp(i), beInfo.GetNumFiles(i),
				beInfo.GetSize(i))
			if int64(i) > backupID {
				// TODO: delete the backup with new id
			}
		}
	}

	start := time.Now()
	log.Printf("begin restore\n")
	r.eng.Close()
	restoreOpts := gorocksdb.NewRestoreOptions()
	err = be.RestoreDBFromLatestBackup(r.cfg.DataDir, r.cfg.DataDir, restoreOpts)
	if err != nil {
		log.Printf("restore failed: %v\n", err)
		return err
	}
	log.Printf("restore done, cost: %v\n", time.Now().Sub(start))
	be.Close()
	return r.reOpen()
}

func (r *RockDB) ClearBackup(backupDir string, metaData []byte) error {
	var backupID int64
	err := json.Unmarshal(metaData, &backupID)
	if err != nil {
		return err
	}
	opts := gorocksdb.NewDefaultOptions()
	be, err := gorocksdb.OpenBackupEngine(opts, backupDir)
	if err != nil {
		log.Printf("backup engine open failed: %v", err)
		return err
	}
	beInfo := be.GetInfo()
	lastID := int64(0)
	if beInfo.GetCount() > 0 {
		lastID = beInfo.GetBackupId(beInfo.GetCount() - 1)
	}
	if lastID >= backupID {
		be.PurgeOldBackups(r.eng, 0)
	}
	beInfo.Destroy()
	be.Close()
	return nil
}
