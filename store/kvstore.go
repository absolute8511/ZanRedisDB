package store

import (
	"errors"
	"log"
	"os"
	"path"

	"github.com/absolute8511/ZanRedisDB/common"
	"github.com/absolute8511/ZanRedisDB/rockredis"
)

// a key-value store
type KVStore struct {
	*rockredis.RockDB
	opts *KVOptions
}

type KVOptions struct {
	DataDir string
	EngType string
}

func NewKVStore(kvopts *KVOptions) *KVStore {
	s := &KVStore{
		opts: kvopts,
	}

	err := s.openDB()
	if err != nil {
		panic(err)
	}
	return s
}

func (s *KVStore) openDB() error {
	var err error
	if s.opts.EngType == "rocksdb" {
		dir := path.Join(s.opts.DataDir, "rocksdb")
		os.MkdirAll(dir, common.DIR_PERM)
		cfg := rockredis.NewRockConfig()
		cfg.DataDir = dir
		s.RockDB, err = rockredis.OpenRockDB(cfg)
	} else {
		return errors.New("Not recognized engine type")
	}
	return err
}

func (s *KVStore) Clear() error {
	s.Close()
	os.RemoveAll(path.Join(s.opts.DataDir, "rocksdb"))
	return s.openDB()
}

func (s *KVStore) LocalLookup(key []byte) ([]byte, error) {
	value, err := s.KVGet(key)
	return value, err
}

func (s *KVStore) LocalDelete(key []byte) error {
	return s.KVDel(key)
}

func (s *KVStore) LocalPut(key []byte, value []byte) error {
	err := s.KVSet(key, value)
	if err != nil {
		log.Printf("failed to write key %v to db: %v\n", string(key), err)
	}
	return err
}

func (s *KVStore) LocalWriteBatch(cmd ...common.WriteCmd) error {
	return nil
}

func (s *KVStore) GetBackupBase() string {
	return s.opts.DataDir
}

func (s *KVStore) GetBackupDir() string {
	return path.Join(s.opts.DataDir, "rocksdb_backup")
}

func (s *KVStore) BackupStore() ([]byte, error) {
	// use the rocksdb backup/checkpoint interface to backup data
	return s.Backup(s.GetBackupDir())
}

func (s *KVStore) IsBackupOK(meta []byte) (bool, error) {
	return s.IsLocalBackupOK(s.GetBackupDir(), meta)
}

func (s *KVStore) ClearStoreBackup(meta []byte) error {
	return s.ClearBackup(s.GetBackupDir(), meta)
}

func (s *KVStore) RestoreStore(meta []byte) error {
	return s.Restore(s.GetBackupDir(), meta)
}
