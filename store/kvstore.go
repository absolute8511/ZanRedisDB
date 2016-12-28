package store

import (
	"errors"
	"log"
	"os"

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
		cfg := rockredis.NewRockConfig()
		cfg.DataDir = s.opts.DataDir
		s.RockDB, err = rockredis.OpenRockDB(cfg)
	} else {
		return errors.New("Not recognized engine type:" + s.opts.EngType)
	}
	return err
}

func (s *KVStore) Clear() error {
	s.Close()
	os.RemoveAll(s.GetDataDir())
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
