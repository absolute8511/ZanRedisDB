package node

import (
	"errors"
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
	DataDir          string
	EngType          string
	ExpirationPolicy common.ExpirationPolicy
	RockOpts         rockredis.RockOptions
}

func NewKVStore(kvopts *KVOptions) (*KVStore, error) {
	s := &KVStore{
		opts: kvopts,
	}

	if err := s.openDB(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *KVStore) openDB() error {
	var err error
	if s.opts.EngType == rockredis.EngType {
		cfg := rockredis.NewRockConfig()
		cfg.DataDir = s.opts.DataDir
		cfg.RockOptions = s.opts.RockOpts
		cfg.ExpirationPolicy = s.opts.ExpirationPolicy
		s.RockDB, err = rockredis.OpenRockDB(cfg)
		if err != nil {
			nodeLog.Warningf("failed to open rocksdb: %v", err)
		}
	} else {
		return errors.New("Not recognized engine type:" + s.opts.EngType)
	}
	return err
}

func (s *KVStore) CleanData() error {
	if s.RockDB == nil {
		nodeLog.Warningf("the db is not opened while clean data")
		return nil
	}
	nodeLog.Infof("the store %v is cleaning data", s.opts.DataDir)
	dataPath := s.GetDataDir()
	s.Close()
	os.RemoveAll(dataPath)

	return s.openDB()
}

// the caller should make sure call this after close/stop finished.
// and no any more operation on this store during and after the destroy
func (s *KVStore) Destroy() error {
	if s.RockDB != nil {
		nodeLog.Infof("the store %v is destroyed", s.opts.DataDir)
		dataPath := s.GetDataDir()
		s.Close()
		return os.RemoveAll(dataPath)
	} else {
		if s.opts.EngType == rockredis.EngType {
			f := rockredis.GetDataDirFromBase(s.opts.DataDir)
			return os.RemoveAll(f)
		}
	}
	return nil
}

func (s *KVStore) CheckExpiredData(buffer common.ExpiredDataBuffer, stop chan struct{}) error {
	if s.opts.EngType == rockredis.EngType {
		return s.RockDB.CheckExpiredData(buffer, stop)
	}
	return nil
}

func (s *KVStore) LocalLookup(key []byte) ([]byte, error) {
	value, err := s.KVGet(key)
	return value, err
}

func (s *KVStore) LocalDelete(key []byte) (int64, error) {
	return s.KVDel(key)
}

func (s *KVStore) LocalPut(ts int64, key []byte, value []byte) error {
	err := s.KVSet(ts, key, value)
	return err
}

func (s *KVStore) LocalWriteBatch(cmd ...common.WriteCmd) error {
	return nil
}

func (s *KVStore) IsBatchableWrite(cmdName string) bool {
	if s.opts.EngType == rockredis.EngType {
		return rockredis.IsBatchableWrite(cmdName)
	} else {
		return false
	}
}

func (s *KVStore) BeginBatchWrite() error {
	if s.opts.EngType == rockredis.EngType {
		return s.RockDB.BeginBatchWrite()
	} else {
		return nil
	}
}

func (s *KVStore) CommitBatchWrite() error {
	if s.opts.EngType == rockredis.EngType {
		return s.RockDB.CommitBatchWrite()
	} else {
		return nil
	}
}
