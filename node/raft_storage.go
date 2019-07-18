package node

import (
	"errors"

	"github.com/youzan/ZanRedisDB/raft/raftpb"
	"github.com/youzan/ZanRedisDB/snap"
	"github.com/youzan/ZanRedisDB/wal"
	"github.com/youzan/ZanRedisDB/wal/walpb"
)

type raftPersistStorage struct {
	*wal.WAL
	*snap.Snapshotter
}

func NewRaftPersistStorage(w *wal.WAL, s *snap.Snapshotter) IRaftPersistStorage {
	return &raftPersistStorage{w, s}
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *raftPersistStorage) SaveSnap(snap raftpb.Snapshot) error {
	if enableSnapSaveTest {
		return errors.New("failed to save snapshot to raft in failed test")
	}
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	err := st.WAL.SaveSnapshot(walsnap)
	if err != nil {
		return err
	}
	err = st.Snapshotter.SaveSnap(snap)
	if err != nil {
		return err
	}
	return st.WAL.ReleaseLockTo(snap.Metadata.Index)
}
