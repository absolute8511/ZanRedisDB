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
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	err := st.Snapshotter.SaveSnap(snap)
	if err != nil {
		return err
	}
	return st.WAL.SaveSnapshot(walsnap)
}

// Release releases resources older than the given snap and are no longer needed:
// - releases the locks to the wal files that are older than the provided wal for the given snap.
// - deletes any .snap.db files that are older than the given snap.
func (st *raftPersistStorage) Release(snap raftpb.Snapshot) error {
	if err := st.WAL.ReleaseLockTo(snap.Metadata.Index); err != nil {
		return err
	}
	return st.Snapshotter.ReleaseSnapDBs(snap)
}
