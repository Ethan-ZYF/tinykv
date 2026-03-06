package raft

import (
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/stretchr/testify/assert"
)

// 构造一个简单的 MemoryStorage，预设一些日志条目
// entries 从 index=1 开始，index=0 是 dummy
func newTestLog(entries []pb.Entry) *RaftLog {
	storage := NewMemoryStorage()
	storage.Append(entries)
	return newLog(storage)
}

// ─────────────────────────────────────────
// LastIndex
// ─────────────────────────────────────────

// 空 storage，LastIndex 应该返回 0
func TestRaftLogLastIndexEmpty(t *testing.T) {
	storage := NewMemoryStorage()
	log := newLog(storage)
	assert.Equal(t, uint64(0), log.LastIndex())
}

// storage 里有 3 条日志，LastIndex 应该返回 3
func TestRaftLogLastIndexFromStorage(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})
	assert.Equal(t, uint64(3), log.LastIndex())
}

// storage 有 3 条，再 append 2 条到内存，LastIndex 应该返回 5
func TestRaftLogLastIndexWithUnstable(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})
	log.entries = append(log.entries, pb.Entry{Index: 4, Term: 2})
	log.entries = append(log.entries, pb.Entry{Index: 5, Term: 3})
	assert.Equal(t, uint64(5), log.LastIndex())
}

// ─────────────────────────────────────────
// Term
// ─────────────────────────────────────────

// index=0 是 dummy entry，term 应该是 0
func TestRaftLogTermDummy(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
	})
	term, err := log.Term(0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), term)
}

// 从 storage 里读 term
func TestRaftLogTermFromStorage(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})
	term, err := log.Term(2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), term)
}

// 从 unstable entries 里读 term
func TestRaftLogTermFromUnstable(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	// 追加一条 unstable entry
	log.entries = append(log.entries, pb.Entry{Index: 3, Term: 3})
	term, err := log.Term(3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), term)
}

// 越界 index，应该返回 error
func TestRaftLogTermOutOfRange(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
	})
	_, err := log.Term(99)
	assert.Error(t, err)
}

// ─────────────────────────────────────────
// allEntries
// ─────────────────────────────────────────

// 空 storage，allEntries 返回空
func TestRaftLogAllEntriesEmpty(t *testing.T) {
	storage := NewMemoryStorage()
	log := newLog(storage)
	assert.Empty(t, log.allEntries())
}

// allEntries 不包含 dummy entry
func TestRaftLogAllEntriesExcludesDummy(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})
	ents := log.allEntries()
	assert.Equal(t, 3, len(ents))
	assert.Equal(t, uint64(1), ents[0].Index) // 从真实日志开始，不含 dummy
}

// storage + unstable 都包含在 allEntries 里
func TestRaftLogAllEntriesIncludesUnstable(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	log.entries = append(log.entries, pb.Entry{Index: 3, Term: 2})
	ents := log.allEntries()
	assert.Equal(t, 3, len(ents))
	assert.Equal(t, uint64(3), ents[2].Index)
}

// ─────────────────────────────────────────
// unstableEntries
// ─────────────────────────────────────────

// 没有 unstable entries，返回空
func TestRaftLogUnstableEntriesEmpty(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	// stabled 应该等于 LastIndex，所以没有 unstable
	assert.Empty(t, log.unstableEntries())
}

// append 了新 entry 之后，unstable 应该包含它
func TestRaftLogUnstableEntriesAfterAppend(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	log.entries = append(log.entries, pb.Entry{Index: 3, Term: 2})
	log.entries = append(log.entries, pb.Entry{Index: 4, Term: 2})
	// stabled 还是 2，unstable 应该是 index 3 和 4
	ents := log.unstableEntries()
	assert.Equal(t, 2, len(ents))
	assert.Equal(t, uint64(3), ents[0].Index)
	assert.Equal(t, uint64(4), ents[1].Index)
}

// ─────────────────────────────────────────
// nextEnts
// ─────────────────────────────────────────

// applied == committed，没有待 apply 的，返回空
func TestRaftLogNextEntsEmpty(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
	})
	log.applied = 2
	log.committed = 2
	assert.Empty(t, log.nextEnts())
}

// committed 推进了，nextEnts 应该返回 applied+1 到 committed 的部分
func TestRaftLogNextEntsBasic(t *testing.T) {
	log := newTestLog([]pb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
	})
	log.applied = 1
	log.committed = 3
	ents := log.nextEnts()
	assert.Equal(t, 2, len(ents))
	assert.Equal(t, uint64(2), ents[0].Index)
	assert.Equal(t, uint64(3), ents[1].Index)
}
