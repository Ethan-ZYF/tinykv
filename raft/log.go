// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)

	return &RaftLog{
		storage:   storage,
		committed: firstIndex - 1,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}

	// 如果所有日志都已持久化
	if l.stabled >= l.LastIndex() {
		return []pb.Entry{}
	}

	// 找到第一个未持久化的日志位置
	firstUnstable := l.stabled + 1
	firstIndex := l.entries[0].Index

	return l.entries[firstUnstable-firstIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}

	// 如果没有新的已提交日志
	if l.applied >= l.committed {
		return []pb.Entry{}
	}

	firstIndex := l.entries[0].Index

	// 返回 (applied, committed] 区间的日志
	start := l.applied + 1 - firstIndex
	end := l.committed + 1 - firstIndex

	return l.entries[start:end]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		lastIndex, _ := l.storage.LastIndex()
		return lastIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// 如果 entries 为空，从 storage 获取
	if len(l.entries) == 0 {
		return l.storage.Term(i)
	}

	firstIndex := l.entries[0].Index
	lastIndex := l.LastIndex()

	// 检查索引范围
	if i < firstIndex {
		// 在 storage 中（已压缩的部分）
		return l.storage.Term(i)
	}

	if i > lastIndex {
		// 超出范围
		return 0, ErrUnavailable
	}

	// 在 entries 中
	return l.entries[i-firstIndex].Term, nil
}

// appendEntries appends new entries to the log
func (l *RaftLog) appendEntries(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}

	firstNewIndex := entries[0].Index

	if len(l.entries) == 0 {
		// 当前没有日志，直接添加
		l.entries = entries
		return
	}

	firstIndex := l.entries[0].Index
	lastIndex := l.LastIndex()

	// 如果新日志完全在当前日志之后
	if firstNewIndex > lastIndex {
		l.entries = append(l.entries, entries...)
		return
	}

	// 有重叠，需要检查冲突
	// 遍历新日志，找到第一个冲突的位置
	for i, entry := range entries {
		idx := entry.Index

		// 如果这个索引超出了我们的日志范围，直接追加剩余的所有日志
		if idx > lastIndex {
			l.entries = append(l.entries, entries[i:]...)
			return
		}

		// 检查这个位置的日志是否冲突（同索引不同任期）
		existingTerm, _ := l.Term(idx)
		if existingTerm != entry.Term {
			// 发现冲突！从这里截断并追加所有剩余的新日志
			// 如果截断位置在stable范围内，需要更新stabled
			if idx <= l.stabled {
				l.stabled = idx - 1
			}
			offset := idx - firstIndex
			l.entries = append(l.entries[:offset], entries[i:]...)
			return
		}
	}

	// 没有冲突，所有新日志都已经存在且匹配
}
