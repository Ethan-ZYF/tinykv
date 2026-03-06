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

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	// 2. 读取 firstIndex 到 lastIndex 的所有日志
	entries, _ := storage.Entries(firstIndex, lastIndex+1)

	// 然后手动把 dummy entry 加到最前面
	// dummy 的 index = firstIndex-1，term 从 storage.Term(firstIndex-1) 获取
	dummyTerm, _ := storage.Term(firstIndex - 1)
	dummy := pb.Entry{Index: firstIndex - 1, Term: dummyTerm}
	entries = append([]pb.Entry{dummy}, entries...)

	// 3. 读取 HardState 获取 committed
	hardState, _, _ := storage.InitialState()

	// 4. 组装 RaftLog
	return &RaftLog{
		storage:   storage,
		entries:   entries, // 注意：entries[0] 是 dummy entry
		committed: hardState.Commit,
		applied:   firstIndex - 1, // applied 初始等于 snapshot 的 lastIndex
		stabled:   lastIndex,      // storage 里的都是已持久化的
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
	if len(l.entries) <= 1 {
		return nil
	}
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	offset := l.FirstIndex()
	if l.stabled+1 < offset {
		return nil
	}
	return l.entries[l.stabled+1-offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return nil
	}
	offset := l.FirstIndex()
	return l.entries[l.applied+1-offset : l.committed+1-offset]
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) != 0 {
		return l.entries[0].GetIndex()
	}
	return 0
}

// commit 消息, 更新 tocommit 消息
// 有以下多种场景都会更新 commit
// 1. heartbeat 	: 收到 leader 心跳信息的时候
// 2. maybeAppend 	: 收到 leader append oplog 消息的时候
// 3. maybeCommit	: leader 更新 commit 信息
func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Fatalf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		log.Debugf("l.committed: %v -> tocommit: %v", l.committed, tocommit)
		l.committed = tocommit
	}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].GetIndex()
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if len(l.entries) > 0 && i >= l.FirstIndex() {
		if i > l.LastIndex() {
			return 0, ErrUnavailable
		}
		offset := i - l.FirstIndex()
		return l.entries[offset].GetTerm(), nil
	}
	// entries 里没有，去 storage 找
	term, err := l.storage.Term(i)
	return term, err
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
// 这个函数有点误导，对于那种越界的，完全是新的，也是直接返回 index 的；
func (l *RaftLog) findConflict(ents []pb.Entry) uint64 {
	// 遍历 ents
	for _, ne := range ents {
		// 判断 term 是否对的上，如果对不上，那么继续判断消息的索引是否比当前最后一条小，如果小，那么说明是冲突的
		// 返回冲突的位置
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				log.Debugf("found conflict at index %d [conflicting term: %d]",
					ne.Index, ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	firstIndex := l.FirstIndex()

	if after <= l.LastIndex() {
		// 截断从 after 开始的所有本地日志
		l.entries = l.entries[:after-firstIndex]
		// stabled 也要回退
		l.stabled = min(l.stabled, after-1)
	}

	// 追加新 entries
	l.entries = append(l.entries, ents...)
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
// index 当前已经复制了的最后一条日志的 index
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	// 判断 index 所对应的日志是否 term 匹配
	if l.matchTerm(index, logTerm) {
		// index 是之前的索引编号（最后一个）
		// lastnewi 是 last new index 的缩写，这个是指添加 ents 之后，最后一个的索引
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		// 没有冲突
		case ci == 0:
		// 非预期，新来的日志，竟然又跳到已经 commit 过的前面去了
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			// 新日志也是走这个
			// 有冲突的场景，裁剪
			offset := index + 1
			// 裁剪，append 消息到 raftLog 中
			l.truncateAndAppend(ents[ci-offset:])
		}
		// 更新本地 commit 信息
		// committed 这个是 leader 传过来的，那么就一定是多数节点 commit 过的消息
		log.Debugf("committed: %v, lastnewi: %v", committed, lastnewi)
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}
