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
	"errors"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
	// PendingSnapshot is true when a snapshot has been sent to this peer and we
	// are waiting for an acknowledgement. While true, sendAppend skips this peer
	// to avoid a snapshot storm.
	PendingSnapshot        bool
	PendingSnapshotElapsed int
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// Randomized election Timout rand[electionTimeout, 2*electionTimeout)
	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rlog := newLog(c.Storage)
	if c.Applied > 0 {
		rlog.applied = c.Applied
	}
	raft := &Raft{
		id:               c.ID,
		RaftLog:          rlog,
		Prs:              make(map[uint64]*Progress),
		heartbeatElapsed: 0,
		electionElapsed:  0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	hardState, confState, _ := c.Storage.InitialState()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{
			Next:  raft.RaftLog.LastIndex() + 1,
			Match: 0,
		}
	}
	raft.becomeFollower(hardState.Term, None)
	raft.Vote = hardState.Vote
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	// Don't send anything while a snapshot is in flight to avoid a snapshot
	// storm. The pending flag is cleared when we get an AppendResponse.
	if pr.PendingSnapshot {
		return false
	}
	prevLogIndex := pr.Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err == ErrCompacted {
		snapshot, err := r.RaftLog.Snapshot()
		if err != nil {
			return false
		}
		// Patch the snapshot's ConfState to the latest configuration so
		// the recipient knows the current membership immediately.
		snapshot.Metadata.ConfState = &pb.ConfState{Nodes: nodes(r)}
		r.send(
			pb.Message{
				MsgType:  pb.MessageType_MsgSnapshot,
				To:       to,
				Snapshot: &snapshot,
			},
		)
		pr.PendingSnapshot = true
		pr.PendingSnapshotElapsed = 0
		return true
	} else if err != nil {
		return false
	}

	nextIndex := pr.Next
	offset := r.RaftLog.FirstIndex()
	entries := r.RaftLog.entries[nextIndex-offset:]

	ents := make([]*pb.Entry, len(entries))
	for i := range entries {
		ents[i] = &entries[i]
	}
	r.send(pb.Message{
		To:      to,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Commit:  r.RaftLog.committed,
		Entries: ents,
		MsgType: pb.MessageType_MsgAppend,
	})

	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := r.RaftLog.committed
	if pr, ok := r.Prs[to]; ok {
		// A heartbeat is also how the leader tells followers about the latest
		// commit index. Clamp it to the follower's matched index so we never ask
		// it to commit entries it does not have locally yet.
		commit = min(commit, pr.Match)
	}
	r.send(pb.Message{
		To:      to,
		Commit:  commit,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

func (r *Raft) sendRequestVote(to uint64, lastIndex uint64, lastTerm uint64) {
	r.send(pb.Message{
		To:      to,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   lastIndex,
		MsgType: pb.MessageType_MsgRequestVote,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		for id, pr := range r.Prs {
			if id == r.id || !pr.PendingSnapshot {
				continue
			}
			pr.PendingSnapshotElapsed++
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
		if r.leadTransferee != None {
			r.electionElapsed++
			if r.electionElapsed >= r.electionTimeout {
				r.electionElapsed = 0
				r.leadTransferee = None
			}
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed == r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	log.Debugf("[%d] have becomeFollower\n", r.id)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateCandidate
	log.Debugf("[%d] have becomeCandidate, term: %d\n", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	lastIndex := r.RaftLog.LastIndex()

	// 追加 noop entry
	noopEntry := pb.Entry{
		Term:  r.Term,
		Index: lastIndex + 1,
	}

	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)

	// 更新 r.Prs[r.id]
	r.Prs[r.id].Match = lastIndex + 1
	r.Prs[r.id].Next = lastIndex + 2
	log.Debugf("[%d] have becomeLeader\n", r.id)

	if r.isSingleNode() {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// 1. 通用 term 检查
	switch {
	case m.Term == 0:
		// 本地消息（MsgHup / MsgBeat / MsgPropose），不带 term，直接放行
	case m.Term > r.Term:
		r.becomeFollower(m.Term, None)
	case m.Term < r.Term:
		// 来自过期节点的消息，直接忽略
		return nil
	}

	// 2. 分角色处理
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgTransferLeader:
		r.handleLeaderTransfer(m.From)
	case pb.MessageType_MsgBeat:
		r.BcastHeartbeat()
	case pb.MessageType_MsgPropose:
		// 追加日志，广播 MsgAppend
		r.handlePropose(m)
	case pb.MessageType_MsgAppendResponse:
		// 处理 follower 的回复
		r.handleAppendResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// 处理心跳回复
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
		})
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		// 已经投自己了
		r.send(pb.Message{
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
		})
	case pb.MessageType_MsgRequestVoteResponse:
		// 统计选票
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgAppend:
		// 收到 append，说明有合法 leader，退回 follower
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgTimeoutNow:
		if r.promotable() {
			r.becomeCandidate()
			r.campaign()
		}
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.campaign()
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return nil
		}
		m.To = r.Lead
		r.send(m)
	}
	return nil
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.grantVote(m.From, m.Term, m.Index, m.LogTerm) {
		r.Vote = m.From
		r.send(pb.Message{
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  false,
		})
	} else {
		r.send(pb.Message{
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			Reject:  true,
		})
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject // 同意或拒绝都记录

	granted := 0
	rejected := 0
	for _, v := range r.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}

	quorum := len(r.Prs)/2 + 1 // 过半数量
	if granted >= quorum {
		r.becomeLeader()
	} else if rejected >= quorum {
		r.becomeFollower(r.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// 把 []*pb.Entry 转成 []pb.Entry
	ents := make([]pb.Entry, len(m.Entries))
	for i, e := range m.Entries {
		ents[i] = *e
	}

	if lastnewi, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...); ok {
		// 成功
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  false,
			Index:   lastnewi,
		})
	} else {
		// 一致性检查失败，拒绝
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
			Index:   r.RaftLog.LastIndex(),
		})
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	// Any AppendResponse (success or reject) means the snapshot was received.
	r.Prs[m.From].PendingSnapshot = false
	r.Prs[m.From].PendingSnapshotElapsed = 0
	if m.Reject {
		// 回退 nextIndex 重试
		if r.Prs[m.From].Next > 0 {
			r.Prs[m.From].Next = m.Index + 1
		}
		r.sendAppend(m.From)
		return
	}
	// 更新进度
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// 尝试推进 commit
	if r.maybeCommit() {
		r.bcastAppend() // 把新 commitIndex 广播给所有 follower
	}
	if m.From == r.leadTransferee && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgTimeoutNow})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.commitTo(m.Commit)
	r.msgs = append(r.msgs, (pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}))
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// A heartbeat response proves the peer is alive and reachable.
	// Clear any stale PendingSnapshot so that sendAppend can retry delivering
	// a snapshot that was lost (e.g. sent before the peer's store was ready).
	r.Prs[m.From].PendingSnapshot = false
	r.Prs[m.From].PendingSnapshotElapsed = 0
	// 如果 follower 的 Match 落后，说明有日志需要补发
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.leadTransferee != None {
		return // drop proposal while transferring
	}
	// 1. 把 entries 追加到本地 RaftLog
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		// Track the index of any pending conf-change so the upper layer can
		// enforce the "at most one conf change in flight" invariant.
		if entry.EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entry.Index
		}
	}
	// 2. 更新自己的 Prs
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	// 3. 广播给所有 Follower
	r.bcastAppend()
	// 4. 单节点直接 commit
	if r.isSingleNode() {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	meta := m.Snapshot.Metadata
	if m.Term < r.Term {
		return
	}
	if meta.Index <= r.RaftLog.committed {
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   r.RaftLog.LastIndex(),
		})
		return
	}

	r.becomeFollower(m.Term, m.From)

	r.RaftLog.committed = meta.Index
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.applied = meta.Index
	r.RaftLog.stabled = meta.Index
	r.RaftLog.entries = []pb.Entry{{Index: meta.Index, Term: meta.Term}}

	r.Prs = make(map[uint64]*Progress)
	for _, id := range meta.ConfState.Nodes {
		r.Prs[id] = &Progress{}
	}

	r.send(pb.Message{
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   meta.Index,
	})
}

// handle leader transfer
func (r *Raft) handleLeaderTransfer(leaderTransferee uint64) {
	// 判断 leadTransferee在不在 peer 里
	if _, exists := r.Prs[leaderTransferee]; !exists {
		// peer not found!
		return
	}
	// 判断自己的 leadTransferee 是否为空，如果不为空，则说明已经有 Leader Transfer 在执行，忽略本次
	if r.leadTransferee != None {
		if r.leadTransferee == leaderTransferee {
			return
		}
		lastLeadTransferee := r.leadTransferee
		r.leadTransferee = None
		log.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
	}
	if leaderTransferee == r.id {
		log.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
		return
	}

	r.electionElapsed = 0
	r.leadTransferee = leaderTransferee
	if r.Prs[leaderTransferee].Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{To: leaderTransferee, MsgType: pb.MessageType_MsgTimeoutNow})
	} else {
		r.sendAppend(leaderTransferee)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	// Next=1 means prevLogIndex=0, which is before the log start.
	// Term(0) returns ErrCompacted, causing sendAppend to send a snapshot
	// so the new peer can catch up from scratch.
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	if r.id == r.Lead {
		r.maybeCommit()
	}
}

func (r *Raft) isSingleNode() bool {
	return len(r.Prs) == 1
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = resetRandomTimeout(r.electionTimeout)
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	lastIndex := r.RaftLog.LastIndex()
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Next:  lastIndex + 1,
			Match: 0,
		}
		if id == r.id {
			r.Prs[id].Match = lastIndex // 自己的 Match 初始化为 lastIndex
		}
	}

	r.votes = make(map[uint64]bool)
	r.leadTransferee = None
}

func (r *Raft) BcastHeartbeat() {
	for p, pr := range r.Prs {
		if p == r.id {
			continue
		}
		if pr.PendingSnapshot && pr.PendingSnapshotElapsed >= r.electionTimeout {
			pr.PendingSnapshot = false
			pr.PendingSnapshotElapsed = 0
			r.sendAppend(p)
			continue
		}
		r.sendHeartbeat(p)
	}
}

func (r *Raft) bcastAppend() {
	for p := range r.Prs {
		if p == r.id {
			continue
		}
		r.sendAppend(p)
	}
}

func (r *Raft) campaign() {
	if r.isSingleNode() {
		r.becomeLeader()
	}

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	log.Debugf("[%d] start to campaign with last log index: %d, last log term %d\n", r.id, lastIndex, lastTerm)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendRequestVote(peer, lastIndex, lastTerm)
	}

}

func (r *Raft) grantVote(cid, term, lastLogIndex, lastLogTerm uint64) bool {
	if r.Vote != None && r.Vote != cid {
		return false
	}
	return r.isUpToDate(lastLogIndex, lastLogTerm)
}

func (r *Raft) isUpToDate(candidateLastIndex, candidateLastTerm uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	if candidateLastTerm != lastTerm {
		return candidateLastTerm > lastTerm
	}
	return candidateLastIndex >= lastIndex
}

// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MessageType_MsgRequestVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MessageType_MsgRequestVoteResp: m.Term is the new r.Term if the MessageType_MsgRequestVote was
			//   granted, non-zero for the same reason MessageType_MsgRequestVote is
			// - MsgPreVote: m.Term is the term the node will campaign
			log.Fatalf("term should be set when sending %s", m.MsgType)
		}
	} else {
		if m.Term != 0 {
			log.Fatalf("term should not be set when sending %s (was %d)", m.MsgType, m.Term)
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}

	// 添加信息到队列中，这个添加的信息，之后会作为 meesage 输出到 Ready 结构中；
	// 之后会发送过网络，m 是本地已经持久化了的消息
	// 经过 raft State Machine 处理过，确认需要发送网络；
	r.msgs = append(r.msgs, m)
}

func (r *Raft) maybeCommit() bool {
	// 收集所有 Match 值排序，找到过半的那个
	matches := make([]uint64, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matches = append(matches, pr.Match)
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j] // 降序
	})
	// 过半数量的节点都至少复制到 matches[quorum-1]
	quorum := len(r.Prs)/2 + 1
	newCommit := matches[quorum-1]

	// 只能 commit 当前 term 的日志（论文 §5.4.2）
	if newCommit > r.RaftLog.committed {
		logTerm, _ := r.RaftLog.Term(newCommit)
		if logTerm == r.Term {
			r.RaftLog.committed = newCommit
			// Once a conf change is committed it is no longer "in flight";
			// allow the next conf change to be proposed immediately.
			if r.PendingConfIndex > 0 && newCommit >= r.PendingConfIndex {
				r.PendingConfIndex = 0
			}
			return true
		}
	}
	return false
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// 是否可以升级为 leader ？需要满足两个条件：
// 1. 自己在 progress 列表中
// 2. 不是 learner 角色
// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *Raft) promotable() bool {
	pr := r.Prs[r.id]
	return pr != nil
}
