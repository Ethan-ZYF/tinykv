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
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ====================================================================================
// 常量和类型定义
// ====================================================================================

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
// 节点在集群中的角色
type StateType uint64

const (
	StateFollower  StateType = iota // 跟随者：被动接收leader的消息
	StateCandidate                  // 候选人：正在竞选leader
	StateLeader                     // 领导者：负责处理客户端请求和日志复制
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

// ====================================================================================
// Config 和 Progress 结构体
// ====================================================================================

// Config contains the parameters to start a raft.
// Raft配置参数
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

// validate 检查配置是否合法
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

// Progress represents a follower's progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// Progress 表示leader视角下follower的进度
type Progress struct {
	Match uint64 // 已知已复制到该节点的最高日志索引
	Next  uint64 // 下一个要发送给该节点的日志索引
}

// ====================================================================================
// Raft 核心结构体
// ====================================================================================

type Raft struct {
	id uint64 // 节点ID

	Term uint64 // 当前任期
	Vote uint64 // 当前任期投票给了谁

	// the log
	RaftLog *RaftLog // 日志管理

	// log replication progress of each peers
	Prs map[uint64]*Progress // 所有节点的日志复制进度（leader使用）

	// this peer's role
	State StateType // 当前角色（Follower/Candidate/Leader）

	// votes records
	votes map[uint64]bool // 投票记录（candidate使用）

	// msgs need to send
	msgs []pb.Message // 待发送的消息队列

	// the leader id
	Lead uint64 // 当前leader的ID

	// heartbeat interval, should send
	heartbeatTimeout int // 心跳超时（tick数）
	// baseline of election interval
	electionTimeout int // 选举超时（tick数，每次选举会随机化）
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int // 距离上次心跳的tick数
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int // 距离上次选举或收到leader消息的tick数

	// Base of election timeout randomized each election
	electionTimeoutBase int // 选举超时基准值（用于随机化）

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

// ====================================================================================
// 初始化函数
// ====================================================================================

// newRaft return a raft peer with the given config
// 创建一个新的Raft节点
func newRaft(c *Config) *Raft {
	log.Debug(fmt.Sprintf("[INIT] newRaft called with id %d, peers %v", c.ID, c.peers))
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	r := &Raft{
		id:                  c.ID,
		Term:                0,    // 初始任期为0
		Vote:                None, // 还未投票
		RaftLog:             newLog(c.Storage),
		Prs:                 make(map[uint64]*Progress),
		State:               StateFollower, // 所有节点启动时都是follower
		votes:               make(map[uint64]bool),
		msgs:                nil, // 初始化为 nil 而不是空切片
		Lead:                None, // 还不知道谁是leader
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		electionTimeoutBase: c.ElectionTick,
		heartbeatElapsed:    0,
		electionElapsed:     0,
	}

	// 从storage恢复持久化状态（Term、Vote、Commit）
	hardState, _, _ := c.Storage.InitialState()
	if hardState.Term != 0 {
		r.Term = hardState.Term
	}
	if hardState.Vote != 0 {
		r.Vote = hardState.Vote
	}
	// 恢复 committed 索引（重启场景很重要）
	r.RaftLog.committed = hardState.Commit

	// 恢复 applied 索引（如果配置中指定了）
	if c.Applied != 0 {
		r.RaftLog.applied = c.Applied
	}

	// 为所有peer（包括自己）初始化Progress
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{}
	}

	// 随机化选举超时，防止所有节点同时发起选举
	r.resetElectionTimeout()

	return r
}

// ====================================================================================
// 时钟驱动
// ====================================================================================

// tick advances the internal logical clock by a single tick.
// tick 推进逻辑时钟，由上层应用定期调用
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		// Follower和Candidate需要检查选举超时
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0
			// 触发选举：发送MsgHup消息给自己
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		// Leader需要定期发送心跳
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// 触发心跳：发送MsgBeat消息给自己
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// ====================================================================================
// 消息处理入口
// ====================================================================================

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// Step 是消息处理的入口函数，所有消息都通过这里分发
func (r *Raft) Step(m pb.Message) error {
	log.Debug(fmt.Sprintf("[%s] %d received %s from %d", r.State.String(), r.id, m.MsgType.String(), m.From))
	// 处理任期更新（本地消息除外）
	// 如果收到的消息任期更高，说明自己的任期过时了，需要转为follower
	if m.Term > r.Term && !IsLocalMsg(m.MsgType) {
		lead := m.From
		// 如果是RequestVote消息，此时还不知道谁是leader
		if m.MsgType == pb.MessageType_MsgRequestVote {
			lead = None
		}
		r.becomeFollower(m.Term, lead)
	}

	// 根据当前角色分发消息到对应的处理函数
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

// stepFollower 处理Follower状态下收到的消息
func (r *Raft) stepFollower(m pb.Message) {
	log.Debug(fmt.Sprintf("%d handle %s from %d in Follower state", r.id, m.MsgType.String(), m.From))
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 选举超时，发起选举
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		// 收到投票请求
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		// 收到leader的心跳
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		// 收到leader的日志复制请求
		r.handleAppendEntries(m)
	}
}

// stepCandidate 处理Candidate状态下收到的消息
func (r *Raft) stepCandidate(m pb.Message) {
	log.Debug(fmt.Sprintf("%d handle %s from %d in Candidate state", r.id, m.MsgType.String(), m.From))
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 选举超时，重新发起选举
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		// 收到其他节点的投票请求
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// 收到投票响应
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		// 收到心跳，说明已经有leader了
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		// 收到AppendEntries，说明已经有leader了
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	}
}

// stepLeader 处理Leader状态下收到的消息
func (r *Raft) stepLeader(m pb.Message) {
	log.Debug(fmt.Sprintf("%d handle %s from %d in Leader state", r.id, m.MsgType.String(), m.From))
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		// 心跳超时，广播心跳
		r.bcastHeartbeat()
	case pb.MessageType_MsgRequestVote:
		// 收到投票请求（可能是任期更高的candidate）
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeatResponse:
		// 收到心跳响应，检查是否需要发送日志
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgPropose:
		// 收到客户端的提议
		r.handleMsgPropose(m)
	case pb.MessageType_MsgAppendResponse:
		// 收到日志复制响应
		r.handleAppendResponse(m)
	}
}

// ====================================================================================
// 状态转换函数
// ====================================================================================

// becomeFollower transform this peer's state to Follower
// 转换为Follower状态
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	log.Debug(fmt.Sprintf("%d became Follower at term %d", r.id, term))
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None // 新任期还未投票
	r.electionElapsed = 0
	r.resetElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
// 转换为Candidate状态
func (r *Raft) becomeCandidate() {
	log.Debug(fmt.Sprintf("%d became Candidate at term %d", r.id, r.Term+1))
	r.State = StateCandidate
	r.Term++              // 增加任期
	r.Vote = r.id         // 投票给自己
	r.Lead = None         // 选举期间没有leader
	r.electionElapsed = 0 // 重置选举计时器
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // 记录自己的投票
	r.resetElectionTimeout()
}

// becomeLeader transform this peer's state to leader
// 转换为Leader状态
func (r *Raft) becomeLeader() {
	log.Debug(fmt.Sprintf("%d became Leader at term %d", r.id, r.Term))
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	// 使用辅助函数初始化 Progress
	r.initProgressAsLeader()

	// Leader必须在当选后立即追加一个no-op entry
	// 这样可以提交之前任期的日志（Raft论文5.4.2节）
	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: nil}},
	})
}

// ====================================================================================
// 选举相关函数
// ====================================================================================

// campaign 发起选举
func (r *Raft) campaign() {
	log.Debug(fmt.Sprintf("[%s] %d campaigns at term %d", r.State.String(), r.id, r.Term+1))
	r.becomeCandidate()

	// 使用辅助函数检查单节点集群
	if r.isSingleNode() {
		r.becomeLeader()
		return
	}

	// 获取日志信息（使用辅助函数）
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm := r.mustGetLastLogTerm()

	// 向所有其他节点发送投票请求
	for peer := range r.Prs {
		if peer == r.id {
			continue // 跳过自己
		}
		// 使用辅助函数构造消息
		r.send(r.newVoteRequest(peer, lastIndex, lastLogTerm))
	}
}

// handleMsgPropose 处理客户端的提议
func (r *Raft) handleMsgPropose(m pb.Message) {
	// Your Code Here (2AB).
	// 收到客户端的提议（2AB阶段实现）
	if len(m.Entries) == 0 {
		return
	}

	// 1. Append to leader's log first
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Index = lastIndex + uint64(i) + 1
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}

	// 2. Update leader's own Progress
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// 3. Single-node cluster commits immediately
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}

	// 4. Now broadcast to followers
	r.bcastAppend()
}

// handleRequestVote 处理投票请求
func (r *Raft) handleRequestVote(m pb.Message) {
	log.Debug(fmt.Sprintf("[%s] %d handle RequestVote from %d at term %d",
		r.State.String(), r.id, m.From, m.Term))

	// 检查投票条件：每个任期只能投一票 + 候选者日志至少和自己一样新
	canVote := r.canVoteFor(m.From)
	logUpToDate := r.isLogUpToDate(m.LogTerm, m.Index)
	grant := canVote && logUpToDate

	if grant {
		r.Vote = m.From
		r.electionElapsed = 0 // 投票后重置选举超时
	}

	// 发送投票响应
	r.send(r.newVoteResponse(m.From, !grant))
}

// handleRequestVoteResponse 处理投票响应
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	log.Debug(fmt.Sprintf("[%s] %d handle RequestVoteResponse from %d at term %d",
		r.State.String(), r.id, m.From, m.Term))

	// 记录投票结果
	r.votes[m.From] = !m.Reject

	// 使用辅助函数检查选举结果
	if r.hasWonElection() {
		r.becomeLeader()
		return
	}

	if r.hasLostElection() {
		r.becomeFollower(r.Term, None)
	}
	// 如果既没赢也没输，继续等待其他节点的投票
}

// ====================================================================================
// 心跳相关函数
// ====================================================================================

// bcastHeartbeat 向所有peer广播心跳
func (r *Raft) bcastHeartbeat() {
	log.Debug(fmt.Sprintf("[%s] %d broadcast Heartbeat at term %d", r.State.String(), r.id, r.Term))
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// 向指定peer发送心跳消息
func (r *Raft) sendHeartbeat(to uint64) {
	log.Debug(fmt.Sprintf("[%s] %d send Heartbeat to %d at term %d", r.State.String(), r.id, to, r.Term))
	// 使用辅助函数构造心跳消息
	r.send(r.newHeartbeatMessage(to, r.RaftLog.committed))
}

// handleHeartbeat handle Heartbeat RPC request
// 处理心跳消息
func (r *Raft) handleHeartbeat(m pb.Message) {
	log.Debug(fmt.Sprintf("[%s] %d handle Heartbeat from %d at term %d", r.State.String(), r.id, m.From, m.Term))
	r.electionElapsed = 0 // 重置选举计时器，防止发起选举
	r.Lead = m.From       // 记录leader

	// 不在心跳中更新committed - 只有AppendEntries验证日志后才更新
	// 这确保我们不会提交未经验证的旧日志条目

	// 使用辅助函数构造心跳响应
	r.send(r.newHeartbeatResponse(m.From))
}

// ====================================================================================
// 日志复制相关函数（2AB实现）
// ====================================================================================

// bcastAppend 向所有peer广播日志复制请求
func (r *Raft) bcastAppend() {
	log.Debug(fmt.Sprintf("[%s] %d broadcast AppendEntries at term %d", r.State.String(), r.id, r.Term))
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// 向指定peer发送AppendEntries消息
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2AB).
	log.Debug(fmt.Sprintf("[%s] %d send AppendEntries to %d at term %d", r.State.String(), r.id, to, r.Term))
	pr := r.Prs[to]
	prevLogIndex := pr.Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		log.Error(fmt.Sprintf("Error getting term for index %d: %v", prevLogIndex, err))
		return false
	}

	// 获取要发送的日志条目
	entries := make([]*pb.Entry, 0)
	if len(r.RaftLog.entries) > 0 {
		lastIndex := r.RaftLog.LastIndex()
		if pr.Next <= lastIndex {
			firstIndex := r.RaftLog.entries[0].Index
			entriesSlice := r.RaftLog.entries[pr.Next-firstIndex:]
			for i := range entriesSlice {
				entries = append(entries, &entriesSlice[i])
			}
		}
	}

	// 发送AppendEntries消息
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed, // 告诉follower当前的commit位置
	})
	return true
}

// handleAppendEntries handle AppendEntries RPC request
// 处理日志复制请求
func (r *Raft) handleAppendEntries(m pb.Message) {
	log.Debug(fmt.Sprintf("[%s] %d handle AppendEntries from %d at term %d", r.State.String(), r.id, m.From, m.Term))
	r.electionElapsed = 0 // 重置选举计时器
	r.Lead = m.From       // 记录leader

	// 使用辅助函数检查日志一致性
	if ok, rejectIndex := r.checkLogConsistency(m.Index, m.LogTerm); !ok {
		r.send(r.newAppendResponse(m.From, true, rejectIndex))
		return
	}

	// 追加日志（使用辅助函数转换）
	if len(m.Entries) > 0 {
		entries := entriesToSlice(m.Entries)
		r.RaftLog.appendEntries(entries)
	}

	// 更新 committed（使用辅助函数）
	if m.Commit > r.RaftLog.committed {
		lastNewEntry := r.getLastNewEntryIndex(m)
		r.RaftLog.committed = min(m.Commit, lastNewEntry)
	}

	// 发送成功响应
	r.send(r.newAppendResponse(m.From, false, r.RaftLog.LastIndex()))
}

// handleHeartbeatResponse handle Heartbeat response
// 处理心跳响应
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	pr := r.Prs[m.From]

	// 如果follower落后了（Match < LastIndex），发送AppendEntries来追赶
	if pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleAppendResponse handle AppendEntries response
// 处理日志复制响应
func (r *Raft) handleAppendResponse(m pb.Message) {
	log.Debug(fmt.Sprintf("[%s] %d handle AppendResponse from %d at term %d", r.State.String(), r.id, m.From, m.Term))

	pr := r.Prs[m.From]

	if m.Reject {
		// 日志不匹配，回退 Next
		pr.Next = max(1, pr.Next-1)
		// 重新发送
		r.sendAppend(m.From)
		return
	}

	// 成功复制，更新 Progress
	if m.Index > pr.Match {
		pr.Match = m.Index
		pr.Next = pr.Match + 1

		// 尝试提交日志
		r.maybeCommit()
	}
}

// maybeCommit attempts to commit logs based on majority replication
// 尝试提交已复制到多数节点的日志
func (r *Raft) maybeCommit() {
	// 收集所有节点的 Match 索引
	matches := make([]uint64, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matches = append(matches, pr.Match)
	}

	// 按降序排序
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] > matches[j]
	})

	// 找到中位数（多数派复制的最小值）
	n := matches[len(matches)/2]

	// 只能提交当前任期的日志（Raft 论文图8的安全性要求）
	if n > r.RaftLog.committed {
		logTerm, _ := r.RaftLog.Term(n)
		if logTerm == r.Term {
			oldCommitted := r.RaftLog.committed
			log.Debug(fmt.Sprintf("[%s] %d commits log up to index %d", r.State.String(), r.id, n))
			r.RaftLog.committed = n
			// 广播更新的commit索引给followers
			// 只有当commit确实增加时才广播，避免不必要的消息
			if r.RaftLog.committed > oldCommitted {
				r.bcastAppend()
			}
		}
	}
}

// ====================================================================================
// 辅助函数
// ====================================================================================

// send 将消息加入发送队列
func (r *Raft) send(m pb.Message) {
	log.Debug(fmt.Sprintf("[%s] %d send %s to %d", r.State.String(), r.id, m.MsgType.String(), m.To))
	r.msgs = append(r.msgs, m)
}

// resetElectionTimeout 重置选举超时为随机值
// 随机化是为了避免多个节点同时超时发起选举导致split vote
func (r *Raft) resetElectionTimeout() {
	log.Debug(fmt.Sprintf("[%s] %d reset election timeout", r.State.String(), r.id))
	// 选举超时在 [electionTimeoutBase, 2*electionTimeoutBase) 之间随机
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase)
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// ====================================================================================
// 快照和配置变更相关（后续项目实现）
// ====================================================================================

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
