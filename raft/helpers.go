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

// ====================================================================================
// 消息构造辅助函数
// ====================================================================================

// newMessage 创建一个基础消息，包含通用字段
func (r *Raft) newMessage(to uint64, msgType pb.MessageType) pb.Message {
	return pb.Message{
		MsgType: msgType,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
}

// newAppendResponse 创建 AppendEntries 响应消息
func (r *Raft) newAppendResponse(to uint64, reject bool, index uint64) pb.Message {
	msg := r.newMessage(to, pb.MessageType_MsgAppendResponse)
	msg.Reject = reject
	msg.Index = index
	return msg
}

// newVoteResponse 创建投票响应消息
func (r *Raft) newVoteResponse(to uint64, reject bool) pb.Message {
	msg := r.newMessage(to, pb.MessageType_MsgRequestVoteResponse)
	msg.Reject = reject
	return msg
}

// newHeartbeatResponse 创建心跳响应消息
func (r *Raft) newHeartbeatResponse(to uint64) pb.Message {
	return r.newMessage(to, pb.MessageType_MsgHeartbeatResponse)
}

// newVoteRequest 创建投票请求消息
func (r *Raft) newVoteRequest(to uint64, lastLogIndex, lastLogTerm uint64) pb.Message {
	msg := r.newMessage(to, pb.MessageType_MsgRequestVote)
	msg.Index = lastLogIndex
	msg.LogTerm = lastLogTerm
	return msg
}

// newHeartbeatMessage 创建心跳消息
func (r *Raft) newHeartbeatMessage(to uint64, commit uint64) pb.Message {
	msg := r.newMessage(to, pb.MessageType_MsgHeartbeat)
	msg.Commit = commit
	return msg
}

// newAppendMessage 创建 AppendEntries 消息
func (r *Raft) newAppendMessage(to uint64, prevLogIndex, prevLogTerm uint64, entries []*pb.Entry, commit uint64) pb.Message {
	msg := r.newMessage(to, pb.MessageType_MsgAppend)
	msg.Index = prevLogIndex
	msg.LogTerm = prevLogTerm
	msg.Entries = entries
	msg.Commit = commit
	return msg
}

// ====================================================================================
// 日志检查辅助函数
// ====================================================================================

// isLogUpToDate 检查候选者的日志是否至少和自己一样新
// Raft 论文 5.4.1: 投票限制
// 如果候选者的日志至少和自己一样新，才能投票给它
func (r *Raft) isLogUpToDate(candidateLastTerm, candidateLastIndex uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)

	// 比较最后日志的任期
	if candidateLastTerm != lastLogTerm {
		return candidateLastTerm > lastLogTerm
	}

	// 任期相同，比较索引
	return candidateLastIndex >= lastIndex
}

// checkLogConsistency 检查日志一致性（AppendEntries RPC 的一致性检查）
// 返回: (是否一致, 如果不一致则返回建议的拒绝索引)
func (r *Raft) checkLogConsistency(prevLogIndex, prevLogTerm uint64) (bool, uint64) {
	lastIndex := r.RaftLog.LastIndex()

	// 检查1: prevLogIndex 是否超出我们的日志范围
	if prevLogIndex > lastIndex {
		return false, lastIndex
	}

	// 检查2: prevLogIndex 位置的任期是否匹配
	// prevLogIndex = 0 表示没有前置日志，总是匹配
	if prevLogIndex > 0 {
		term, err := r.RaftLog.Term(prevLogIndex)
		if err != nil || term != prevLogTerm {
			return false, prevLogIndex
		}
	}

	return true, 0
}

// getLastNewEntryIndex 获取消息中最后一个新条目的索引
// 如果有新条目，返回最后一个条目的索引
// 如果没有新条目（心跳或空 AppendEntries），返回 prevLogIndex
func (r *Raft) getLastNewEntryIndex(m pb.Message) uint64 {
	if len(m.Entries) > 0 {
		return m.Entries[len(m.Entries)-1].Index
	}
	return m.Index
}

// mustGetLastLogTerm 获取最后日志的任期，出错则 panic
// 用于不应该出错的场景（如 LastIndex() 一定存在）
func (r *Raft) mustGetLastLogTerm() uint64 {
	lastIndex := r.RaftLog.LastIndex()
	if lastIndex == 0 {
		return 0
	}
	term, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic("failed to get last log term: " + err.Error())
	}
	return term
}

// ====================================================================================
// Progress 管理辅助函数
// ====================================================================================

// initProgressAsLeader 初始化 Progress（成为 Leader 后调用）
// Leader 需要知道每个 follower 的日志复制进度
func (r *Raft) initProgressAsLeader() {
	lastIndex := r.RaftLog.LastIndex()

	for id := range r.Prs {
		// 乐观假设所有 follower 都有所有日志（Next = lastIndex + 1）
		r.Prs[id].Next = lastIndex + 1

		if id == r.id {
			// Leader 知道自己的 Match
			r.Prs[id].Match = lastIndex
		} else {
			// Follower 的 Match 从 0 开始（通过 AppendEntries 响应更新）
			r.Prs[id].Match = 0
		}
	}
}

// updateProgress 更新指定 peer 的进度
func (r *Raft) updateProgress(peer, matchIndex uint64) {
	pr := r.Prs[peer]
	if matchIndex > pr.Match {
		pr.Match = matchIndex
		pr.Next = pr.Match + 1
	}
}

// decrementProgress 递减指定 peer 的 Next 索引（日志冲突时使用）
func (r *Raft) decrementProgress(peer uint64) {
	pr := r.Prs[peer]
	if pr.Next > 1 {
		pr.Next--
	}
}

// ====================================================================================
// 状态检查辅助函数
// ====================================================================================

// canVoteFor 检查是否可以投票给指定的候选者
// 每个任期只能投一票，遵循先来先得原则
func (r *Raft) canVoteFor(candidate uint64) bool {
	return r.Vote == None || r.Vote == candidate
}

// hasQuorum 检查是否达到多数（quorum）
func (r *Raft) hasQuorum(count int) bool {
	return count >= len(r.Prs)/2+1
}

// quorumSize 返回 quorum 大小（多数节点数）
func (r *Raft) quorumSize() int {
	return len(r.Prs)/2 + 1
}

// isLeader 检查当前节点是否是 Leader
func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

// isCandidate 检查当前节点是否是 Candidate
func (r *Raft) isCandidate() bool {
	return r.State == StateCandidate
}

// isFollower 检查当前节点是否是 Follower
func (r *Raft) isFollower() bool {
	return r.State == StateFollower
}

// isSingleNode 检查是否是单节点集群
func (r *Raft) isSingleNode() bool {
	return len(r.Prs) == 1
}

// ====================================================================================
// 投票统计辅助函数
// ====================================================================================

// countVotes 统计投票结果
// 返回: (赞成票数, 反对票数)
func (r *Raft) countVotes() (granted, rejected int) {
	for _, vote := range r.votes {
		if vote {
			granted++
		} else {
			rejected++
		}
	}
	return granted, rejected
}

// hasWonElection 检查是否赢得选举（获得多数赞成票）
func (r *Raft) hasWonElection() bool {
	granted, _ := r.countVotes()
	return r.hasQuorum(granted)
}

// hasLostElection 检查是否失去选举（获得多数反对票）
func (r *Raft) hasLostElection() bool {
	_, rejected := r.countVotes()
	return r.hasQuorum(rejected)
}

// ====================================================================================
// 条目转换辅助函数
// ====================================================================================

// entriesToSlice 将消息中的 entries 指针数组转换为值数组
func entriesToSlice(entries []*pb.Entry) []pb.Entry {
	if len(entries) == 0 {
		return nil
	}
	result := make([]pb.Entry, len(entries))
	for i, e := range entries {
		result[i] = *e
	}
	return result
}

// sliceToEntries 将值数组转换为指针数组（用于构造消息）
func sliceToEntries(entries []pb.Entry) []*pb.Entry {
	if len(entries) == 0 {
		return nil
	}
	result := make([]*pb.Entry, len(entries))
	for i := range entries {
		result[i] = &entries[i]
	}
	return result
}

// ====================================================================================
// 日志索引计算辅助函数
// ====================================================================================

// calculateCommitIndex 计算应该提交到哪个索引
// 给定一组 Match 索引，返回多数节点已复制的最高索引
func calculateCommitIndex(matches []uint64) uint64 {
	if len(matches) == 0 {
		return 0
	}

	// 降序排序
	sorted := make([]uint64, len(matches))
	copy(sorted, matches)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] < sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	// 返回中位数（多数节点的最小值）
	return sorted[len(sorted)/2]
}
