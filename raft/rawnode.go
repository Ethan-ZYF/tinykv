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

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	prevSoftState *SoftState
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	rn := &RawNode{
		Raft: raft,
		prevSoftState: &SoftState{
			Lead:      raft.Lead,
			RaftState: raft.State,
		},
		prevHardState: raft.hardState(),
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		log.Debugf("[RawNode %d] Step %s from %d (term=%d, index=%d, commit=%d)",
			rn.Raft.id, m.MsgType, m.From, m.Term, m.Index, m.Commit)
		return rn.Raft.Step(m)
	}
	log.Debugf("[RawNode %d] Step drop %s from %d: peer not found", rn.Raft.id, m.MsgType, m.From)
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	r := rn.Raft
	rd := Ready{}

	// 1. SoftState：Lead 或 State 变化了才填
	ss := r.softState()
	if ss.Lead != rn.prevSoftState.Lead || ss.RaftState != rn.prevSoftState.RaftState {
		rd.SoftState = ss
		log.Debugf("[RawNode %d] SoftState change: state=%s lead=%d", r.id, ss.RaftState, ss.Lead)
	}

	// 2. HardState：Term/Vote/Commit 变化了才填
	hs := r.hardState()
	if !isHardStateEqual(hs, rn.prevHardState) {
		rd.HardState = hs
		log.Debugf("[RawNode %d] HardState change: term=%d vote=%d commit=%d", r.id, hs.Term, hs.Vote, hs.Commit)
	}

	// 3. Entries：unstable entries，需要持久化
	rd.Entries = r.RaftLog.unstableEntries()

	// 4. Snapshot：待应用的快照（2C 再处理）
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		rd.Snapshot = *r.RaftLog.pendingSnapshot
		log.Debugf("[RawNode %d] Ready snapshot index=%d term=%d",
			r.id, rd.Snapshot.Metadata.Index, rd.Snapshot.Metadata.Term)
	}

	// 5. CommittedEntries：已提交但未 apply 的日志
	rd.CommittedEntries = r.RaftLog.nextEnts()

	// 6. Messages：待发送的网络消息
	rd.Messages = r.msgs

	if len(rd.Entries) > 0 || len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 {
		log.Debugf("[RawNode %d] Ready: unstableEntries=%d committedEntries=%d messages=%d",
			r.id, len(rd.Entries), len(rd.CommittedEntries), len(rd.Messages))
	}

	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	r := rn.Raft
	// SoftState 变化
	if rn.prevSoftState.Lead != r.Lead || rn.prevSoftState.RaftState != r.State {
		return true
	}
	// HardState 变化
	hs := r.hardState()
	if !isEmptyHardState(hs) && !isHardStateEqual(hs, rn.prevHardState) {
		return true
	}
	// unstable entries
	if len(r.RaftLog.unstableEntries()) > 0 {
		return true
	}
	// committed but not applied
	if len(r.RaftLog.nextEnts()) > 0 {
		return true
	}
	// pending messages
	if len(r.msgs) > 0 {
		return true
	}
	// pending snapshot to handle
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		return true
	}
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// 1. 更新 prevSoftState / prevHardState
	if rd.SoftState != nil {
		rn.prevSoftState = rd.SoftState
	}
	if !isEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}
	// 2. 推进 stabled（entries 已持久化）
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		log.Debugf("[RawNode %d] Advance stabled: %d -> %d", rn.Raft.id, rn.Raft.RaftLog.stabled, e.Index)
		rn.Raft.RaftLog.stabled = e.Index
	}
	// 3. 推进 applied（committed entries 已应用）
	if len(rd.CommittedEntries) > 0 {
		e := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		log.Debugf("[RawNode %d] Advance applied: %d -> %d", rn.Raft.id, rn.Raft.RaftLog.applied, e.Index)
		rn.Raft.RaftLog.applied = e.Index
	}

	// 4. 清空 snapshot
	rn.Raft.RaftLog.pendingSnapshot = nil
	rn.Raft.RaftLog.maybeCompact()

	// 5. 清空 msgs
	rn.Raft.msgs = nil
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

func isEmptyHardState(st pb.HardState) bool {
	return st.Term == 0 && st.Vote == 0 && st.Commit == 0
}
