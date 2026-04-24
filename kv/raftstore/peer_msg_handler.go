package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	log.Debugf("%s HandleRaftReady: unstableEntries=%d committedEntries=%d messages=%d hasSnap=%v",
		d.Tag, len(rd.Entries), len(rd.CommittedEntries), len(rd.Messages), rd.Snapshot.GetMetadata() != nil && rd.Snapshot.GetMetadata().GetIndex() != 0)
	applyResult, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		log.Fatal(err)
	}

	// Update storeMeta and peer's own region if snapshot was applied
	if applyResult != nil {
		log.Debugf("%s applied snapshot, new region: %v", d.Tag, applyResult.Region)
		d.peerStorage.SetRegion(applyResult.Region)
		d.ctx.storeMeta.Lock()
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applyResult.Region})
		d.ctx.storeMeta.regions[applyResult.Region.Id] = applyResult.Region
		d.ctx.storeMeta.Unlock()
	}

	if len(rd.Messages) != 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}

	for _, entry := range rd.CommittedEntries {
		d.applyEntry(entry)
		if d.stopped {
			return
		}
	}
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) applyEntry(entry eraftpb.Entry) {
	log.Debugf("%s applyEntry index=%d term=%d type=%s", d.Tag, entry.Index, entry.Term, entry.EntryType)
	if entry.Data == nil {
		// noop entry from leader election
		d.peerStorage.applyState.AppliedIndex = entry.Index
		kvWB := new(engine_util.WriteBatch)
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		kvWB.WriteToDB(d.ctx.engine.Kv)
		d.handleProposals(entry, nil)
		return
	}

	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		d.applyConfChange(entry)
		return
	}

	var msg raft_cmdpb.RaftCmdRequest
	msg.Unmarshal(entry.Data)

	if msg.AdminRequest != nil {
		d.applyAdminRequest(entry, &msg)
		return
	}

	kvWB := new(engine_util.WriteBatch)
	resp := newCmdResp()
	for _, request := range msg.Requests {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Invalid:
		case raft_cmdpb.CmdType_Get:
			val, err := engine_util.GetCF(d.ctx.engine.Kv, request.Get.GetCf(), request.Get.GetKey())
			if err != nil {
				val = nil
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: val},
			})
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(request.Put.GetCf(), request.Put.GetKey(), request.Put.GetValue())
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(request.Delete.GetCf(), request.Delete.GetKey())
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		case raft_cmdpb.CmdType_Snap:
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
			})
		}
	}
	d.peerStorage.applyState.AppliedIndex = entry.Index
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	kvWB.WriteToDB(d.ctx.engine.Kv)

	d.handleProposals(entry, resp)
}

func (d *peerMsgHandler) handleProposals(entry eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index < entry.Index {
			// Stale proposal, notify and discard
			log.Debugf("%s stale proposal index=%d (entry index=%d)", d.Tag, p.index, entry.Index)
			if p.cb != nil {
				NotifyStaleReq(entry.Term, p.cb)
			}
			d.proposals = d.proposals[1:]
			continue
		}

		if p.index > entry.Index {
			break
		}

		// p.index == entry.Index
		if p.term == entry.Term && p.cb != nil && resp != nil {
			for _, r := range resp.Responses {
				if r.CmdType == raft_cmdpb.CmdType_Snap {
					p.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
				}
			}
			p.cb.Done(resp)
		} else if p.cb != nil {
			NotifyStaleReq(entry.Term, p.cb)
		}

		d.proposals = d.proposals[1:]
		break
	}
}

// applyConfChange applies a committed EntryConfChange entry.
// It updates raft's internal peer set, the region metadata, and persists
// the new state to disk. If the removed peer is self, destroyPeer is called.
func (d *peerMsgHandler) applyConfChange(entry eraftpb.Entry) {
	// Decode the ConfChange and the original RaftCmdRequest embedded in cc.Context.
	var cc eraftpb.ConfChange
	cc.Unmarshal(entry.Data)
	var cmdMsg raft_cmdpb.RaftCmdRequest
	cmdMsg.Unmarshal(cc.Context)
	changePeer := cmdMsg.AdminRequest.ChangePeer

	log.Infof("%s applyConfChange index=%d type=%s nodeId=%d", d.Tag, entry.Index, cc.ChangeType, cc.NodeId)

	// Notify the raft layer so it updates its internal peer routing table.
	d.RaftGroup.ApplyConfChange(cc)

	// Get a mutable copy of the region to update peer list and epoch.
	region := d.Region()

	peerListChanged := false
	if changePeer.ChangeType == eraftpb.ConfChangeType_AddNode {
		// If exact same peer ID is already present, this is a duplicate committed entry
		// (e.g. scheduler re-sent AddNode before the first one was visible). Treat it as
		// a no-op: do not modify the peer list and do not bump ConfVer, since the
		// scheduler requires ConfVer to increment by exactly 1 per visible peer-count change.
		alreadyMember := false
		for _, p := range region.Peers {
			if p.Id == changePeer.Peer.Id {
				alreadyMember = true
				break
			}
		}
		if alreadyMember {
			log.Infof("%s applyConfChange SKIP duplicate AddNode peer %v (already member)", d.Tag, changePeer.Peer)
		} else {
			// Check if a different peer on the same store already exists (conflict).
			for _, p := range region.Peers {
				if p.StoreId == changePeer.Peer.StoreId {
					log.Errorf("%s can't add duplicated peer %v to region %v", d.Tag, changePeer.Peer, region)
					return
				}
			}
			region.Peers = append(region.Peers, changePeer.Peer)
			d.insertPeerCache(changePeer.Peer)
			peerListChanged = true
		}
	} else {
		// Find and remove the peer; validate it matches exactly.
		found := false
		for i, p := range region.Peers {
			if p.Id == changePeer.Peer.Id {
				found = true
				region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
				break
			}
		}
		if !found {
			// Duplicate committed RemoveNode (same peer removed by an earlier entry).
			// Treat as a no-op: still advance AppliedIndex so progress isn't stalled.
			log.Infof("%s applyConfChange SKIP duplicate RemoveNode peer %v (already removed)", d.Tag, changePeer.Peer)
			kvWB := new(engine_util.WriteBatch)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.ctx.engine.Kv)
			d.handleProposals(entry, nil)
			return
		}
		// If we are the removed peer, broadcast a final heartbeat so
		// followers can learn the latest commit index before we disappear.
		// Then destroy ourselves.
		if changePeer.Peer.Id == d.PeerId() {
			kvWB := new(engine_util.WriteBatch)
			d.peerStorage.applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone)
			kvWB.WriteToDB(d.ctx.engine.Kv)
			// ApplyConfChange has already been called above, so Raft.Prs is
			// updated. BcastHeartbeat will reach the remaining peers.
			d.RaftGroup.Raft.BcastHeartbeat()
			rd := d.RaftGroup.Ready()
			if len(rd.Messages) != 0 {
				d.Send(d.ctx.trans, rd.Messages)
			}
			d.RaftGroup.Advance(rd)
			d.destroyPeer()
			return
		}
		d.removePeerCache(changePeer.Peer.Id)
		peerListChanged = true
	}

	// Only bump ConfVer when the peer list actually changed. Duplicate committed entries
	// (same peer ID added twice) must not inflate ConfVer, or the scheduler will see an
	// epoch jump with an unchanged peer count and panic ("unmatched version").
	if peerListChanged {
		region.RegionEpoch.ConfVer++
	}
	log.Infof("%s applyConfChange EPOCH AFTER: confVer=%d version=%d peers=%v changed=%v",
		d.Tag, region.RegionEpoch.ConfVer, region.RegionEpoch.Version, region.Peers, peerListChanged)

	// Persist the updated region state and apply index atomically.
	kvWB := new(engine_util.WriteBatch)
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	d.peerStorage.applyState.AppliedIndex = entry.Index
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	kvWB.WriteToDB(d.ctx.engine.Kv)

	// Update peerStorage.region so d.Region() returns the new membership for proposal checks.
	d.SetRegion(region)

	// Update the in-memory region map so the router and snapshot checks stay consistent.
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regions[d.regionId] = region
	d.ctx.storeMeta.Unlock()

	// Tell the scheduler about the new region shape so it doesn't report "no region".
	if peerListChanged {
		d.notifyHeartbeatScheduler(region, d.peer)
	}

	// Respond to the original proposal callback.
	resp := &raft_cmdpb.RaftCmdResponse{
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region},
		},
	}
	d.handleProposals(entry, resp)
}

// applyAdminRequest applies a committed admin entry (currently only CompactLog).
// ConfChange entries are handled separately in applyConfChange.
func (d *peerMsgHandler) applyAdminRequest(entry eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLog := msg.AdminRequest.GetCompactLog()
		// Only compact if the new index advances the truncated state.
		if compactLog.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
			d.peerStorage.applyState.TruncatedState.Index = compactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = compactLog.CompactTerm
			d.ScheduleCompactLog(compactLog.CompactIndex)
		}
	}
	d.peerStorage.applyState.AppliedIndex = entry.Index
	kvWB := new(engine_util.WriteBatch)
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	kvWB.WriteToDB(d.ctx.engine.Kv)
	d.handleProposals(entry, nil)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		log.Debugf("%s proposeRaftCommand rejected: %v", d.Tag, err)
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}

	if msg.AdminRequest != nil {
		log.Infof("%s propose admin cmd type=%s", d.Tag, msg.AdminRequest.CmdType)
	} else {
		log.Debugf("%s propose %d requests", d.Tag, len(msg.Requests))
	}

	data, marErr := msg.Marshal()
	if marErr != nil {
		log.Fatal(marErr)
	}
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			target := msg.AdminRequest.TransferLeader.Peer
			if target == nil {
				if cb != nil {
					cb.Done(ErrResp(errors.New("invalid transfer leader request: missing target peer id")))
				}
				return
			}
			d.RaftGroup.TransferLeader(target.Id)
			if cb != nil {
				resp := newCmdResp()
				resp.AdminResponse = &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
				}
				cb.Done(resp)
			}
			return
		case raft_cmdpb.AdminCmdType_ChangePeer:
			log.Infof("%s propose conf change %s\n", d.Tag, msg.AdminRequest.ChangePeer)
			changePeer := msg.AdminRequest.ChangePeer
			// Reject if a conf change is already pending (not yet applied).
			if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
				log.Infof("%s REJECT conf change (pending): pendingConfIndex=%d appliedIndex=%d",
					d.Tag, d.RaftGroup.Raft.PendingConfIndex, d.peerStorage.AppliedIndex())
				if cb != nil {
					cb.Done(ErrResp(errors.New("pending conf change")))
				}
				return
			}
			// Reject redundant adds and removes at proposal time.
			peerExists := false
			for _, p := range d.Region().Peers {
				if p.Id == changePeer.Peer.Id {
					peerExists = true
					break
				}
			}
			log.Infof("%s conf change %s: peerExists=%v currentPeers=%v",
				d.Tag, msg.AdminRequest.ChangePeer, peerExists, d.Region().Peers)
			if changePeer.ChangeType == eraftpb.ConfChangeType_AddNode && peerExists {
				// Peer already a full member — no-op.
				if cb != nil {
					cb.Done(newCmdResp())
				}
				return
			}
			if changePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && !peerExists {
				// Peer already gone — no-op.
				if cb != nil {
					cb.Done(newCmdResp())
				}
				return
			}
			cc := eraftpb.ConfChange{
				ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
				NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
				Context:    data, // marshaled RaftCmdRequest
			}
			d.proposals = append(d.proposals, &proposal{
				term:  d.Term(),
				index: d.nextProposalIndex(),
				cb:    cb,
			})
			d.RaftGroup.ProposeConfChange(cc)
			return
		}
	}
	// Your Code Here (2B).
	d.proposals = append(d.proposals, &proposal{
		term:  d.Term(),
		index: d.nextProposalIndex(),
		cb:    cb,
	})

	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	log.Infof("%s sending heartbeat to scheduler: confVer=%d version=%d peerCount=%d",
		peer.Tag, clonedRegion.RegionEpoch.ConfVer, clonedRegion.RegionEpoch.Version, len(clonedRegion.Peers))
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}
