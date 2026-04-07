# Project 3B ConfChange Roadmap

## Step 1: Propose ConfChange

**File:** `kv/raftstore/peer_msg_handler.go` — `proposeRaftCommand()`

In the `AdminCmdType_ChangePeer` case, instead of calling `d.RaftGroup.Propose()`, call `d.RaftGroup.ProposeConfChange()`:

```go
case raft_cmdpb.AdminCmdType_ChangePeer:
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
```

Note: `data` is the marshaled `msg` (same as normal propose). You need to marshal `msg` before building the `ConfChange`.

**Test after this step:** None yet — applying is needed first.

---

## Step 2: Apply ConfChange

**File:** `kv/raftstore/peer_msg_handler.go` — `applyAdminRequest()`

Add a `AdminCmdType_ChangePeer` case. Steps:

1. Unmarshal the `ConfChange` from the entry data (entry type is `EntryConfChange`):
   ```go
   var cc eraftpb.ConfChange
   cc.Unmarshal(entry.Data)
   ```

2. Call `d.RaftGroup.ApplyConfChange(cc)` to update raft-layer peer list.

3. Modify `region.Peers`:
   - `ConfChangeType_AddNode`: append the new peer to `region.Peers`
   - `ConfChangeType_RemoveNode`: remove the peer from `region.Peers`

4. If removing self (`peer.Id == d.PeerId()`):
   - Call `d.destroyPeer()` and return immediately.

5. Increment `region.RegionEpoch.ConfVer++`.

6. Persist the updated region using `meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)`.

7. Update peer cache:
   - Add: `d.insertPeerCache(peer)`
   - Remove: `d.removePeerCache(peer.Id)`

8. Notify scheduler (so it doesn't report "no region"):
   ```go
   d.notifyHeartbeatScheduler(region, d.peer)
   ```

9. Call `d.handleProposals(entry, resp)` with an `AdminResponse`:
   ```go
   resp := &raft_cmdpb.RaftCmdResponse{
       AdminResponse: &raft_cmdpb.AdminResponse{
           CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
           ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region},
       },
   }
   ```

**Important:** ConfChange entries have `entry.EntryType == eraftpb.EntryType_EntryConfChange`. You need to detect this in `appendEntry()` and route accordingly (the `entry.Data` is a raw `ConfChange`, not a `RaftCmdRequest`). The `RaftCmdRequest` is stored inside `cc.Context`.

So in `appendEntry`:
```go
if entry.EntryType == eraftpb.EntryType_EntryConfChange {
    d.applyConfChange(entry)
    return
}
```

**Test after this step:** `TestBasicConfChange3B`

---

## Step 3: Handle Duplicate ConfChange (PendingConfIndex)

**File:** `kv/raftstore/peer_msg_handler.go` — `proposeRaftCommand()`

Before proposing a ConfChange, check if there's already a pending one:
```go
if d.RaftGroup.Raft.PendingConfIndex > d.peerStorage.AppliedIndex() {
    // already a pending conf change, reject
    if cb != nil {
        cb.Done(ErrResp(errors.New("pending conf change")))
    }
    return
}
```

This prevents two overlapping conf changes in the log simultaneously.

**Test after this step:** `TestConfChangeRecover3B`, `TestConfChangeRecoverManyClients3B`

---

## Step 4: Handle storeMeta Updates

**File:** `kv/raftstore/peer_msg_handler.go` — in the apply ConfChange path

When applying ConfChange, update `d.ctx.storeMeta`:
```go
d.ctx.storeMeta.Lock()
d.ctx.storeMeta.regions[d.regionId] = region
d.ctx.storeMeta.Unlock()
```

This must be done under the lock. Without this, the router/snapshot checks may fail.

**Test after this step:** `TestConfChangeUnreliable3B`

---

## Summary Table

| Step | What to implement | Test to run |
|------|-------------------|-------------|
| 1 | Propose ConfChange via `ProposeConfChange` | — |
| 2 | Apply ConfChange: update peers, epoch, persist, notify | `TestBasicConfChange3B` |
| 3 | Reject duplicate ConfChange (PendingConfIndex check) | `TestConfChangeRecover3B`, `TestConfChangeRecoverManyClients3B` |
| 4 | Update `storeMeta.regions` under lock | `TestConfChangeUnreliable3B` |

---

## Key Details to Watch Out For

- **EntryType**: ConfChange entries have `EntryType_EntryConfChange`, not `EntryType_EntryNormal`. Check this in `appendEntry` to route correctly.
- **Context field**: The `RaftCmdRequest` is encoded in `cc.Context`, not `entry.Data` directly.
- **destroyPeer**: Must return immediately after — don't update region or call handleProposals.
- **ApplyConfChange return value**: It returns a `*pb.ConfState`; you don't need to use it explicitly.
- **notifyHeartbeatScheduler**: Required to avoid "no region" errors from the mock scheduler.
