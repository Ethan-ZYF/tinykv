# TinyKV Project 2 Complete Implementation Guide (English)

## Table of Contents
- [Overview](#overview)
- [Project 2AA: Leader Election](#project-2aa-leader-election)
- [Project 2AB: Log Replication](#project-2ab-log-replication)
- [Complete Function Reference](#complete-function-reference)
- [Debugging Tips](#debugging-tips)

---

## Overview

This guide provides a **complete walkthrough** of implementing Raft consensus algorithm in TinyKV, covering both leader election (2AA) and log replication (2AB).

### Key Files
```
raft/
├── raft.go      # Main Raft logic (YOU IMPLEMENT THIS)
├── log.go       # Log management (YOU IMPLEMENT THIS)
├── rawnode.go   # RawNode interface (Project 2AC)
└── storage.go   # Storage interface (provided)
```

---

## Project 2AA: Leader Election

### Core Data Structures

#### 1. Raft struct
**Location**: `raft/raft.go:130`

```go
type Raft struct {
    id    uint64    // Node ID
    Term  uint64    // Current term
    Vote  uint64    // Who we voted for in current term

    RaftLog *RaftLog               // Log manager
    Prs     map[uint64]*Progress   // Progress of each peer
    State   StateType              // Current state (Follower/Candidate/Leader)

    votes   map[uint64]bool        // Vote tracking (for candidates)
    msgs    []pb.Message           // Outgoing message queue
    Lead    uint64                 // Current leader ID

    heartbeatTimeout    int        // Heartbeat interval (ticks)
    electionTimeout     int        // Election timeout (ticks, randomized)
    heartbeatElapsed    int        // Ticks since last heartbeat
    electionElapsed     int        // Ticks since last election/message
    electionTimeoutBase int        // Base value for randomization
}
```

**Understanding each field**:
- `Term`: Logical clock, increases with each election
- `Vote`: Ensures "one vote per term" rule
- `State`: Current role (Follower/Candidate/Leader)
- `msgs`: Messages to be sent by upper layer
- `electionTimeout`: Randomized to prevent split votes

#### 2. Progress struct
**Location**: `raft/raft.go:121`

```go
type Progress struct {
    Match uint64  // Highest log index known to be replicated
    Next  uint64  // Next log index to send to this peer
}
```

**Purpose**: Leader tracks each follower's replication status

---

### Core Functions Explained

#### 1. `newRaft()` - Initialization
**Location**: `raft/raft.go:191`

**Purpose**: Create and initialize a Raft node

**What it does**:
```go
func newRaft(c *Config) *Raft {
    // 1. Validate config
    if err := c.validate(); err != nil {
        panic(err.Error())
    }

    // 2. Create Raft instance with initial values
    r := &Raft{
        id:    c.ID,
        Term:  0,         // Start at term 0
        Vote:  None,      // Haven't voted yet
        State: StateFollower,  // All nodes start as followers
        // ... other fields
    }

    // 3. Restore persisted state (Term, Vote) from storage
    hardState, _, _ := c.Storage.InitialState()
    if hardState.Term != 0 {
        r.Term = hardState.Term
    }
    if hardState.Vote != 0 {
        r.Vote = hardState.Vote
    }

    // 4. Initialize Progress for all peers
    for _, peer := range c.peers {
        r.Prs[peer] = &Progress{}
    }

    // 5. Randomize election timeout
    r.resetElectionTimeout()

    return r
}
```

**Why restore from storage?**:
- Nodes can crash and restart
- Must remember who they voted for (prevents voting twice in same term)
- Must remember their term (prevents going backwards)

---

#### 2. `tick()` - Time Advancement
**Location**: `raft/raft.go:231`

**Purpose**: Advance logical clock and trigger timeouts

**What it does**:
```go
func (r *Raft) tick() {
    switch r.State {
    case StateFollower, StateCandidate:
        // Check for election timeout
        r.electionElapsed++
        if r.electionElapsed >= r.electionTimeout {
            r.electionElapsed = 0
            // Trigger election by sending MsgHup to self
            r.Step(pb.Message{
                From: r.id,
                To: r.id,
                MsgType: pb.MessageType_MsgHup
            })
        }

    case StateLeader:
        // Check for heartbeat timeout
        r.heartbeatElapsed++
        if r.heartbeatElapsed >= r.heartbeatTimeout {
            r.heartbeatElapsed = 0
            // Trigger heartbeat by sending MsgBeat to self
            r.Step(pb.Message{
                From: r.id,
                To: r.id,
                MsgType: pb.MessageType_MsgBeat
            })
        }
    }
}
```

**Key insight**: Uses internal messages (MsgHup, MsgBeat) to trigger actions

---

#### 3. `Step()` - Message Entry Point
**Location**: `raft/raft.go:259`

**Purpose**: Main message dispatcher

**What it does**:
```go
func (r *Raft) Step(m pb.Message) error {
    // 1. Update term if we receive higher term
    if m.Term > r.Term && !IsLocalMsg(m.MsgType) {
        lead := m.From
        if m.MsgType == pb.MessageType_MsgRequestVote {
            lead = None  // Don't know leader during election
        }
        r.becomeFollower(m.Term, lead)
    }

    // 2. Dispatch to state-specific handler
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
```

**Why check term first?**: Ensures we're always up-to-date before processing

---

#### 4. `becomeFollower()` - Transition to Follower
**Location**: `raft/raft.go:353`

**Purpose**: Become a follower

**What it does**:
```go
func (r *Raft) becomeFollower(term uint64, lead uint64) {
    r.State = StateFollower
    r.Term = term
    r.Lead = lead
    r.Vote = None              // New term = new vote
    r.electionElapsed = 0      // Reset timer
    r.resetElectionTimeout()   // Randomize timeout
}
```

**When called**:
- Receiving message with higher term
- Losing an election
- Receiving heartbeat from leader

---

#### 5. `becomeCandidate()` - Transition to Candidate
**Location**: `raft/raft.go:365`

**Purpose**: Start an election

**What it does**:
```go
func (r *Raft) becomeCandidate() {
    r.State = StateCandidate
    r.Term++                    // Increment term
    r.Vote = r.id               // Vote for self
    r.Lead = None               // No leader during election
    r.electionElapsed = 0       // Reset timer
    r.votes = make(map[uint64]bool)
    r.votes[r.id] = true        // Record self-vote
    r.resetElectionTimeout()    // Randomize timeout
}
```

**Key point**: Term MUST be incremented before requesting votes

---

#### 6. `becomeLeader()` - Transition to Leader
**Location**: `raft/raft.go:379`

**Purpose**: Become the leader

**What it does**:
```go
func (r *Raft) becomeLeader() {
    r.State = StateLeader
    r.Lead = r.id
    r.heartbeatElapsed = 0

    // Initialize Progress for all peers
    lastIndex := r.RaftLog.LastIndex()
    for id := range r.Prs {
        r.Prs[id] = &Progress{
            Match: 0,              // Don't know what they have
            Next:  lastIndex + 1,  // Start from next entry
        }
    }

    // Append no-op entry (CRITICAL for Raft safety)
    r.Step(pb.Message{
        From:    r.id,
        To:      r.id,
        MsgType: pb.MessageType_MsgPropose,
        Entries: []*pb.Entry{{Data: nil}},
    })
}
```

**Why no-op entry?**:
- Leader can't directly commit logs from previous terms
- Must commit current-term log first to "activate" old logs
- See Raft paper Section 5.4.2, Figure 8

---

#### 7. `campaign()` - Start Election
**Location**: `raft/raft.go:411`

**Purpose**: Initiate election campaign

**What it does**:
```go
func (r *Raft) campaign() {
    r.becomeCandidate()

    // Special case: single node cluster
    if len(r.Prs) == 1 {
        r.becomeLeader()
        return
    }

    // Get log info for vote comparison
    lastIndex := r.RaftLog.LastIndex()
    lastLogTerm, _ := r.RaftLog.Term(lastIndex)

    // Send RequestVote to all peers
    for peer := range r.Prs {
        if peer == r.id {
            continue  // Skip self
        }
        r.send(pb.Message{
            MsgType: pb.MessageType_MsgRequestVote,
            To:      peer,
            From:    r.id,
            Term:    r.Term,
            LogTerm: lastLogTerm,  // For log comparison
            Index:   lastIndex,
        })
    }
}
```

**Log comparison**: Prevents nodes with stale logs from becoming leader

---

#### 8. `handleRequestVote()` - Process Vote Request
**Location**: `raft/raft.go:465`

**Purpose**: Decide whether to grant vote

**What it does**:
```go
func (r *Raft) handleRequestVote(m pb.Message) {
    // Rule 1: One vote per term (first-come-first-served)
    canVote := r.Vote == None || r.Vote == m.From

    // Rule 2: Only vote for candidates with up-to-date logs
    lastIndex := r.RaftLog.LastIndex()
    lastTerm, _ := r.RaftLog.Term(lastIndex)

    // Candidate's log is up-to-date if:
    // 1. Last log term is higher, OR
    // 2. Same term but equal or longer log
    logUpToDate := m.LogTerm > lastTerm ||
        (m.LogTerm == lastTerm && m.Index >= lastIndex)

    // Grant vote if both conditions met
    reject := true
    if canVote && logUpToDate {
        reject = false
        r.Vote = m.From              // Record vote
        r.electionElapsed = 0        // Reset timer
    }

    // Send response
    r.send(pb.Message{
        MsgType: pb.MessageType_MsgRequestVoteResponse,
        To:      m.From,
        From:    r.id,
        Term:    r.Term,
        Reject:  reject,
    })
}
```

**Why log comparison?**: Ensures **Leader Completeness** property (Raft paper 5.4.1)

---

#### 9. `handleRequestVoteResponse()` - Count Votes
**Location**: `raft/raft.go:508`

**Purpose**: Tally votes and decide election outcome

**What it does**:
```go
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
    // Record vote
    r.votes[m.From] = !m.Reject

    // Count granted votes
    granted := 0
    for _, vote := range r.votes {
        if vote {
            granted++
        }
    }

    quorum := len(r.Prs)/2 + 1  // Majority

    // Win: got majority
    if granted >= quorum {
        r.becomeLeader()
        return
    }

    // Lose: rejected by majority
    rejected := len(r.votes) - granted
    if rejected >= quorum {
        r.becomeFollower(r.Term, None)
    }

    // Otherwise: wait for more responses
}
```

**Quorum math**: In 5-node cluster, need 3 votes (including self)

---

#### 10. `bcastHeartbeat()` / `sendHeartbeat()` - Heartbeats
**Location**: `raft/raft.go:543, 554`

**Purpose**: Prevent followers from starting elections

**What it does**:
```go
func (r *Raft) bcastHeartbeat() {
    for peer := range r.Prs {
        if peer != r.id {
            r.sendHeartbeat(peer)
        }
    }
}

func (r *Raft) sendHeartbeat(to uint64) {
    r.send(pb.Message{
        MsgType: pb.MessageType_MsgHeartbeat,
        To:      to,
        From:    r.id,
        Term:    r.Term,
        Commit:  r.RaftLog.committed,  // Tell follower commit index
    })
}
```

**Why send commit?**: Followers need to know what's committed

---

#### 11. `handleHeartbeat()` - Receive Heartbeat
**Location**: `raft/raft.go:567`

**Purpose**: Reset election timer, stay as follower

**What it does**:
```go
func (r *Raft) handleHeartbeat(m pb.Message) {
    r.electionElapsed = 0  // Reset timer (CRITICAL!)
    r.Lead = m.From        // Remember leader

    // Note: Don't update committed here
    // Only AppendEntries verifies logs

    r.send(pb.Message{
        MsgType: pb.MessageType_MsgHeartbeatResponse,
        To:      m.From,
        From:    r.id,
        Term:    r.Term,
    })
}
```

**Why not update committed?**: Heartbeats don't verify log consistency

---

#### 12. `resetElectionTimeout()` - Randomize Timeout
**Location**: `raft/raft.go:633`

**Purpose**: Prevent split votes

**What it does**:
```go
func (r *Raft) resetElectionTimeout() {
    // Random timeout in [base, 2*base)
    r.electionTimeout = r.electionTimeoutBase +
        rand.Intn(r.electionTimeoutBase)
}
```

**Why randomize?**:
- Without it, all nodes timeout simultaneously
- All become candidates → split vote → repeat
- With randomization, usually one node times out first and wins

---

## Project 2AB: Log Replication

### Additional Data Structures

#### RaftLog struct
**Location**: `raft/log.go:27`

```go
type RaftLog struct {
    storage Storage      // Persistent storage

    committed uint64     // Highest committed index
    applied   uint64     // Highest applied index
    stabled   uint64     // Highest persisted index

    entries []pb.Entry   // In-memory log entries

    pendingSnapshot *pb.Snapshot  // For 2C
}
```

**Index relationships**:
```
first <= applied <= committed <= stabled <= last
```

---

### Log Management Functions

#### 1. `newLog()` - Initialize Log
**Location**: `raft/log.go:57`

**What it does**:
```go
func newLog(storage Storage) *RaftLog {
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
```

**Why `firstIndex - 1`?**: Before first entry means nothing committed/applied yet

---

#### 2. `LastIndex()` - Get Last Index
**Location**: `raft/log.go:128`

**What it does**:
```go
func (l *RaftLog) LastIndex() uint64 {
    if len(l.entries) == 0 {
        lastIndex, _ := l.storage.LastIndex()
        return lastIndex
    }
    return l.entries[len(l.entries)-1].Index
}
```

**Two cases**: Check memory first, fallback to storage

---

#### 3. `Term()` - Get Term at Index
**Location**: `raft/log.go:137`

**What it does**:
```go
func (l *RaftLog) Term(i uint64) (uint64, error) {
    if len(l.entries) == 0 {
        return l.storage.Term(i)
    }

    firstIndex := l.entries[0].Index
    lastIndex := l.LastIndex()

    if i < firstIndex {
        return l.storage.Term(i)  // In storage
    }
    if i > lastIndex {
        return 0, ErrUnavailable  // Out of range
    }

    return l.entries[i-firstIndex].Term, nil
}
```

**Three regions**: storage, memory, and invalid

---

#### 4. `appendEntries()` - Append New Entries
**Location**: `raft/log.go:162`

**Purpose**: Add entries, handling conflicts

**What it does**:
```go
func (l *RaftLog) appendEntries(entries []pb.Entry) {
    if len(entries) == 0 {
        return
    }

    firstNewIndex := entries[0].Index

    if len(l.entries) == 0 {
        l.entries = entries
        return
    }

    firstIndex := l.entries[0].Index
    lastIndex := l.LastIndex()

    // Case 1: Entries completely after existing
    if firstNewIndex > lastIndex {
        l.entries = append(l.entries, entries...)
        return
    }

    // Case 2: Overlapping - check for conflicts
    for i, entry := range entries {
        idx := entry.Index

        if idx > lastIndex {
            // Beyond our log, append remaining
            l.entries = append(l.entries, entries[i:]...)
            return
        }

        // Check for conflict (same index, different term)
        existingTerm, _ := l.Term(idx)
        if existingTerm != entry.Term {
            // Conflict! Truncate and append
            if idx <= l.stabled {
                l.stabled = idx - 1  // Update stabled
            }
            offset := idx - firstIndex
            l.entries = append(l.entries[:offset], entries[i:]...)
            return
        }
    }

    // No conflicts, all entries already exist
}
```

**Key point**: When truncating stable entries, must update `stabled`

---

#### 5. `unstableEntries()` - Get Unstable Entries
**Location**: `raft/log.go:90`

**Purpose**: Return entries that need persistence

**What it does**:
```go
func (l *RaftLog) unstableEntries() []pb.Entry {
    if len(l.entries) == 0 {
        return []pb.Entry{}
    }

    if l.stabled >= l.LastIndex() {
        return []pb.Entry{}  // All stable
    }

    // Return entries after stabled
    firstUnstable := l.stabled + 1
    firstIndex := l.entries[0].Index
    return l.entries[firstUnstable-firstIndex:]
}
```

**Why return `[]` not `nil`?**: Tests use `reflect.DeepEqual`

---

#### 6. `nextEnts()` - Get Committed Unapplied Entries
**Location**: `raft/log.go:108`

**Purpose**: Return entries ready to apply to state machine

**What it does**:
```go
func (l *RaftLog) nextEnts() []pb.Entry {
    if len(l.entries) == 0 {
        return []pb.Entry{}
    }

    if l.applied >= l.committed {
        return []pb.Entry{}  // Nothing new
    }

    firstIndex := l.entries[0].Index
    start := l.applied + 1 - firstIndex
    end := l.committed + 1 - firstIndex

    return l.entries[start:end]  // (applied, committed]
}
```

**Range**: Entries in `(applied, committed]`

---

### Leader Functions

#### 7. `handleMsgPropose()` - Leader Receives Proposal
**Location**: `raft/raft.go:442`

**Purpose**: Leader receives new log from client

**What it does**:
```go
func (r *Raft) handleMsgPropose(m pb.Message) {
    if len(m.Entries) == 0 {
        return
    }

    // 1. Append to leader's log
    lastIndex := r.RaftLog.LastIndex()
    for i, entry := range m.Entries {
        entry.Index = lastIndex + uint64(i) + 1
        entry.Term = r.Term
        r.RaftLog.entries = append(r.RaftLog.entries, *entry)
    }

    // 2. Update leader's Progress
    r.Prs[r.id].Match = r.RaftLog.LastIndex()
    r.Prs[r.id].Next = r.Prs[r.id].Match + 1

    // 3. Single-node cluster commits immediately
    if len(r.Prs) == 1 {
        r.RaftLog.committed = r.RaftLog.LastIndex()
        return
    }

    // 4. Replicate to followers
    r.bcastAppend()
}
```

**Flow**: Append locally → Update Progress → Broadcast

---

#### 8. `sendAppend()` - Send AppendEntries to Peer
**Location**: `raft/raft.go:598`

**Purpose**: Replicate logs to one follower

**What it does**:
```go
func (r *Raft) sendAppend(to uint64) bool {
    pr := r.Prs[to]

    // Get prevLog info for consistency check
    prevLogIndex := pr.Next - 1
    prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
    if err != nil {
        return false  // Need snapshot (2C)
    }

    // Get entries to send
    entries := make([]*pb.Entry, 0)
    if len(r.RaftLog.entries) > 0 {
        lastIndex := r.RaftLog.LastIndex()
        if pr.Next <= lastIndex {
            firstIndex := r.RaftLog.entries[0].Index
            slice := r.RaftLog.entries[pr.Next-firstIndex:]
            for i := range slice {
                entries = append(entries, &slice[i])
            }
        }
    }

    r.send(pb.Message{
        MsgType: pb.MessageType_MsgAppend,
        To:      to,
        From:    r.id,
        Term:    r.Term,
        LogTerm: prevLogTerm,   // For consistency check
        Index:   prevLogIndex,  // For consistency check
        Entries: entries,
        Commit:  r.RaftLog.committed,
    })
    return true
}
```

**Key fields**:
- `Index`, `LogTerm`: For follower to check consistency
- `Entries`: New logs to append
- `Commit`: Tell follower what's committed

---

#### 9. `handleAppendResponse()` - Process Replication Response
**Location**: `raft/raft.go:693`

**Purpose**: Update Progress and try to commit

**What it does**:
```go
func (r *Raft) handleAppendResponse(m pb.Message) {
    pr := r.Prs[m.From]

    if m.Reject {
        // Log mismatch, backtrack
        pr.Next = max(1, pr.Next-1)
        r.sendAppend(m.From)  // Retry
        return
    }

    // Success, update Progress
    if m.Index > pr.Match {
        pr.Match = m.Index
        pr.Next = pr.Match + 1

        // Try to commit
        r.maybeCommit()
    }
}
```

**Two paths**: Rejection (retry) or Success (update & commit)

---

#### 10. `maybeCommit()` - Attempt to Commit Logs
**Location**: `raft/raft.go:715`

**Purpose**: Commit logs replicated to majority

**What it does**:
```go
func (r *Raft) maybeCommit() {
    // Collect all Match indices
    matches := make([]uint64, 0, len(r.Prs))
    for _, pr := range r.Prs {
        matches = append(matches, pr.Match)
    }

    // Sort descending
    sort.Slice(matches, func(i, j int) bool {
        return matches[i] > matches[j]
    })

    // Find median (majority's minimum)
    n := matches[len(matches)/2]

    // Can only commit current-term logs (Raft safety)
    if n > r.RaftLog.committed {
        logTerm, _ := r.RaftLog.Term(n)
        if logTerm == r.Term {
            oldCommitted := r.RaftLog.committed
            r.RaftLog.committed = n

            // Broadcast updated commit index
            if r.RaftLog.committed > oldCommitted {
                r.bcastAppend()
            }
        }
    }
}
```

**Why only current term?**: Raft paper Figure 8 - prevents safety violation

**Example**:
```
5 nodes: Match = [10, 10, 8, 7, 5]
Sorted:  [10, 10, 8, 7, 5]
Median (index 2): 8
→ Can commit up to index 8 (replicated on 3/5 nodes)
```

---

### Follower Functions

#### 11. `handleAppendEntries()` - Follower Receives Logs
**Location**: `raft/raft.go:634`

**Purpose**: Verify consistency and append logs

**What it does**:
```go
func (r *Raft) handleAppendEntries(m pb.Message) {
    r.electionElapsed = 0  // Reset timer
    r.Lead = m.From

    lastIndex := r.RaftLog.LastIndex()

    // Check 1: Do we have prevLogIndex?
    if m.Index > lastIndex {
        // Reject: log too short
        r.send(pb.Message{
            MsgType: pb.MessageType_MsgAppendResponse,
            To:      m.From,
            From:    r.id,
            Term:    r.Term,
            Reject:  true,
            Index:   lastIndex,
        })
        return
    }

    // Check 2: Does prevLogTerm match?
    if m.Index > 0 {
        prevLogTerm, err := r.RaftLog.Term(m.Index)
        if err != nil || prevLogTerm != m.LogTerm {
            // Reject: term mismatch
            r.send(pb.Message{
                MsgType: pb.MessageType_MsgAppendResponse,
                To:      m.From,
                From:    r.id,
                Term:    r.Term,
                Reject:  true,
                Index:   m.Index,
            })
            return
        }
    }

    // Consistency checks passed, append entries
    if len(m.Entries) > 0 {
        entries := make([]pb.Entry, 0, len(m.Entries))
        for _, e := range m.Entries {
            entries = append(entries, *e)
        }
        r.RaftLog.appendEntries(entries)
    }

    // Update committed
    if m.Commit > r.RaftLog.committed {
        // Commit up to min(leaderCommit, last new entry)
        lastNewEntry := m.Index
        if len(m.Entries) > 0 {
            lastNewEntry = m.Entries[len(m.Entries)-1].Index
        }
        r.RaftLog.committed = min(m.Commit, lastNewEntry)
    }

    // Send success
    r.send(pb.Message{
        MsgType: pb.MessageType_MsgAppendResponse,
        To:      m.From,
        From:    r.id,
        Term:    r.Term,
        Reject:  false,
        Index:   r.RaftLog.LastIndex(),
    })
}
```

**Three stages**:
1. Consistency check (prevLogIndex, prevLogTerm)
2. Append entries (handle conflicts)
3. Update committed

**Why `min(m.Commit, lastNewEntry)`?**: Can't commit beyond what we have

---

### Heartbeat Optimization

#### 12. `handleHeartbeatResponse()` - Trigger Catchup
**Location**: `raft/raft.go:683`

**Purpose**: Send logs to lagging followers

**What it does**:
```go
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
    pr := r.Prs[m.From]

    // If follower is behind, send AppendEntries
    if pr.Match < r.RaftLog.LastIndex() {
        r.sendAppend(m.From)
    }
}
```

**Why needed?**: Heartbeat is lightweight (no entries). When follower is behind, we need to send actual logs.

---

## Complete Function Reference

### Message Flow Diagrams

#### Election Flow
```
Follower timeout
    ↓
tick() detects timeout
    ↓
Step(MsgHup) → campaign()
    ↓
becomeCandidate() (Term++, Vote=self)
    ↓
send RequestVote to all peers
    ↓
Peers: handleRequestVote()
    ↓
send RequestVoteResponse
    ↓
Candidate: handleRequestVoteResponse()
    ↓
Count votes → becomeLeader() or becomeFollower()
```

#### Log Replication Flow
```
Client → Leader: Propose
    ↓
Leader: handleMsgPropose()
    ↓
Append to local log, update Progress
    ↓
bcastAppend() → sendAppend(to each follower)
    ↓
Follower: handleAppendEntries()
    ↓
Check consistency, append entries, update committed
    ↓
send AppendResponse
    ↓
Leader: handleAppendResponse()
    ↓
Update Progress, maybeCommit()
    ↓
Find quorum → update committed
    ↓
bcastAppend() to notify followers
```

---

## Debugging Tips

### 1. Add State Logging
```go
func (r *Raft) Step(m pb.Message) error {
    log.Debugf("[%s] Node %d received %s from %d (term %d)",
        r.State, r.id, m.MsgType, m.From, m.Term)
    // ...
}
```

### 2. Check Invariants
```go
func (r *Raft) checkInvariants() {
    // applied <= committed <= stabled <= last
    if r.RaftLog.applied > r.RaftLog.committed {
        panic("applied > committed")
    }
    // ... more checks
}
```

### 3. Print Progress
```go
func (r *Raft) printProgress() {
    for id, pr := range r.Prs {
        log.Debugf("  Peer %d: Match=%d, Next=%d",
            id, pr.Match, pr.Next)
    }
}
```

### 4. Common Bugs

**Bug**: Leader commits logs from previous term
**Fix**: Check `logTerm == r.Term` in `maybeCommit()`

**Bug**: Follower commits beyond its log
**Fix**: Use `min(m.Commit, lastNewEntry)` not `min(m.Commit, LastIndex())`

**Bug**: Nodes don't restore Vote from storage
**Fix**: Load HardState in `newRaft()`

**Bug**: `nil` vs `[]` in test comparisons
**Fix**: Return `[]pb.Entry{}` not `nil`

---

## Summary

### Project 2AA Key Points
1. **Randomize election timeout** - Prevents split votes
2. **One vote per term** - Check `r.Vote`
3. **Log comparison** - Only vote for up-to-date candidates
4. **Reset timers** - On leader messages
5. **No-op entry** - Leader appends on election

### Project 2AB Key Points
1. **Consistency check** - prevLogIndex + prevLogTerm
2. **Handle conflicts** - Truncate and append
3. **Only commit current term** - Safety requirement
4. **Update stabled on truncate** - When overwriting stable logs
5. **Quorum-based commit** - Use median of Match indices
6. **Follower commit limit** - min(leaderCommit, lastNewEntry)

---

**Last Updated**: 2026-01-28
**Author**: TinyKV Student Guide
