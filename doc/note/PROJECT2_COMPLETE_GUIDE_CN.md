# TinyKV Project 2 完整实现指南（中文版）

## 目录

* [概述](#概述)
* [核心数据结构](#核心数据结构)
* [初始化函数](#初始化函数)
* [状态转换函数](#状态转换函数)
* [选举相关函数](#选举相关函数)
* [心跳相关函数](#心跳相关函数)
* [日志复制函数](#日志复制函数)
* [日志管理函数](#日志管理函数)
* [消息流程图](#消息流程图)
* [调试技巧](#调试技巧)
* [常见错误模式](#常见错误模式)

---

## 概述

TinyKV Project 2 分为两个部分：
* **Project 2AA**: 实现 Raft 领导者选举（Leader Election）
* **Project 2AB**: 实现 Raft 日志复制（Log Replication）

### Raft 角色

1. **Follower（跟随者）**: 被动接收消息，响应选举和日志复制请求
2. **Candidate（候选者）**: 发起选举，尝试成为领导者
3. **Leader（领导者）**: 处理客户端请求，复制日志到跟随者

### 消息类型

* **本地消息**（不通过网络）:
  + `MsgHup`: 选举超时，触发选举
  + `MsgBeat`: 心跳超时，领导者发送心跳
  + `MsgPropose`: 客户端提议，领导者追加日志

* **网络消息**:
  + `MsgRequestVote`: 请求投票
  + `MsgRequestVoteResponse`: 投票响应
  + `MsgAppend`: 追加日志（AppendEntries RPC）
  + `MsgAppendResponse`: 追加日志响应
  + `MsgHeartbeat`: 心跳消息
  + `MsgHeartbeatResponse`: 心跳响应

---

## 核心数据结构

### Raft 结构体

```go
type Raft struct {
    id    uint64           // 当前节点 ID
    Term  uint64           // 当前任期
    Vote  uint64           // 投票给谁（0 表示未投票）

    RaftLog *RaftLog       // 日志管理器

    Prs map[uint64]*Progress  // 进度跟踪器（仅领导者使用）

    State StateType        // 当前角色: Follower/Candidate/Leader

    votes map[uint64]bool  // 收到的投票（候选者使用）

    Lead uint64            // 当前领导者 ID

    heartbeatTimeout int   // 心跳超时（tick 数）
    electionTimeout  int   // 选举超时（tick 数）
    randomElectionTimeout int // 随机化的选举超时

    heartbeatElapsed int   // 自上次心跳以来的 tick 数
    electionElapsed  int   // 自上次选举/心跳以来的 tick 数

    msgs []pb.Message      // 待发送的消息队列
}
```

### Progress 结构体

领导者用它跟踪每个跟随者的日志复制进度：

```go
type Progress struct {
    Match uint64  // 已知已复制的最高日志索引
    Next  uint64   // 下一个要发送的日志索引
}
```

**工作原理**:
* 当一个节点成为领导者时，初始化所有 `Next = lastIndex + 1`,    `Match = 0`
* 发送 AppendEntries 时，从 `Next` 索引开始
* 收到成功响应后，更新 `Match` 和 `Next`
* 收到拒绝响应后，递减 `Next` 并重试

### RaftLog 结构体

```go
type RaftLog struct {
    storage Storage          // 持久化存储

    committed uint64         // 已提交的最高索引
    applied   uint64         // 已应用的最高索引
    stabled   uint64         // 已持久化的最高索引

    entries []pb.Entry       // 内存中的日志条目

    pendingSnapshot *pb.Snapshot  // 待处理的快照（2C 使用）
}
```

**日志索引关系**:

```
applied <= committed <= stabled <= lastIndex
```

* `applied`: 已应用到状态机的日志
* `committed`: 已在多数节点上复制的日志
* `stabled`: 已持久化到存储的日志
* `lastIndex`: 最新的日志索引

---

## 初始化函数

### newRaft()

**作用**: 创建一个新的 Raft 节点实例

**实现要点**:

```go
func newRaft(c *Config) *Raft {
    r := &Raft{
        id:               c.ID,
        Prs:              make(map[uint64]*Progress),
        votes:            make(map[uint64]bool),
        RaftLog:          newLog(c.Storage),
        heartbeatTimeout: c.HeartbeatTick,
        electionTimeout:  c.ElectionTick,
    }

    // 初始化进度跟踪器
    for _, peer := range c.peers {
        r.Prs[peer] = &Progress{}
    }

    // 从存储恢复持久化状态
    hardState, _, _ := c.Storage.InitialState()
    if hardState.Term != 0 {
        r.Term = hardState.Term
    }
    if hardState.Vote != 0 {
        r.Vote = hardState.Vote
    }

    // 成为跟随者
    r.becomeFollower(r.Term, None)

    return r
}
```

**为什么要恢复 HardState**:
* `Term` 和 `Vote` 必须在重启后保持一致
* 防止在同一任期内投票两次
* 这是 Raft 安全性的关键

### newLog()

**作用**: 创建日志管理器

**实现要点**:

```go
func newLog(storage Storage) *RaftLog {
    firstIndex, _ := storage.FirstIndex()
    lastIndex, _ := storage.LastIndex()
    entries, _ := storage.Entries(firstIndex, lastIndex+1)

    return &RaftLog{
        storage:   storage,
        committed: firstIndex - 1,  // 初始化为 firstIndex - 1
        applied:   firstIndex - 1,
        stabled:   lastIndex,
        entries:   entries,
    }
}
```

**为什么 committed 和 applied 初始化为 firstIndex - 1**:
* 当日志为空时，`firstIndex = 1`
* `committed = 0` 表示没有已提交的日志
* 这避免了特殊情况处理

---

## 状态转换函数

### becomeFollower()

**作用**: 切换到跟随者状态

**实现要点**:

```go
func (r *Raft) becomeFollower(term uint64, lead uint64) {
    r.State = StateFollower
    r.Term = term
    r.Vote = None
    r.Lead = lead
    r.electionElapsed = 0
    r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}
```

**何时调用**:
1. 收到更高任期的消息
2. 候选者选举失败
3. 节点初始化时
4. 领导者收到更高任期时

**为什么重置 Vote**:
* 新任期开始时，可以重新投票
* 旧任期的投票不再有效

**为什么随机化选举超时**:
* 防止选票分裂（split vote）
* 让一个节点更可能先超时并赢得选举

### becomeCandidate()

**作用**: 切换到候选者状态并发起选举

**实现要点**:

```go
func (r *Raft) becomeCandidate() {
    r.State = StateCandidate
    r.Term++               // 增加任期
    r.Vote = r.id          // 投票给自己
    r.Lead = None
    r.electionElapsed = 0
    r.votes = make(map[uint64]bool)
    r.votes[r.id] = true   // 记录自己的投票
    r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}
```

**何时调用**:
* 收到 `MsgHup` 消息（选举超时）

**为什么增加任期**:
* Raft 的任期是单调递增的逻辑时钟
* 每次选举都使用新任期

### becomeLeader()

**作用**: 切换到领导者状态

**实现要点**:

```go
func (r *Raft) becomeLeader() {
    r.State = StateLeader
    r.Lead = r.id
    r.heartbeatElapsed = 0

    lastIndex := r.RaftLog.LastIndex()

    // 初始化所有跟随者的进度
    for peer := range r.Prs {
        if peer == r.id {
            r.Prs[peer].Match = lastIndex
            r.Prs[peer].Next = lastIndex + 1
        } else {
            r.Prs[peer].Match = 0
            r.Prs[peer].Next = lastIndex + 1
        }
    }

    // 追加一个空日志条目
    r.RaftLog.appendEntries([]pb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1}})

    // 更新自己的 Match
    r.Prs[r.id].Match = r.RaftLog.LastIndex()
    r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

    // 广播追加日志
    r.bcastAppend()

    // 单节点集群立即提交
    if len(r.Prs) == 1 {
        r.RaftLog.committed = r.RaftLog.LastIndex()
    }
}
```

**为什么追加空日志条目**:
* Raft 论文要求领导者在当选后立即追加一个新任期的日志
* 这有助于提交之前任期的日志
* 确保领导者具有最新的已提交索引

**为什么初始化 Next = lastIndex + 1**:
* 乐观假设跟随者拥有所有日志
* 如果不匹配，会通过拒绝响应逐步回退

---

## 选举相关函数

### tick()

**作用**: 周期性调用的时钟函数

**实现要点**:

```go
func (r *Raft) tick() {
    switch r.State {
    case StateFollower, StateCandidate:
        r.electionElapsed++
        if r.electionElapsed >= r.randomElectionTimeout {
            r.electionElapsed = 0
            r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
        }
    case StateLeader:
        r.heartbeatElapsed++
        if r.heartbeatElapsed >= r.heartbeatTimeout {
            r.heartbeatElapsed = 0
            r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
        }
    }
}
```

**为什么跟随者和候选者使用选举超时**:
* 跟随者: 如果没有收到领导者消息，发起选举
* 候选者: 如果选举失败，重新发起选举

**为什么领导者使用心跳超时**:
* 定期发送心跳维持领导地位
* 防止跟随者超时并发起选举

### Step()

**作用**: 消息处理的入口函数

**实现要点**:

```go
func (r *Raft) Step(m pb.Message) error {
    // 处理任期更新
    if m.Term > r.Term {
        r.becomeFollower(m.Term, None)
    }

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
```

**为什么先检查任期**:
* 收到更高任期的消息时，立即成为跟随者
* 这确保了 Raft 的任期单调性

### campaign()

**作用**: 发起选举活动

**实现要点**:

```go
func (r *Raft) campaign() {
    r.becomeCandidate()

    // 单节点集群直接成为领导者
    if len(r.Prs) == 1 {
        r.becomeLeader()
        return
    }

    // 广播投票请求
    lastIndex := r.RaftLog.LastIndex()
    lastLogTerm, _ := r.RaftLog.Term(lastIndex)

    for peer := range r.Prs {
        if peer == r.id {
            continue
        }
        r.send(pb.Message{
            MsgType: pb.MessageType_MsgRequestVote,
            To:      peer,
            From:    r.id,
            Term:    r.Term,
            LogTerm: lastLogTerm,
            Index:   lastIndex,
        })
    }
}
```

**为什么发送最后的日志索引和任期**:
* 接收者使用这些信息判断候选者的日志是否足够新
* 只有日志至少和自己一样新的候选者才能获得投票

### handleRequestVote()

**作用**: 处理投票请求

**实现要点**:

```go
func (r *Raft) handleRequestVote(m pb.Message) {
    // 检查是否可以投票
    canVote := r.Vote == None || r.Vote == m.From

    // 检查候选者的日志是否至少和自己一样新
    lastIndex := r.RaftLog.LastIndex()
    lastLogTerm, _ := r.RaftLog.Term(lastIndex)

    logUpToDate := m.LogTerm > lastLogTerm ||
                   (m.LogTerm == lastLogTerm && m.Index >= lastIndex)

    grant := canVote && logUpToDate

    if grant {
        r.Vote = m.From
        r.electionElapsed = 0  // 重置选举超时
    }

    r.send(pb.Message{
        MsgType: pb.MessageType_MsgRequestVoteResponse,
        To:      m.From,
        From:    r.id,
        Term:    r.Term,
        Reject:  !grant,
    })
}
```

**为什么检查日志新旧**:
* 只有拥有所有已提交日志的节点才能成为领导者
* 这确保了领导者完整性（Leader Completeness）

**日志比较规则**:
1. 最后日志的任期更大 → 更新
2. 任期相同，索引更大或相等 → 更新

**为什么投票后重置选举超时**:
* 给予候选者成为领导者的时间
* 防止在选举期间发起新选举

### handleRequestVoteResponse()

**作用**: 处理投票响应

**实现要点**:

```go
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
    r.votes[m.From] = !m.Reject

    granted := 0
    rejected := 0

    for _, vote := range r.votes {
        if vote {
            granted++
        } else {
            rejected++
        }
    }

    quorum := len(r.Prs)/2 + 1

    if granted >= quorum {
        r.becomeLeader()
    } else if rejected >= quorum {
        r.becomeFollower(r.Term, None)
    }
}
```

**为什么需要多数投票（quorum）**:
* 确保同一任期内最多只有一个领导者
* 多数原则保证选举的安全性

**为什么拒绝票达到多数时成为跟随者**:
* 选举失败，等待超时后重新发起选举
* 避免浪费资源继续无效的选举

---

## 心跳相关函数

### bcastHeartbeat()

**作用**: 向所有跟随者广播心跳

**实现要点**:

```go
func (r *Raft) bcastHeartbeat() {
    for peer := range r.Prs {
        if peer == r.id {
            continue
        }
        r.sendHeartbeat(peer)
    }
}
```

**何时调用**:
* 收到 `MsgBeat` 消息（心跳超时）

### sendHeartbeat()

**作用**: 发送心跳消息给指定跟随者

**实现要点**:

```go
func (r *Raft) sendHeartbeat(to uint64) {
    commit := min(r.Prs[to].Match, r.RaftLog.committed)

    r.send(pb.Message{
        MsgType: pb.MessageType_MsgHeartbeat,
        To:      to,
        From:    r.id,
        Term:    r.Term,
        Commit:  commit,
    })
}
```

**为什么发送 Commit 索引**:
* 告诉跟随者可以提交哪些日志
* `Commit` 设置为 `min(Match, committed)` 确保不发送跟随者没有的日志的提交信息

### handleHeartbeat()

**作用**: 处理心跳消息

**实现要点**:

```go
func (r *Raft) handleHeartbeat(m pb.Message) {
    r.electionElapsed = 0
    r.Lead = m.From

    r.send(pb.Message{
        MsgType: pb.MessageType_MsgHeartbeatResponse,
        To:      m.From,
        From:    r.id,
        Term:    r.Term,
    })
}
```

**为什么重置选举超时**:
* 收到领导者的心跳，确认领导者存活
* 防止发起不必要的选举

**为什么不在这里更新 committed**:
* 心跳消息的 Commit 字段在 2A 中未使用
* 日志提交由 AppendEntries 处理

### handleHeartbeatResponse()

**作用**: 处理心跳响应

**实现要点**:

```go
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
    pr := r.Prs[m.From]

    // 如果跟随者落后了，发送 AppendEntries 来追赶
    if pr.Match < r.RaftLog.LastIndex() {
        r.sendAppend(m.From)
    }
}
```

**为什么检查 Match < LastIndex**:
* 心跳响应表明跟随者存活，但可能日志落后
* 主动发送 AppendEntries 帮助跟随者追赶

**为什么这样设计**:
* 2AA 测试要求使用 MsgHeartbeat 消息
* 2AB 测试需要日志追赶机制
* 通过心跳响应触发追赶，满足两个要求

---

## 日志复制函数

### handleMsgPropose()

**作用**: 处理客户端提议，追加新日志

**实现要点**:

```go
func (r *Raft) handleMsgPropose(m pb.Message) {
    if len(m.Entries) == 0 {
        return
    }

    // 1. 直接追加到 leader 的日志
    lastIndex := r.RaftLog.LastIndex()
    for i, entry := range m.Entries {
        entry.Index = lastIndex + uint64(i) + 1
        entry.Term = r.Term
        r.RaftLog.entries = append(r.RaftLog.entries, *entry)
    }

    // 2. 更新 leader 自己的进度
    r.Prs[r.id].Match = r.RaftLog.LastIndex()
    r.Prs[r.id].Next = r.Prs[r.id].Match + 1

    // 3. 单节点集群立即提交
    if len(r.Prs) == 1 {
        r.RaftLog.committed = r.RaftLog.LastIndex()
        return
    }

    // 4. 广播给跟随者
    r.bcastAppend()
}
```

**为什么直接操作 entries 数组**:
* 领导者追加新日志时不会有冲突（不需要 appendEntries 的冲突检测逻辑）
* 新日志总是追加在末尾，索引和任期都是新分配的
* 更简单高效，避免不必要的检查

**为什么设置 Term = r. Term**:
* 日志条目记录它被创建时的任期
* 用于日志一致性检查

**为什么广播追加日志**:
* 将新日志复制到所有跟随者
* 收集足够的复制确认后才能提交

### bcastAppend()

**作用**: 向所有跟随者广播追加日志

**实现要点**:

```go
func (r *Raft) bcastAppend() {
    for peer := range r.Prs {
        if peer == r.id {
            continue
        }
        r.sendAppend(peer)
    }
}
```

### sendAppend()

**作用**: 发送追加日志消息给指定跟随者

**实现要点**:

```go
func (r *Raft) sendAppend(to uint64) {
    pr := r.Prs[to]

    prevLogIndex := pr.Next - 1
    prevLogTerm, err := r.RaftLog.Term(prevLogIndex)

    if err != nil {
        // 需要发送快照（2C）
        return
    }

    // 获取要发送的日志
    entries := []*pb.Entry{}
    firstIndex := r.RaftLog.entries[0].Index
    lastIndex := r.RaftLog.LastIndex()

    for i := pr.Next; i <= lastIndex; i++ {
        offset := i - firstIndex
        entry := r.RaftLog.entries[offset]
        entries = append(entries, &entry)
    }

    r.send(pb.Message{
        MsgType: pb.MessageType_MsgAppend,
        To:      to,
        From:    r.id,
        Term:    r.Term,
        LogTerm: prevLogTerm,
        Index:   prevLogIndex,
        Entries: entries,
        Commit:  r.RaftLog.committed,
    })
}
```

**prevLogIndex 和 prevLogTerm 的作用**:
* 一致性检查：跟随者验证这个位置的日志是否匹配
* 如果不匹配，跟随者拒绝请求
* 这确保了日志的前缀一致性

**为什么发送从 Next 开始的所有日志**:
* 一次性发送所有缺失的日志，提高效率
* 如果跟随者拒绝，会递减 Next 并重试

### handleAppendEntries()

**作用**: 处理追加日志请求

**实现要点**:

```go
func (r *Raft) handleAppendEntries(m pb.Message) {
    // 重置选举超时并更新领导者
    r.electionElapsed = 0
    r.Lead = m.From

    // 检查 1: prevLogIndex 是否超出我们的日志范围
    lastIndex := r.RaftLog.LastIndex()
    if m.Index > lastIndex {
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

    // 检查 2: prevLogIndex 位置的任期是否匹配
    if m.Index > 0 {
        prevLogTerm, err := r.RaftLog.Term(m.Index)
        if err != nil || prevLogTerm != m.LogTerm {
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
    }

    // 追加日志
    if len(m.Entries) > 0 {
        entries := make([]pb.Entry, 0, len(m.Entries))
        for _, e := range m.Entries {
            entries = append(entries, *e)
        }
        r.RaftLog.appendEntries(entries)
    }

    // 更新 committed
    if m.Commit > r.RaftLog.committed {
        lastNewEntry := m.Index
        if len(m.Entries) > 0 {
            lastNewEntry = m.Entries[len(m.Entries)-1].Index
        }
        r.RaftLog.committed = min(m.Commit, lastNewEntry)
    }

    // 发送成功响应
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

**一致性检查的原理**:
1. **检查索引**: prevLogIndex 必须在跟随者的日志范围内
2. **检查任期**: prevLogIndex 位置的任期必须匹配

**为什么使用 min(m. Commit, lastNewEntry)**:
* `m.Commit` 是领导者的已提交索引
* `lastNewEntry` 是我们刚刚追加的最后一个日志
* 只能提交我们实际拥有的日志

**为什么在拒绝响应中发送 Index**:
* 帮助领导者更快地找到匹配点
* 可以实现快速回退优化

### handleAppendResponse()

**作用**: 处理追加日志响应

**实现要点**:

```go
func (r *Raft) handleAppendResponse(m pb.Message) {
    pr := r.Prs[m.From]

    if m.Reject {
        // 递减 Next 并重试
        pr.Next = max(1, pr.Next-1)
        r.sendAppend(m.From)
        return
    }

    // 更新进度
    if m.Index > pr.Match {
        pr.Match = m.Index
        pr.Next = pr.Match + 1

        // 尝试提交
        r.maybeCommit()
    }
}
```

**为什么拒绝时递减 Next**:
* 逐步回退找到匹配点
* 找到匹配点后，可以从那里开始追加

**为什么检查 m. Index > pr. Match**:
* 防止过时的响应导致进度倒退
* 只使用最新的响应更新进度

### maybeCommit()

**作用**: 尝试提交日志

**实现要点**:

```go
func (r *Raft) maybeCommit() {
    // 收集所有 Match 索引
    matches := make([]uint64, len(r.Prs))
	for i, pr := range r.Prs {
		matches[i] = pr.Match
	}

    // 排序并找到中位数
    sort.Slice(matches, func(i, j int) bool {
        return matches[i] > matches[j]
    })

    n := matches[len(matches)/2]

    // 提交条件：n > committed 且 term[n] == currentTerm
    if n > r.RaftLog.committed {
        logTerm, _ := r.RaftLog.Term(n)
        if logTerm == r.Term {
            oldCommitted := r.RaftLog.committed
            r.RaftLog.committed = n

            // 提交后广播，通知跟随者
            if r.RaftLog.committed > oldCommitted {
                r.bcastAppend()
            }
        }
    }
}
```

**为什么使用中位数（Median）**:
* 对于 N 个节点，中位数表示第 (N/2 + 1) 个节点
* 这正是多数节点（quorum）的定义
* 如果中位数是 N，说明至少有多数节点的 Match >= N

**为什么只提交当前任期的日志**:
* Raft 论文的安全性要求
* 防止提交不安全的旧日志
* 当当前任期的日志被提交时，之前任期的日志也会被间接提交

**为什么提交后广播**:
* 通知跟随者更新 committed 索引
* 跟随者可以应用这些日志到状态机

---

## 日志管理函数

### appendEntries()

**作用**: 追加日志到 RaftLog（在 log.go 中）

**实现要点**:

```go
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
            // 如果截断位置在 stable 范围内，需要更新 stabled
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
```

**为什么检查冲突**:
* 跟随者可能有与领导者冲突的日志（来自旧领导者）
* 冲突的日志必须被删除并替换为领导者的日志

**为什么更新 stabled**:
* 截断时可能删除了已持久化的日志
* `stabled` 必须反映实际持久化的位置
* 被截断的日志需要重新持久化

### unstableEntries()

**作用**: 返回未持久化的日志

**实现要点**:

```go
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
```

**何时使用**:
* 在 `Ready()` 中返回，告诉上层需要持久化哪些日志
* 持久化完成后，更新 `stabled`

**为什么返回 []pb. Entry{} 而不是 nil**:
* 测试使用 `reflect.DeepEqual` 比较
* `nil != []` 会导致测试失败

### nextEnts()

**作用**: 返回已提交但未应用的日志

**实现要点**:

```go
func (l *RaftLog) nextEnts() []pb.Entry {
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
```

**何时使用**:
* 在 `Ready()` 中返回，告诉上层需要应用哪些日志到状态机
* 应用完成后，更新 `applied`

**为什么是 (applied, committed] 区间**:
* `applied` 已经应用过了
* `committed` 是可以安全应用的最高索引

### LastIndex()

**作用**: 返回最后的日志索引

**实现要点**:

```go
func (l *RaftLog) LastIndex() uint64 {
    if len(l.entries) == 0 {
        lastIndex, _ := l.storage.LastIndex()
        return lastIndex
    }
    return l.entries[len(l.entries)-1].Index
}
```

**为什么检查 entries 是否为空**:
* 如果内存中没有日志，从存储中获取
* 这处理了快照之后的情况

### Term()

**作用**: 返回指定索引的日志任期

**实现要点**:

```go
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
```

**为什么需要访问 storage**:
* 旧日志可能已经被压缩（compacted）
* 压缩的日志在 storage 中，不在内存 entries 中

---

## 消息流程图

### 领导者选举流程

```
1. 跟随者超时 (tick())
   → 发送 MsgHup 给自己

2. 收到 MsgHup (stepFollower)
   → 调用 campaign()

3. campaign()
   → becomeCandidate()
   → 增加 Term, 投票给自己
   → 广播 MsgRequestVote

4. 其他节点收到 MsgRequestVote
   → handleRequestVote()
   → 检查投票条件
   → 返回 MsgRequestVoteResponse

5. 候选者收到 MsgRequestVoteResponse
   → handleRequestVoteResponse()
   → 统计投票
   → 如果获得多数票: becomeLeader()
   → 否则: 等待超时重新选举
```

### 日志复制流程

```
1. 客户端提议 (MsgPropose)
   → handleMsgPropose()
   → 直接追加到本地日志（操作 entries 数组）
   → 更新 Progress
   → bcastAppend() 广播给跟随者

2. 领导者发送 AppendEntries
   → sendAppend(to)
   → 包含 prevLogIndex, prevLogTerm, entries, commit

3. 跟随者收到 MsgAppend
   → handleAppendEntries()
   → 检查 prevLogIndex/prevLogTerm 一致性
   → 如果一致: appendEntries(), 更新 committed
   → 返回 MsgAppendResponse

4. 领导者收到 MsgAppendResponse
   → handleAppendResponse()
   → 如果成功: 更新 Match/Next, maybeCommit()
   → 如果失败: 递减 Next, 重试

5. maybeCommit()
   → 计算 Match 索引的中位数
   → 如果 median > committed 且 term 匹配: 提交
   → 广播 AppendEntries 通知跟随者
```

### 心跳流程

```
1. 领导者心跳超时 (tick())
   → 发送 MsgBeat 给自己

2. 收到 MsgBeat (stepLeader)
   → bcastHeartbeat()

3. 跟随者收到 MsgHeartbeat
   → handleHeartbeat()
   → 重置选举超时
   → 返回 MsgHeartbeatResponse

4. 领导者收到 MsgHeartbeatResponse
   → handleHeartbeatResponse()
   → 检查跟随者是否落后 (Match < LastIndex)
   → 如果落后: sendAppend() 帮助追赶
```

---

## 调试技巧

### 1. 添加日志输出

在关键函数中添加调试日志：

```go
func (r *Raft) handleAppendEntries(m pb.Message) {
    log.Infof("Node %d handling AppendEntries from %d, prevIdx=%d, prevTerm=%d, entries=%d",
        r.id, m.From, m.Index, m.LogTerm, len(m.Entries))

    // ... 实现

    log.Infof("Node %d AppendEntries result: reject=%v, lastIdx=%d",
        r.id, reject, r.RaftLog.LastIndex())
}
```

### 2. 检查不变量

在关键位置验证不变量：

```go
func (r *Raft) checkInvariants() {
    // applied <= committed <= stabled <= lastIndex
    assert(r.RaftLog.applied <= r.RaftLog.committed)
    assert(r.RaftLog.committed <= r.RaftLog.stabled)
    assert(r.RaftLog.stabled <= r.RaftLog.LastIndex())

    // Leader's Match[id] == LastIndex
    if r.State == StateLeader {
        assert(r.Prs[r.id].Match == r.RaftLog.LastIndex())
    }
}
```

### 3. 使用测试工具

运行特定测试：

```bash
# 运行单个测试
go test -run TestFollowerAppendEntries2AB

# 运行多次以发现竞态条件
go test -race -count=100 -run TestLeaderElection2AA
```

### 4. 理解测试失败信息

```
committed = 2, want 1
```

* `committed = 2`: 实际值
* `want 1`: 期望值
* 说明提交了不应该提交的日志

```
state = StateLeader, want StateFollower
```

* 节点成为了领导者，但应该是跟随者
* 检查选举逻辑和投票规则

### 5. 绘制状态图

当测试失败时，绘制节点的状态变化：

```
Time  Node1   Node2   Node3
  0   F(T=1)  F(T=1)  F(T=1)
  1   C(T=2)  F(T=1)  F(T=1)
  2   C(T=2)  F(T=2)  F(T=2)  [Node2,3 收到 RequestVote]
  3   L(T=2)  F(T=2)  F(T=2)  [Node1 获得多数票]
```

---

## 常见错误模式

### 1. 忘记重置选举超时

**错误**:

```go
func (r *Raft) handleHeartbeat(m pb.Message) {
    // 没有重置 electionElapsed
    r.Lead = m.From
}
```

**结果**: 跟随者即使收到心跳也会超时并发起选举

**修复**:

```go
func (r *Raft) handleHeartbeat(m pb.Message) {
    r.electionElapsed = 0  // 必须重置
    r.Lead = m.From
}
```

### 2. 未更新 stabled

**错误**:

```go
func (l *RaftLog) appendEntries(entries []pb.Entry) {
    if existingTerm != entry.Term {
        // 截断但未更新 stabled
        l.entries = append(l.entries[:offset], entries[i:]...)
    }
}
```

**结果**: `unstableEntries()` 返回错误的日志，导致持久化不正确

**修复**:

```go
if existingTerm != entry.Term {
    if idx <= l.stabled {
        l.stabled = idx - 1  // 必须更新
    }
    l.entries = append(l.entries[:offset], entries[i:]...)
}
```

### 3. 提交旧任期的日志

**错误**:

```go
func (r *Raft) maybeCommit() {
    n := matches[len(matches)/2]
    if n > r.RaftLog.committed {
        r.RaftLog.committed = n  // 未检查任期
    }
}
```

**结果**: 违反 Raft 安全性，可能提交未复制的日志

**修复**:

```go
if n > r.RaftLog.committed {
    logTerm, _ := r.RaftLog.Term(n)
    if logTerm == r.Term {  // 必须检查任期
        r.RaftLog.committed = n
    }
}
```

### 4. 未恢复持久化状态

**错误**:

```go
func newRaft(c *Config) *Raft {
    r := &Raft{
        Term: 0,   // 总是从 0 开始
        Vote: None,
    }
}
```

**结果**: 节点重启后忘记投票，可能在同一任期投票两次

**修复**:

```go
hardState, _, _ := c.Storage.InitialState()
if hardState.Term != 0 {
    r.Term = hardState.Term
}
if hardState.Vote != 0 {
    r.Vote = hardState.Vote
}
```

### 5. 在心跳中更新 committed

**错误**:

```go
func (r *Raft) handleHeartbeat(m pb.Message) {
    if m.Commit > r.RaftLog.committed {
        r.RaftLog.committed = m.Commit  // 未验证日志
    }
}
```

**结果**: 可能提交我们没有的日志

**修复**: 只在 `handleAppendEntries()` 中更新 committed，并且使用 `min(m.Commit, lastNewEntry)`

### 6. 返回 nil 而不是空切片

**错误**:

```go
func (l *RaftLog) unstableEntries() []pb.Entry {
    if l.stabled >= l.LastIndex() {
        return nil  // 测试失败
    }
}
```

**结果**: `reflect.DeepEqual(nil, [])` 返回 false，测试失败

**修复**:

```go
return []pb.Entry{}  // 返回空切片
```

### 7. 未处理单节点集群

**错误**:

```go
func (r *Raft) campaign() {
    r.becomeCandidate()
    // 未检查单节点情况
    for peer := range r.Prs {
        if peer == r.id {
            continue
        }
        r.sendRequestVote(peer)
    }
}
```

**结果**: 单节点集群永远不会选举出领导者（没有其他节点投票）

**修复**:

```go
func (r *Raft) campaign() {
    r.becomeCandidate()

    if len(r.Prs) == 1 {
        r.becomeLeader()  // 单节点直接成为领导者
        return
    }

    // ... 发送投票请求
}
```

### 8. Match/Next 初始化错误

**错误**:

```go
func (r *Raft) becomeLeader() {
    for peer := range r.Prs {
        r.Prs[peer].Match = 0
        r.Prs[peer].Next = 1  // 应该是 lastIndex + 1
    }
}
```

**结果**: 发送不必要的旧日志，效率低下

**修复**:

```go
lastIndex := r.RaftLog.LastIndex()
for peer := range r.Prs {
    r.Prs[peer].Match = 0
    r.Prs[peer].Next = lastIndex + 1  // 乐观假设
}
```

---

## 总结

### Project 2AA（领导者选举）关键点：

1. **选举超时随机化**：防止选票分裂
2. **投票规则**：每任期一票，日志至少和自己一样新
3. **心跳维持**：领导者定期发送心跳防止新选举
4. **任期单调性**：收到更高任期立即成为跟随者

### Project 2AB（日志复制）关键点：

1. **一致性检查**：prevLogIndex/prevLogTerm 必须匹配
2. **冲突解决**：发现冲突时截断并替换
3. **提交安全**：只提交当前任期的日志
4. **多数复制**：使用中位数判断多数节点
5. **持久化管理**：正确更新 stabled 索引

### 调试建议：

1. 添加详细的日志输出
2. 验证不变量
3. 绘制状态转换图
4. 理解测试的期望行为
5. 使用竞态检测器

### 测试通过标准：

* Project 2AA: 24/24 tests passing
* Project 2AB: 26/26 tests passing

祝你在 TinyKV 项目中取得成功！
