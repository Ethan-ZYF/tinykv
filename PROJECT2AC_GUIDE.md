# Project 2AC 实现指南：RawNode Interface

## 概述

Project 2AC 要实现 `RawNode` 接口，这是 Raft 核心算法与上层应用之间的桥梁。

### 架构图

```
┌─────────────────────────────────────────┐
│        上层应用 (raftstore)              │
│                                         │
│  for {                                  │
│    RawNode.Tick()      ← 推进时钟       │
│    if HasReady() {                      │
│      rd := Ready()     ← 获取更新       │
│      处理 rd (持久化/发送/应用)          │
│      Advance(rd)       ← 确认完成       │
│    }                                    │
│  }                                      │
└─────────────────────────────────────────┘
                 ↕
┌─────────────────────────────────────────┐
│          RawNode (rawnode.go)           │
│                                         │
│  - HasReady(): 是否有更新？             │
│  - Ready(): 获取待处理的更新             │
│  - Advance(): 确认更新已处理             │
│  - Propose(): 提议新日志                │
│  - Step(): 处理消息                     │
│  - Tick(): 推进时钟                     │
└─────────────────────────────────────────┘
                 ↕
┌─────────────────────────────────────────┐
│           Raft (raft.go)                │
│                                         │
│  你已经实现的核心 Raft 算法：             │
│  - Leader Election                      │
│  - Log Replication                      │
│  - Message Handling                     │
└─────────────────────────────────────────┘
```

## 需要实现的三个核心函数

### 1. HasReady() - 检查是否有待处理的更新

**作用**：快速检查是否有任何更新需要上层应用处理。

**检查项**：
- ✅ SoftState 是否变化？（Lead 或 State 改变）
- ✅ HardState 是否变化？（Term、Vote、Commit 改变）
- ✅ 是否有未持久化的日志？（unstableEntries）
- ✅ 是否有待应用的已提交日志？（nextEnts）
- ✅ 是否有待发送的消息？（msgs）
- ✅ 是否有快照？（pendingSnapshot）

**实现思路**：

```go
func (rn *RawNode) HasReady() bool {
    r := rn.Raft

    // 检查 SoftState 是否改变
    if rn.prevSoftState.Lead != r.Lead || rn.prevSoftState.RaftState != r.State {
        return true
    }

    // 检查 HardState 是否改变
    if !isHardStateEqual(rn.prevHardState, r.hardState()) {
        return true
    }

    // 检查是否有未持久化日志
    if len(r.RaftLog.unstableEntries()) > 0 {
        return true
    }

    // 检查是否有待应用日志
    if len(r.RaftLog.nextEnts()) > 0 {
        return true
    }

    // 检查是否有待发送消息
    if len(r.msgs) > 0 {
        return true
    }

    // 检查是否有快照（2C 才需要）
    if r.RaftLog.pendingSnapshot != nil {
        return true
    }

    return false
}
```

### 2. Ready() - 获取待处理的更新

**作用**：封装所有需要上层应用处理的更新，返回 `Ready` 结构体。

**Ready 结构体包含**：

```go
type Ready struct {
    *SoftState        // 易失性状态（Lead, RaftState）
    pb.HardState      // 持久化状态（Term, Vote, Commit）
    Entries          // 未持久化的日志（需要写入磁盘）
    Snapshot         // 快照（2C）
    CommittedEntries // 已提交但未应用的日志（需要应用到状态机）
    Messages         // 待发送的消息
}
```

**实现思路**：

```go
func (rn *RawNode) Ready() Ready {
    r := rn.Raft
    rd := Ready{
        Messages: r.msgs,
    }

    // 1. SoftState（如果改变）
    softState := &SoftState{
        Lead:      r.Lead,
        RaftState: r.State,
    }
    if !isSoftStateEqual(rn.prevSoftState, *softState) {
        rd.SoftState = softState
    }

    // 2. HardState（如果改变）
    hardState := r.hardState()
    if !isHardStateEqual(rn.prevHardState, hardState) {
        rd.HardState = hardState
    }

    // 3. 未持久化的日志（Entries）
    rd.Entries = r.RaftLog.unstableEntries()

    // 4. 已提交但未应用的日志（CommittedEntries）
    rd.CommittedEntries = r.RaftLog.nextEnts()

    // 5. 快照（2C 才需要）
    if r.RaftLog.pendingSnapshot != nil {
        rd.Snapshot = *r.RaftLog.pendingSnapshot
    }

    return rd
}
```

### 3. Advance() - 确认更新已处理

**作用**：上层应用处理完 Ready 后，通知 RawNode 更新内部状态。

**需要更新的状态**：
- ✅ 保存当前的 SoftState 和 HardState（用于下次比较）
- ✅ 更新 `applied` 索引（已应用的最高日志）
- ✅ 更新 `stabled` 索引（已持久化的最高日志）
- ✅ 清空消息队列（msgs）

**实现思路**：

```go
func (rn *RawNode) Advance(rd Ready) {
    r := rn.Raft

    // 1. 更新 prevSoftState
    if rd.SoftState != nil {
        rn.prevSoftState = *rd.SoftState
    }

    // 2. 更新 prevHardState
    if !IsEmptyHardState(rd.HardState) {
        rn.prevHardState = rd.HardState
    }

    // 3. 更新 applied 索引
    if len(rd.CommittedEntries) > 0 {
        lastApplied := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
        r.RaftLog.applied = lastApplied
    }

    // 4. 更新 stabled 索引
    if len(rd.Entries) > 0 {
        lastStabled := rd.Entries[len(rd.Entries)-1].Index
        r.RaftLog.stabled = lastStabled
    }

    // 5. 清空消息队列
    r.msgs = nil

    // 6. 清空 pendingSnapshot（2C 才需要）
    if !IsEmptySnap(&rd.Snapshot) {
        r.RaftLog.pendingSnapshot = nil
    }
}
```

## RawNode 需要的额外状态

在 `RawNode` 结构体中添加：

```go
type RawNode struct {
    Raft *Raft

    // 用于检测状态变化
    prevSoftState SoftState
    prevHardState pb.HardState
}
```

在 `NewRawNode` 中初始化：

```go
func NewRawNode(config *Config) (*RawNode, error) {
    r := newRaft(config)
    rn := &RawNode{
        Raft: r,
        prevSoftState: SoftState{
            Lead:      r.Lead,
            RaftState: r.State,
        },
        prevHardState: r.hardState(),
    }
    return rn, nil
}
```

## Raft 需要的辅助函数

在 `raft.go` 中添加：

```go
// hardState 返回当前的 HardState
func (r *Raft) hardState() pb.HardState {
    return pb.HardState{
        Term:   r.Term,
        Vote:   r.Vote,
        Commit: r.RaftLog.committed,
    }
}
```

## 辅助函数

```go
// isHardStateEqual 检查两个 HardState 是否相等
func isHardStateEqual(a, b pb.HardState) bool {
    return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// isSoftStateEqual 检查两个 SoftState 是否相等
func isSoftStateEqual(a, b SoftState) bool {
    return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// IsEmptyHardState 检查 HardState 是否为空
func IsEmptyHardState(st pb.HardState) bool {
    return isHardStateEqual(st, pb.HardState{})
}

// IsEmptySnap 检查 Snapshot 是否为空
func IsEmptySnap(sp *pb.Snapshot) bool {
    return sp == nil || IsEmptySnap(sp.Metadata)
}
```

## 使用流程示例

```go
// 上层应用的主循环
func (ps *peerStorage) HandleLoop() {
    for {
        select {
        case <-ticker.C:
            // 推进逻辑时钟
            rawNode.Tick()

        case msg := <-msgCh:
            // 处理接收的消息
            rawNode.Step(msg)
        }

        // 检查是否有更新
        if rawNode.HasReady() {
            rd := rawNode.Ready()

            // 1. 持久化 HardState 和 Entries
            if !IsEmptyHardState(rd.HardState) {
                storage.SaveHardState(rd.HardState)
            }
            if len(rd.Entries) > 0 {
                storage.Append(rd.Entries)
            }

            // 2. 发送消息给其他节点
            for _, msg := range rd.Messages {
                transport.Send(msg)
            }

            // 3. 应用已提交的日志到状态机
            for _, entry := range rd.CommittedEntries {
                stateMachine.Apply(entry)
            }

            // 4. 确认处理完成
            rawNode.Advance(rd)
        }
    }
}
```

## 完整的处理顺序（重要！）

**必须按照以下顺序处理 Ready**：

```
1. 持久化 HardState 和 Entries（写入磁盘）
   ↓
2. 发送 Messages 给其他节点
   ↓
3. 应用 CommittedEntries 到状态机
   ↓
4. 调用 Advance() 更新状态
```

**为什么这个顺序很重要？**

- **先持久化再发送**：确保发送的日志已经安全保存
- **先发送再应用**：并行性，可以同时发送和应用
- **最后 Advance**：确保所有操作完成后才更新状态

## 常见错误

### 1. 忘记初始化 prevSoftState 和 prevHardState

```go
// ❌ 错误
func NewRawNode(config *Config) (*RawNode, error) {
    r := newRaft(config)
    return &RawNode{Raft: r}, nil  // 没有初始化 prev 状态
}

// ✅ 正确
func NewRawNode(config *Config) (*RawNode, error) {
    r := newRaft(config)
    rn := &RawNode{
        Raft: r,
        prevSoftState: SoftState{Lead: r.Lead, RaftState: r.State},
        prevHardState: r.hardState(),
    }
    return rn, nil
}
```

### 2. HasReady() 检查不完整

```go
// ❌ 错误：只检查消息
func (rn *RawNode) HasReady() bool {
    return len(rn.Raft.msgs) > 0
}

// ✅ 正确：检查所有更新
func (rn *RawNode) HasReady() bool {
    return 状态变化 || 有日志 || 有消息 || 有快照
}
```

### 3. Advance() 不更新 applied 和 stabled

```go
// ❌ 错误：忘记更新索引
func (rn *RawNode) Advance(rd Ready) {
    rn.Raft.msgs = nil  // 只清空消息
}

// ✅ 正确：更新所有状态
func (rn *RawNode) Advance(rd Ready) {
    更新 prevState + 更新 applied + 更新 stabled + 清空 msgs
}
```

### 4. Ready() 返回可变引用

```go
// ❌ 错误：返回指向内部状态的指针
func (rn *RawNode) Ready() Ready {
    return Ready{
        Entries: rn.Raft.RaftLog.entries,  // 直接返回内部切片！
    }
}

// ✅ 正确：返回副本
func (rn *RawNode) Ready() Ready {
    entries := rn.Raft.RaftLog.unstableEntries()  // 返回新切片
    return Ready{Entries: entries}
}
```

## 测试

运行测试：

```bash
# 只测试 2AC
make project2ac

# 测试整个 Part A（2AA + 2AB + 2AC）
make project2a
```

## 调试技巧

### 1. 打印 Ready 内容

```go
func (rn *RawNode) Ready() Ready {
    rd := Ready{...}

    log.Debugf("Ready: SoftState=%v, HardState=%v, Entries=%d, CommittedEntries=%d, Messages=%d",
        rd.SoftState, rd.HardState, len(rd.Entries), len(rd.CommittedEntries), len(rd.Messages))

    return rd
}
```

### 2. 验证不变量

```go
func (rn *RawNode) Advance(rd Ready) {
    // 验证 applied <= committed
    if rn.Raft.RaftLog.applied > rn.Raft.RaftLog.committed {
        panic("applied > committed")
    }

    // 验证 committed <= stabled
    if rn.Raft.RaftLog.committed > rn.Raft.RaftLog.stabled {
        panic("committed > stabled")
    }
}
```

### 3. 检查 Ready 是否真的为空

```go
rd := rawNode.Ready()
if !IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
    t.Fatalf("expected empty ready, got: %#v", rd)
}
```

## 总结

### 核心概念

1. **Ready 是快照**：封装了某个时间点 Raft 的所有更新
2. **HasReady 是优化**：避免不必要的 Ready() 调用
3. **Advance 是确认**：告诉 Raft "我处理完了，可以更新状态了"

### 实现清单

- [x] 在 `RawNode` 添加 `prevSoftState` 和 `prevHardState` 字段
- [x] 在 `NewRawNode` 初始化这些字段
- [x] 在 `Raft` 添加 `hardState()` 辅助函数
- [x] 实现 `HasReady()`：检查所有可能的更新
- [x] 实现 `Ready()`：封装所有更新到 Ready 结构体
- [x] 实现 `Advance()`：更新 prevState、applied、stabled、清空 msgs
- [ ] 实现辅助函数：`isHardStateEqual`、`isSoftStateEqual`
- [ ] 运行 `make project2ac` 测试

祝你顺利完成 Project 2AC！
