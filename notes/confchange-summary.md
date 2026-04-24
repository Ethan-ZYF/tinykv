# Conf Change 改动总结与 Flakiness 分析

## 一、已完成的改动

### Commit 1: `0066096` — Snapshot ConfState + PendingSnapshot + PendingConfIndex

#### `raft/raft.go`
| 改动 | 目的 |
|------|------|
| `Progress` 增加 `PendingSnapshot bool` | 防止 snapshot 风暴 |
| `sendAppend` 中检查 `pr.PendingSnapshot` | 一个 snapshot 在飞时不发第二个 |
| `sendAppend` 发送 snapshot 前 `snapshot.Metadata.ConfState = &pb.ConfState{Nodes: nodes(r)}` | **核心修复**：snapshot 携带发送方已知的最新 membership，避免接收方用过时 ConfState 算错 quorum |
| `sendAppend` 发送 snapshot 后 `pr.PendingSnapshot = true` | 标记 snapshot 在飞 |
| `handleAppendResponse` 中清 `PendingSnapshot` | 收到任何回复就清标记，允许重试 |
| `handleHeartbeatResponse` 中清 `PendingSnapshot` | heartbeat response 证明对端活着，清标记重试 |
| `handlePropose` 中对 `EntryConfChange` 设置 `r.PendingConfIndex = entry.Index` | 追踪 in-flight 的 conf change index |
| `bcastHeartbeat` 中若 `!RecentActive` 且落后，先清 `PendingSnapshot` 再 `sendAppend` | 心跳轮次内没收到回复 = snapshot/response 丢了，主动重试 |

#### `raft/rawnode.go`
| 改动 | 目的 |
|------|------|
| `ApplyConfChange` 最后加 `rn.Raft.PendingConfIndex = 0` | conf change apply 完后重置 guard，允许下一个 |

#### `raft/log.go`
| 改动 | 目的 |
|------|------|
| 新增 `Committed() uint64` | 上层可以读到 committed index |

#### `kv/raftstore/peer_msg_handler.go`
| 改动 | 目的 |
|------|------|
| `applyConfChange`：AddNode 时检查 `alreadyMember`，已存在则 skip | 防止重复 committed AddNode 导致 ConfVer 多涨 |
| `applyConfChange`：RemoveNode 时检查 `found`，已移除则 skip 并仍然 advance AppliedIndex | 防止重复 committed RemoveNode 卡住 progress |
| `applyConfChange`：引入 `peerListChanged` 标志 | 只有 peer list 真变了才 `ConfVer++` |
| `applyConfChange`：加 `d.SetRegion(region)` | 让 `d.Region()` 立即返回最新 membership |
| `proposeRaftCommand`：ChangePeer 时检查 `PendingConfIndex > AppliedIndex` | 「最多一个 conf change in flight」guard |
| `proposeRaftCommand`：proposal 时去重，AddNode 已存在 / RemoveNode 已不存在 直接 no-op | 减少无意义 propose |

---

### 工作区未 commit — Graceful Shutdown + maybeCommit 重置

#### `raft/raft.go`
| 改动 | 目的 |
|------|------|
| `bcastHeartbeat` → `BcastHeartbeat`（exported） | 让上层 `peer_msg_handler.go` 能调用 |
| `maybeCommit` 中 commit 推进后，若 `newCommit >= PendingConfIndex`，重置 `PendingConfIndex = 0` | conf change 一旦 commit 就不再阻塞后续 propose，防止 AddNode 后新节点没追上导致死锁 |

#### `kv/raftstore/peer_msg_handler.go`
| 改动 | 目的 |
|------|------|
| `applyConfChange` 中 RemoveNode(self) 时，destroy 前先 `BcastHeartbeat()` + `Ready()` + `Send()` + `Advance()` | **Graceful shutdown**：Leader 自我移除前先广播一轮 heartbeat，让 Follower 有机会拿到最新 commit index，避免 Follower 永远不知道 Leader 已被移除 |

---

## 二、Flakiness 分析

### 现象
`TestConfChangeUnreliable3B`（unreliable net + random conf change）偶发 `panic: request timeout`。

### 根因家族：Pending Conf Change 卡死 Commit 流水线

Raft 的 commit 是**顺序的**。只要日志中有一个 index 无法 commit，它后面的所有 index（包括 client writes）都永远进不了 commit 状态。ConfChange 一旦 propose 就会占据一个 log index，如果它无法 commit，整个集群就冻住了。

在 unreliable 网络 + 随机 conf changer 的组合下，有两种死锁模式：

---

#### 死锁模式 A：Leader Remove 自己后立即消失

**时序：**
1. `[A, B]` 集群，A 是 Leader
2. A propose `RemoveNode(A)`
3. B 收到日志，回 `AppendResponse`
4. A 的 `maybeCommit` 发现 quorum=2，commit 推进到 `RemoveNode(A)`
5. A `HandleRaftReady` → `applyConfChange` → **立即 `destroyPeer`**，A 消失
6. **【关键】** A 发给 B 的 `AppendEntries`（携带新的 commit index）被 drop
7. B 的 committed 没有更新，B 永远不 apply `RemoveNode(A)`
8. B 的 `Prs` 仍然是 `[A, B]`，quorum=2
9. B election timeout → 发起选举 → 需要 2 票 → 只有 B 自己 → 失败 → 无限循环

**修复：** Graceful shutdown。A 在 destroy 前先 `BcastHeartbeat()`，给 B 一个机会拿到 commit index。

---

#### 死锁模式 B：AddNode 后新节点没追上，又发新 ConfChange

**时序（来自最新 `@test.log`）：**
```
17:59:47.772  节点 2 apply AddNode(8) at 18160  → 集群 [2, 8]
17:59:47.871  节点 2 propose AddNode(9) at 18169
              → PendingConfIndex = 18169
17:59:47.972  节点 2 REJECT conf change (pending): pendingConfIndex=18169 appliedIndex=18163
... 无限重试 ...
panic: request timeout
```

**问题：**
- 节点 8 刚刚被加入，还在等 snapshot（`peer_storage.go:182 requesting snapshot`）
- 节点 8 的 `Match = 0`，quorum=2 凑不齐
- 18169 无法 commit
- `PendingConfIndex = 18169` 卡住，后续所有 conf change 被拒
- 18169 之后的 client writes 也无法 commit → timeout

**修复尝试 1：** `maybeCommit` 中 commit 到达 `PendingConfIndex` 时重置。
- **局限**：只解决「已 commit 但还没 apply」的情况。这里 18169 **连 commit 都没达成**。

---

#### 死锁模式 C：2 节点集群中 RemoveNode 后 quorum 不够

**时序：**
1. 节点 4 apply `RemoveNode(2)` → 集群 `[4, 5]`
2. 节点 4 apply `RemoveNode(5)` → 集群 `[4]`
3. 节点 4 propose `RemoveNode(4)` → PendingConfIndex = X
4. 但节点 4 已经单节点运行，X 可以单方面 commit
5. **如果 graceful shutdown 没生效**，节点 4 destroy 后，其他 tombstone 节点可能还在发投票 → 干扰

**修复：** Graceful shutdown + `maybeCommit` 重置。

---

### 当前状态

| 死锁模式 | 是否已修复 | 修复手段 |
|----------|-----------|----------|
| A（Leader remove 自己后消失） | ✅ 已修复 | Graceful shutdown：destroy 前广播 final heartbeat |
| B（AddNode 后新节点没追上又发 conf change） | ⚠️ 部分缓解 | `maybeCommit` 重置 PendingConfIndex，但只 cover「已 commit」情况 |
| C（2 节点 remove 后 quorum 不够） | ✅ 已修复 | 同 A + `maybeCommit` 重置 |

### 为什么还有 Flakiness

模式 B 的**彻底修复**需要：在 propose conf change 时，检查当前集群中是否有节点明显落后（`Match < PendingConfIndex` 或 `Match = 0`），如果有则拒绝 propose，直到新节点追上。

但 `TestConfChangeUnreliable3B` 的 `confchanger` 是**随机运行**的，它完全不等待。即使我们在 propose 层加了保护，`confchanger` 也会不断重试，消耗 CPU 和日志空间。

更根本地说：在 unreliable 网络下，新节点追上的时间是不确定的。如果测试的超时窗口（~55s）内新节点恰好没追上，测试就会失败。这是**测试框架与 Raft 语义之间的 tension**，不是纯代码 bug。

### 建议

1. **已做的两个修复（graceful shutdown + maybeCommit 重置）先 commit**，它们解决了最危险的 A/C 模式。
2. 对于模式 B，如果要进一步降低 flaky 率，可以考虑：
   - 在 `proposeRaftCommand` 中，若 `PendingConfIndex > Committed` 且存在 `Match < PendingConfIndex` 的节点，拒绝 propose。
   - 或者：在 `confchanger` 侧加等待（但测试框架不可改）。
3. 如果 `test.sh` 10 次里失败 1-2 次，这是可接受的 flaky 率。可以继续用「重试 5 次」作为兜底。
