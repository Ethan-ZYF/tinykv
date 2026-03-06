# Project2 RaftKV 中文学习路线图

> 目标：实现基于 Raft 共识算法的高可用 KV 存储服务

---

## 总览

```
Project2 RaftKV
├── 前置准备
│   ├── 理论学习
│   └── 代码结构熟悉
├── Part A — 实现 Raft 核心算法
│   ├── 2AA: Leader 选举
│   ├── 2AB: 日志复制
│   └── 2AC: RawNode 接口
├── Part B — 构建 KV 服务层
│   ├── PeerStorage 实现
│   └── Raft Ready 处理流程
└── Part C — 日志压缩与快照
    ├── Raft 层快照处理
    └── Raftstore 层快照处理
```

---

## 第一阶段：前置准备

### 1.1 理论学习

```
理论资料（按优先级排序）
├── 必读
│   ├── Raft 论文（精读核心章节）
│   │   └── https://raft.github.io/raft.pdf
│   │       ├── §5   — Raft 基础（选举 + 日志复制）
│   │       ├── §6   — 集群成员变更（Part C 前读）
│   │       └── §7   — 日志压缩 / 快照（Part C 前读）
│   └── Raft 可视化动画（帮助直觉理解）
│       └── https://raft.github.io/
├── 推荐
│   ├── TiKV Raftstore 设计文档（英文）
│   │   └── https://pingcap.com/blog/design-and-implementation-of-multi-raft/
│   └── TiKV Raftstore 设计文档（中文）
│       └── https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/
└── 参考
    └── TiKV 快照源码解析
        └── https://pingcap.com/blog-cn/tikv-source-code-reading-10/
```

### 1.2 代码结构熟悉（必须在动手前通读）

```
源码阅读清单（按顺序）
├── proto 定义（理解数据结构）
│   ├── proto/proto/eraftpb.proto       ← 消息类型定义，所有 Message/Entry/Snapshot
│   ├── proto/proto/metapb.proto        ← Store / Peer / Region 概念定义
│   ├── proto/proto/raft_serverpb.proto ← RaftLocalState / RaftApplyState / RegionLocalState
│   └── proto/proto/raft_cmdpb.proto    ← RaftCmdRequest / RaftCmdResponse
│
├── Raft 模块（Part A 核心）
│   ├── raft/doc.go          ← ⭐ 必读！设计概览 + 消息类型说明
│   ├── raft/raft.go         ← 核心结构体骨架（需实现）
│   ├── raft/log.go          ← RaftLog 日志管理（需实现）
│   ├── raft/rawnode.go      ← RawNode 接口 + Ready 结构体（需实现）
│   └── raft/storage.go      ← Storage 接口定义（只读）
│
├── KV 服务层（Part B / C 核心）
│   ├── kv/raftstore/raftstore.go            ← Raftstore 入口，Workers 启动
│   ├── kv/raftstore/raft_worker.go          ← ⭐ RaftWorker 主循环
│   ├── kv/raftstore/peer_msg_handler.go     ← HandleMsg / HandleRaftReady（需实现）
│   ├── kv/raftstore/peer_storage.go         ← PeerStorage + SaveReadyState（需实现）
│   ├── kv/raftstore/peer.go                 ← Peer 结构体
│   └── kv/storage/raft_storage/raft_server.go ← RaftStorage（Storage 接口实现）
│
└── 工具 / 辅助
    ├── kv/raftstore/meta/               ← badger Key 格式工具函数
    ├── kv/raftstore/util/error.go       ← ErrNotLeader / ErrStaleCommand 等错误定义
    ├── kv/raftstore/cmd_resp.go         ← BindRespError 工具函数
    ├── kv/raftstore/message/msg.go      ← 消息类型定义
    └── kv/raftstore/runner/region_task.go ← Region Worker 任务处理（Part C）
```

---

## 第二阶段：Part A — Raft 核心算法

### 整体设计思路（先设计，再动手）

```
设计要点
├── 状态机角色转换
│   ├── Follower → Candidate（选举超时）
│   ├── Candidate → Leader（获得多数票）
│   ├── Candidate/Leader → Follower（发现更高 term）
│   └── 实现：becomeFollower / becomeCandidate / becomeLeader
│
├── 消息驱动模型（无阻塞）
│   ├── 不设置物理时钟，上层通过 Tick() 推进逻辑时钟
│   ├── 消息发送：push 到 raft.msgs，不直接发送
│   └── 消息接收：通过 Step() 统一入口处理
│
└── 日志索引体系
    ├── 理解 lastIndex / commitIndex / appliedIndex 三者关系
    ├── 理解 stabled（已持久化）vs unstable（内存中）
    └── 理解快照引入后的 dummyIndex offset 概念
```

### 2AA：Leader 选举

```
实现清单
├── 阅读
│   └── raft/doc.go 中 MsgHup / MsgRequestVote / MsgRequestVoteResponse 说明
│
├── 设计
│   ├── 选举超时随机化（不同节点应有不同超时，防止同时选举）
│   └── 投票规则：候选人日志至少与自己一样新才投票
│       ├── 比较最后一条日志的 term
│       └── term 相同时比较 index
│
├── 实现顺序
│   ├── 1. raft.Raft 结构体补充字段（votes / electionElapsed 等）
│   ├── 2. tick() → tickElection（follower/candidate 用）
│   ├── 3. becomeFollower() / becomeCandidate() / becomeLeader()
│   ├── 4. campaign()：转为 candidate，发送 MsgRequestVote 给所有 peers
│   ├── 5. Step() 处理 MsgRequestVote（判断是否投票，回复 MsgRequestVoteResponse）
│   └── 6. Step() 处理 MsgRequestVoteResponse（统计票数，过半则 becomeLeader）
│
└── 测试
    └── make project2aa
```

### 2AB：日志复制

```
实现清单
├── 阅读
│   ├── raft/doc.go 中 MsgAppend / MsgAppendResponse / MsgHeartbeat 说明
│   └── raft/log.go 骨架（理解 RaftLog 结构）
│
├── 设计
│   ├── Leader 维护每个 Follower 的 nextIndex / matchIndex
│   ├── AppendEntries 一致性检查：prevLogIndex + prevLogTerm 必须匹配
│   ├── 冲突处理：Follower 发现冲突时截断日志，返回 reject + hint
│   └── Commit 推进：当 matchIndex 过半时更新 commitIndex，广播新 commit
│
├── 实现顺序
│   ├── 1. RaftLog：entries / offset / stabled 字段，实现 Term() / LastIndex()
│   ├── 2. Leader tick() → tickHeartbeat，触发 MsgBeat → bcastHeartbeat
│   ├── 3. sendAppend()：构造 MsgAppend，处理找不到日志时发 MsgSnapshot
│   ├── 4. handleAppendEntries()：一致性检查，截断冲突，追加新条目，回复
│   ├── 5. handleAppendResponse()：更新 nextIndex/matchIndex，推进 commitIndex
│   ├── 6. Leader 新当选时追加一条 noop 日志（空 Entry）
│   └── 7. handleHeartbeat() / handleHeartbeatResponse()
│
└── 测试
    └── make project2ab
```

### 2AC：RawNode 接口

```
实现清单
├── 阅读
│   └── raft/rawnode.go 中 Ready 结构体定义
│
├── 设计
│   ├── Ready 是一次"快照"，包含需要处理的所有变更
│   │   ├── Messages：待发送消息
│   │   ├── Entries：需持久化的新日志
│   │   ├── HardState：需持久化的硬状态（term/vote/commit）
│   │   ├── CommittedEntries：需应用到状态机的日志
│   │   └── Snapshot：需应用的快照（Part C）
│   └── HasReady() 检查是否有需要处理的变更
│
├── 实现顺序
│   ├── 1. RawNode 结构体（包含 Raft，记录 prevHardState 等）
│   ├── 2. HasReady()：检查 msgs / unstable entries / hardState 变化 / committed 推进
│   ├── 3. Ready()：收集所有待处理变更打包返回
│   ├── 4. Advance()：上层处理完 Ready 后调用，更新 stabled / applied 索引
│   ├── 5. Propose()：封装为 MsgPropose 调用 Step
│   └── 6. Tick() / Step() 的包装
│
└── 测试
    └── make project2ac && make project2a
```

---

## 第三阶段：Part B — 构建 KV 服务层

### 整体设计思路

```
数据流向
Client RPC
  → RaftStorage.Write/Read
    → 封装为 RaftCmdRequest 发送到 raftCh
      → RaftWorker.HandleMsg（MsgTypeRaftCmd）
        → peer.proposeRaftCommand（提案到 Raft 日志）
          → Raft 共识提交
            → HandleRaftReady → 应用已提交日志
              → 执行 Get/Put/Delete 到 badger
                → 通过 callback 返回响应
                  → RaftStorage 收到响应 → 返回给 Client
```

### 3.1 PeerStorage 实现

```
实现清单
├── 阅读
│   ├── kv/raftstore/peer_storage.go（重点看初始化逻辑）
│   ├── kv/raftstore/meta/ 工具函数（key 格式）
│   └── proto/proto/raft_serverpb.proto（三种状态定义）
│
├── 理解存储布局
│   ├── raftdb（raft 专用）
│   │   ├── raft_log_key   → Entry（日志条目）
│   │   └── raft_state_key → RaftLocalState（HardState + lastIndex）
│   └── kvdb（状态机 + 元数据）
│       ├── apply_state_key  → RaftApplyState（appliedIndex + truncatedState）
│       └── region_state_key → RegionLocalState（Region 信息 + Peer 状态）
│
├── 实现：SaveReadyState()
│   ├── 1. 处理 Snapshot（Part C，先跳过）
│   ├── 2. Append Entries：写入 raftdb，删除被覆盖的旧日志
│   ├── 3. 更新 RaftLocalState.LastIndex
│   └── 4. 保存 HardState 到 RaftLocalState（如果有变化）
│   └── 5. 用 WriteBatch 原子写入所有变更
│
└── 注意
    ├── 初始值：RAFT_INIT_LOG_TERM = RAFT_INIT_LOG_INDEX = 5（非 0）
    └── WriteBatch.SetMeta() 写元数据，WriteBatch.SetCF() 写 KV 数据
```

### 3.2 Raft Ready 处理流程

```
实现清单
├── 阅读
│   ├── kv/raftstore/raft_worker.go（主循环，理解调用顺序）
│   └── kv/raftstore/peer_msg_handler.go（骨架代码）
│
├── 实现：proposeRaftCommand()
│   ├── 1. 检查请求头（Region ID / peer ID / term 是否匹配）
│   ├── 2. 根据请求类型做基本检查
│   ├── 3. 序列化 RaftCmdRequest 为 []byte
│   ├── 4. 调用 RawNode.Propose() 提案
│   └── 5. 保存 callback（通过 index 或 proposal 队列关联）
│
├── 实现：HandleRaftReady()
│   ├── 1. 调用 HasReady() 检查是否有变更
│   ├── 2. 调用 SaveReadyState() 持久化
│   ├── 3. 通过 Transport 发送 rd.Messages
│   ├── 4. 遍历 rd.CommittedEntries 应用到状态机
│   │   ├── 普通命令（Get/Put/Delete/Snap）→ applyRequest()
│   │   └── 管理命令（CompactLog）→ Part C 实现
│   ├── 5. 更新并持久化 RaftApplyState.AppliedIndex
│   └── 6. 调用 RawNode.Advance()
│
├── 错误处理
│   ├── ErrNotLeader：收到非 leader 的提案请求时返回
│   └── ErrStaleCommand：leader 切换导致日志被覆盖时返回
│
└── 测试
    └── make project2b
```

---

## 第四阶段：Part C — 日志压缩与快照

### 整体设计思路

```
快照触发链路
定时检查日志数量（onRaftGcLogTick）
  → 超过阈值时提案 CompactLogRequest（admin cmd）
    → Raft 提交该命令
      → 应用时更新 RaftTruncatedState
        → 调度 ScheduleCompactLog（异步删除旧日志）
          → 后续 sendAppend 发现 nextIndex 已被截断
            → 调用 Storage.Snapshot() 生成快照
              → 发送 MsgSnapshot 给落后的 Follower
                → Follower handleSnapshot() 恢复状态
                  → 下次 Ready 包含 Snapshot
                    → applySnapshot() 更新所有状态
```

### 4.1 Raft 层快照处理

```
实现清单
├── 阅读
│   └── eraftpb.Snapshot 定义（data 字段是元数据，非真实数据）
│
├── 实现
│   ├── sendAppend 中：获取 term/entries 失败时调用 Storage.Snapshot()
│   │   └── 成功则发送 MsgSnapshot
│   ├── handleSnapshot()：从 SnapshotMetadata 恢复 term/commit/membership
│   │   ├── 更新 RaftLog 的 dummyIndex（offset）
│   │   └── 重置日志条目（仅保留快照元数据）
│   └── RaftLog.maybeCompact()：配合 Storage 截断内存中的日志
│
└── 注意
    └── Storage.Snapshot() 可能返回 ErrSnapshotTemporarilyUnavailable
        → 快照还在异步生成中，本次跳过，下次重试
```

### 4.2 Raftstore 层快照处理

```
实现清单
├── 阅读
│   ├── kv/raftstore/runner/region_task.go（RegionTaskGen / RegionTaskApply）
│   └── kv/storage/raft_storage/snap_runner.go（快照收发，了解即可）
│
├── 实现 CompactLog 管理命令处理
│   ├── HandleRaftReady 中识别 EntryType_EntryConfChange 和管理命令
│   ├── 应用 CompactLogRequest：更新 RaftApplyState.TruncatedState
│   └── 调用 ScheduleCompactLog 异步删除
│
├── 实现 PeerStorage.SaveReadyState() 中的快照处理
│   ├── 检测 Ready.Snapshot 是否非空
│   ├── 更新内存状态：RaftLocalState / RaftApplyState / RegionLocalState
│   ├── 原子写入 kvdb 和 raftdb（删除旧数据，写入新元数据）
│   ├── 设置 snapState = SnapState_Applying
│   └── 发送 RegionTaskApply 到 regionSched，等待完成
│
└── 测试
    └── make project2c
```

---

## 关键概念速查

### 三个 Index 的关系

```
日志 Index 示意
[snapshot] [5][6][7][8][9][10]
            ↑           ↑   ↑
     truncatedIndex  committed  lastIndex
                         ↑
                      applied（≤ committed）
                              ↑
                           stabled（持久化边界）
```

### 三种持久化状态

| 状态 | 存储位置 | 内容 |
|------|----------|------|
| RaftLocalState | raftdb | HardState (term/vote/commit) + LastIndex |
| RaftApplyState | kvdb | AppliedIndex + TruncatedState |
| RegionLocalState | kvdb | Region 元信息 + Peer 状态 |

### 角色与消息对应表

| 角色 | 发出消息 | 处理消息 |
|------|----------|----------|
| Follower | MsgRequestVoteResponse, MsgAppendResponse | MsgRequestVote, MsgAppend, MsgHeartbeat |
| Candidate | MsgRequestVote | MsgRequestVoteResponse |
| Leader | MsgAppend, MsgHeartbeat, MsgSnapshot | MsgAppendResponse, MsgHeartbeatResponse |

---

## 调试建议

```
调试工具
├── 日志级别：LOG_LEVEL=debug make project2b
├── 单元测试：make project2aa / project2ab / project2ac
├── 集成测试：make project2b / project2c
└── 注意：2A 之后某些测试需多次运行才能暴露 bug（随机超时相关）

常见 Bug 类型
├── Index off-by-one（日志 index 从 1 开始，注意 dummyEntry）
├── Term 比较遗漏（收到旧 term 消息需忽略或更新自身）
├── callback 泄漏（提案未提交时需清理 pending callback）
├── WriteBatch 未原子提交（apply + 更新 applyIndex 必须在同一个 batch）
└── Snapshot 生成异步（ErrSnapshotTemporarilyUnavailable 需正确处理）
```

---

## 里程碑检查点

- [ ] `make project2aa` 全绿 — Leader 选举正常
- [ ] `make project2ab` 全绿 — 日志复制正常
- [ ] `make project2ac` 全绿 — RawNode 接口正常
- [ ] `make project2a`  全绿 — Part A 完整通过
- [ ] `make project2b`  全绿 — KV 服务层正常
- [ ] `make project2c`  全绿 — 快照机制正常
