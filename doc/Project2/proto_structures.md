# TinyKV Proto 数据结构总结

## 一、eraftpb.proto — Raft 协议核心数据结构

这个文件定义了 Raft 算法本身需要的所有数据结构，与上层业务无关。

### Entry（日志条目）
| 字段 | 说明 |
|------|------|
| `entry_type` | `EntryNormal`（普通数据变更）或 `EntryConfChange`（配置变更） |
| `term` | 该条目所属的任期 |
| `index` | 该条目在 Raft log 中的位置索引 |
| `data` | 载荷。普通条目存业务数据，配置变更条目存序列化的 `ConfChange` |

### Message（Raft 消息）
节点之间（以及节点内部）传递的所有消息的统一结构。

| 字段 | 说明 |
|------|------|
| `msg_type` | 消息类型（见下方枚举） |
| `to` / `from` | 目标/源节点 ID |
| `term` | 发送方当前任期 |
| `log_term` | 与 `index` 配合使用，表示特定 log 位置的 term（用于 AppendEntries 和投票） |
| `index` | 语义取决于消息类型，如 prevLogIndex（Append）或 lastLogIndex（Vote） |
| `entries` | 日志条目列表（仅 MsgAppend / MsgPropose 使用） |
| `commit` | Leader 的 commit index |
| `snapshot` | 快照数据（仅 MsgSnapshot 使用） |
| `reject` | 拒绝标志（用于响应消息） |

#### MessageType 枚举

| 类型 | 方向 | 说明 |
|------|------|------|
| `MsgHup` | 本地 | 触发选举（election timeout 到期时调用） |
| `MsgBeat` | 本地 | 触发 Leader 发送心跳 |
| `MsgPropose` | 本地 | 客户端提案，请求 Leader 追加日志 |
| `MsgAppend` | 网络 | Leader → Follower，复制日志 |
| `MsgAppendResponse` | 网络 | Follower → Leader，日志复制响应 |
| `MsgRequestVote` | 网络 | Candidate → All，请求投票 |
| `MsgRequestVoteResponse` | 网络 | All → Candidate，投票响应 |
| `MsgSnapshot` | 网络 | Leader → Follower，安装快照 |
| `MsgHeartbeat` | 网络 | Leader → Follower，心跳 |
| `MsgHeartbeatResponse` | 网络 | Follower → Leader，心跳响应 |
| `MsgTransferLeader` | 本地 | 请求 Leader 转让领导权 |
| `MsgTimeoutNow` | 网络 | Leader → 转让目标，立即超时发起选举 |

### HardState（需持久化的状态）
必须落盘的 Raft 状态，重启后恢复。

| 字段 | 说明 |
|------|------|
| `term` | 当前任期 |
| `vote` | 本任期投票给了谁 |
| `commit` | 当前 commit index |

### Snapshot / SnapshotMetadata（快照）
| 字段 | 说明 |
|------|------|
| `data` | 快照的实际状态机数据 |
| `metadata.conf_state` | 快照时刻的成员配置 |
| `metadata.index/term` | 快照包含的最后一条日志的 index 和 term |

### ConfState / ConfChange（成员变更）
- `ConfState`：当前集群所有节点 ID 列表
- `ConfChange`：一次成员变更操作（`AddNode` / `RemoveNode` + `node_id`）

---

## 二、metapb.proto — 集群拓扑元信息

定义了 TinyKV 集群的拓扑概念：Cluster、Store、Region、Peer 的关系。

```
Cluster
 └── Store (物理节点进程)
      └── Peer (Region 在该 Store 上的副本)
           └── Region (一段连续 key 范围，由多个 Peer 组成 Raft Group)
```

### Cluster
| 字段 | 说明 |
|------|------|
| `id` | 集群 ID |
| `max_peer_count` | 每个 Region 的最大副本数，Scheduler 据此做自动均衡 |

### Store（存储节点）
| 字段 | 说明 |
|------|------|
| `id` | 全局唯一的 Store ID |
| `address` | 处理客户端请求的地址 |
| `state` | `Up`（正常）/ `Offline`（下线中）/ `Tombstone`（已销毁） |

### Region（数据分片）
| 字段 | 说明 |
|------|------|
| `id` | 全局唯一的 Region ID |
| `start_key` / `end_key` | 左闭右开的 key 范围 `[start_key, end_key)` |
| `region_epoch` | 版本号，用于检测过期请求 |
| `peers` | 该 Region 的所有副本列表 |

### RegionEpoch（Region 版本）
| 字段 | 说明 |
|------|------|
| `conf_ver` | 成员变更版本号，每次 AddPeer/RemovePeer 自增 |
| `version` | Region 版本号，每次 Split/Merge 自增 |

### Peer（副本）
| 字段 | 说明 |
|------|------|
| `id` | 全局唯一的 Peer ID |
| `store_id` | 该副本所在的 Store |

---

## 三、raft_serverpb.proto — Raft 存储层持久化与通信

连接 Raft 协议层和存储引擎层，定义了网络传输封装和持久化状态。

### RaftMessage（网络层 Raft 消息封装）
在 `eraftpb.Message` 外包装了路由信息，让消息能在多 Region 环境下正确投递。

| 字段 | 说明 |
|------|------|
| `region_id` | 目标 Region |
| `from_peer` / `to_peer` | 源/目标 Peer |
| `message` | 内部的 `eraftpb.Message` |
| `region_epoch` | 用于检测消息是否过期 |
| `is_tombstone` | 若为 true，通知对端该 Peer 已被移除 |
| `start_key` / `end_key` | Region 的 key 范围（3B 使用） |

### RaftLocalState（Raft 层持久化状态）
存储在 RaftDB 中，重启后恢复 Raft 状态。

| 字段 | 说明 |
|------|------|
| `hard_state` | `eraftpb.HardState`（term / vote / commit） |
| `last_index` | Raft log 中最后一条日志的 index |
| `last_term` | Raft log 中最后一条日志的 term |

### RaftApplyState（状态机 Apply 持久化状态）
存储在 KvDB 中，记录状态机的应用进度。

| 字段 | 说明 |
|------|------|
| `applied_index` | 状态机已 apply 到的 index，防止重启后重复 apply |
| `truncated_state` | 已截断（compact）的日志的最后 index 和 term（2C 使用） |

### RaftTruncatedState（日志压缩状态）
| 字段 | 说明 |
|------|------|
| `index` | 已截断的最后一条日志的 index |
| `term` | 已截断的最后一条日志的 term |

### RegionLocalState（Region 在本 Store 上的状态）
| 字段 | 说明 |
|------|------|
| `state` | `Normal`（正常）或 `Tombstone`（已从该 Region 移除） |
| `region` | 完整的 Region 元信息 |

### StoreIdent（Store 身份标识）
持久化的 Store 标识，重启后据此恢复 store_id。

| 字段 | 说明 |
|------|------|
| `cluster_id` | 所属集群 ID |
| `store_id` | 本 Store 的 ID |

### 快照相关（RaftSnapshotData 等）
用于快照的发送和接收，不在课程核心范围内。

---

## 四、raft_cmdpb.proto — 上层 KV 命令与管理命令

定义了客户端发给 Raft Group 的所有请求/响应，分为**普通读写命令**和**管理命令**两大类。

### 普通命令（CmdType）

| 命令 | Request 字段 | Response 字段 | 说明 |
|------|-------------|--------------|------|
| `Get` | `cf`, `key` | `value` | 读取指定 CF 下的 key |
| `Put` | `cf`, `key`, `value` | 无 | 写入 |
| `Delete` | `cf`, `key` | 无 | 删除 |
| `Snap` | 无 | `region` | 获取 Region 快照（用于 Scan） |

> `cf` = Column Family，TinyKV 使用 `default`、`lock`、`write` 三个 CF。

### 管理命令（AdminCmdType）

| 命令 | Request 字段 | Response 字段 | 说明 |
|------|-------------|--------------|------|
| `ChangePeer` | `change_type`, `peer` | `region` | 成员变更（加/删节点） |
| `CompactLog` | `compact_index`, `compact_term` | 无 | 日志压缩（2C） |
| `TransferLeader` | `peer` | 无 | 转让 Leader |
| `Split` | `split_key`, `new_region_id`, `new_peer_ids` | `regions` | Region 分裂（3B） |

### RaftCmdRequest / RaftCmdResponse（顶层封装）

```
RaftCmdRequest
├── header        ← region_id, peer, region_epoch, term（路由 + 合法性校验）
├── requests[]    ← 普通读写命令（可批量）
└── admin_request ← 管理命令（与 requests 互斥，不能同时存在）
```

```
RaftCmdResponse
├── header          ← error, uuid, current_term
├── responses[]     ← 普通命令响应
└── admin_response  ← 管理命令响应
```

---

## 五、数据结构关系总图

```
客户端请求                          Raft 协议内部                    持久化存储
─────────                          ──────────                    ────────
RaftCmdRequest                     Message                       RaftLocalState
 ├─ header (region_id, epoch)       ├─ msg_type                   ├─ hard_state (term/vote/commit)
 ├─ requests[] (Get/Put/Delete)     ├─ entries[] → Entry          └─ last_index/last_term
 └─ admin_request                   ├─ snapshot → Snapshot
    (ChangePeer/Split/Compact/      └─ commit                    RaftApplyState
     TransferLeader)                                              ├─ applied_index
        │                                  │                      └─ truncated_state
        │ 序列化为 Entry.data               │
        ▼                                  ▼                     RegionLocalState
    写入 Raft Log ──────────────→  复制到 Follower                  ├─ state (Normal/Tombstone)
                                                                  └─ region (metapb.Region)
                          网络层封装:
                          RaftMessage
                           ├─ region_id
                           ├─ from_peer / to_peer
                           └─ message (eraftpb.Message)
```
