# message-broker-quorum-leader-fencing-validator
验证 broker quorum、leader 和生产者 fencing

> **注意：本仓库中所有 broker、controller、producer、consumer 的状态均为进程内模拟（simulated）。**
> 不涉及任何真实网络、磁盘或 ZooKeeper/KRaft 交互；代码仅用于验证 leader 切换后的
> fencing 语义，不能替代真实消息队列集群。

## 背景

broker leader 切换后，旧 leader 可能短暂存活并仍认为自己是 ISR 中的写入点：
生产者在旧 epoch 上收到了 ACK，消费者却从新 leader 读到不同的顺序。
本工具以事件驱动的方式模拟这一过程，验证 quorum leader fencing 是否阻止了
脑裂写入被新 quorum 接受。

## 环境

- Python 3.8.2+（仅标准库，无第三方依赖）

## 输入

场景以 JSON 描述（经 stdin 或文件路径传入 `quorum_fencing.py`）：

```json
{
  "brokers": [0, 1, 2],
  "leader": 0,
  "isr": [0, 1, 2],
  "leader_epoch": 0,
  "events": [
    {"type": "write", "broker": 0, "producer_id": "p1",
     "producer_epoch": 0, "sequence": 0, "value": "a"},
    {"type": "elect", "leader": 1, "isr": [1, 2], "leader_epoch": 1},
    {"type": "write", "broker": 0, "producer_id": "p1",
     "producer_epoch": 0, "sequence": 1, "value": "stale"},
    {"type": "isr_change", "isr": [1]},
    {"type": "sync", "broker": 0},
    {"type": "read", "consumer_offset": 0}
  ]
}
```

事件类型：

- `write`：向指定 broker 发起一次写入，携带 producer epoch 与 sequence。
- `elect`：controller 选举新 leader（leader epoch 递增，ISR 可同时收缩）；
  旧 leader 不会被通知，模拟其"短暂存活"窗口。
- `isr_change`：当前 leader 上的 ISR 收缩/扩张，提交水位随之重算（单调不回退）。
- `sync`：旧 broker 学习新 epoch、以 follower 身份重新加入，
  在恢复边界处截断分歧日志。
- `read`：消费者从当前 leader 按 offset 读取，仅高水位以下的 offset 可见。

## 输出

JSON 报告，关键字段：

- `writes`：每次写入的判定结果——`acked`（broker 是否返回 ACK）与
  `quorum_accepted`（新 quorum 是否接受）分离；`status` 取值包括
  `accepted`、`stale_leader_epoch`、`producer_fenced_epoch_regression`、
  `duplicate_ack`、`out_of_order_sequence`、`not_leader_for_partition`。
- `illegal_writes`：被旧 leader ACK 但未被新 quorum 接受的非法写入。
- `reads[].visible_offsets` / `reads[].values`：消费者可见的 offset 与顺序
  （来自新 quorum 的日志，旧 leader 的分歧写入不可见）。
- `recovery_boundary` / `truncated_offsets`：旧 leader 重新加入时的恢复边界
  （新 leader 高水位与自身高水位的较小者）及被截断的 offset。

## fencing 规则（模拟语义）

- **Leader epoch fencing**：旧 leader 短暂存活期间仍本地 ACK，
  但写入不会进入新 quorum，记为非法写入，恢复时被截断。
- **Producer epoch fencing**：producer epoch 回退即被拒绝；
  更新的 epoch 会重置 sequence 空间。
- **幂等 sequence**：重复 sequence 返回重复 ACK 且不重复追加；
  sequence 跳号被拒绝。
- **ISR 收缩**：提交水位 = ISR 成员最小 LEO，且单调不回退。

## 运行

```bash
# 运行测试（leader 切换、旧 epoch 写入、ISR 变化、producer epoch 回退、重复 ACK）
python -m unittest

# 运行一个场景
python quorum_fencing.py scenario.json
cat scenario.json | python quorum_fencing.py
```

## 文件

- `quorum_fencing.py`：模拟器核心（`QuorumCluster`）+ 场景运行器 + CLI。
- `test_quorum_fencing.py`：`unittest` 测试，覆盖 leader 切换、旧 epoch 写入、
  ISR 变化、producer epoch 回退、重复 ACK 与恢复边界。
