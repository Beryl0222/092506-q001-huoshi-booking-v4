# 火柿水上采摘调度

面向「西溪火柿活动」运营后台的名额与船班调度服务。工作人员可以维护扁柿、火柿、
方柿三个品种的树区每日容量与乘船班次，按采摘日锁定名额；游客侧处理带幂等键的
预订、付款确认、取消与跨日改期；园区临时封存船班时，按规则退回未完成订单（生成
退款待办）或迁移到其他船班。所有状态迁移留下全局递增的事件记录，服务重启后
库存、订单、退款待办与事件顺序保持一致。

## 核心约束

- **双维度库存**：名额按「采摘日 + 品种树区」与「船班载客」分别计数，两者都不允许超卖。
- **并发不重复扣减**：所有写操作在 `BEGIN IMMEDIATE` 事务中完成「校验 → 查库存 →
  写订单 → 记事件」；订单表上有部分唯一索引，同一游客对同一（品种/日期/船班）
  至多一个未完结订单（待付款或已确认）。
- **幂等**：预订、付款、取消、改期都必须带 `idempotency_key`；服务保存请求指纹与
  首次响应，重复回调直接重放首次结果，幂等键复用于不同请求会被拒绝。
- **封存处置**：`refund` 模式下已付款订单置为 `refunded` 并进退款待办、未付款订单
  直接取消；`migrate` 模式下按「同路线 → 其他路线、同日优先、时段从早到晚」迁移，
  容量不足的订单按退款规则兜底。
- **可追溯**：每次状态变化同事务写一条 `event_log`，`seq` 全局递增，可按订单/船班
  查询，重启后顺序不变。

## 目录

- `src/huoshi_booking/domain.py` 领域对象、订单/退款状态与错误类型。
- `src/huoshi_booking/store.py` SQLite 表结构、WAL 配置、事务与查询。
- `src/huoshi_booking/service.py` 应用服务：容量维护、预订全流程、封存处置。
- `src/huoshi_booking/api.py` JSON 字符串进/出的进程内请求适配层。
- `src/huoshi_booking/server.py` 标准库 `http.server` 入口（可选，便于接口验收）。
- `tests/` 基线测试与并发/改期/重复回调/封存/重启验收测试。

## 运行

运行测试（项目只依赖 Python 标准库）：

```
PYTHONPATH=src python3 -m unittest discover -s tests
```

可选的 HTTP 入口：

```
PYTHONPATH=src python3 -m huoshi_booking.server --db data/huoshi.db --port 8080
```

启动后向 `POST http://127.0.0.1:8080/` 发送 JSON，例如：

```json
{"action": "zone.upsert", "zone_id": "z-bs", "variety": "扁柿",
 "name": "扁柿A区", "daily_capacity": 200, "unit_price_fen": 5000}
{"action": "trip.upsert", "trip_id": "t-1001", "boat_route": "深潭口线",
 "pick_date": "2026-10-01", "time_slot": "09:00-10:00", "capacity": 30}
{"action": "order.book", "visitor_id": "v1", "variety": "扁柿",
 "zone_id": "z-bs", "pick_date": "2026-10-01", "trip_id": "t-1001",
 "quantity": 2, "idempotency_key": "visitor-uuid-1"}
{"action": "order.pay", "order_id": "O-...", "idempotency_key": "pay-cb-1"}
{"action": "order.reschedule", "order_id": "O-...",
 "new_pick_date": "2026-10-02", "new_trip_id": "t-1002",
 "idempotency_key": "rs-1"}
{"action": "trip.seal", "trip_id": "t-1001", "reason": "封航",
 "mode": "migrate"}
```

## 接口动作一览

| action | 说明 |
| --- | --- |
| `zone.upsert` / `zone.list` | 维护/查询品种树区与每日容量、单价 |
| `trip.upsert` / `trip.list` | 维护/查询船班（路线、日期、时段、载客量） |
| `trip.seal` | 封存船班，`mode=refund|migrate`，可指定 `target_trip_id` |
| `inventory` | 查询某日树区/船班容量、占用与剩余 |
| `order.book` | 带幂等键的预订，锁定树区+船班名额 |
| `order.pay` | 付款确认回调（重复回调安全） |
| `order.cancel` | 取消；已付款订单生成退款待办 |
| `order.reschedule` | 改期/换船（支持跨日），派生单号 `原单号-R1` |
| `order.get` | 查询订单当前状态 |
| `refund.list` / `refund.complete` | 退款待办查询与财务确认 |
| `events` | 按订单/船班查询事件流（支持 `after_seq` 轮询） |
| `sweep.expired` | 取消超时未付款订单并释放名额 |

## 验收场景

`tests/test_acceptance.py` 重放了：

1. **并发预订**：多连接、屏障同时发起，船班/树区不超卖；同一游客相同幂等键
   只产生一笔订单（其余为重放），不同幂等键也只有一笔成功、不重复扣减。
2. **跨日改期**：旧名额释放、新名额占用，事件链 `created → paid → rescheduled → created`
   完整且 `seq` 递增；满载时改期失败、原订单不受影响。
3. **重复回调**：付款回调重复推送只确认一次；幂等键复用指纹不同会报冲突。
4. **封存处置**：退回模式生成退款待办、迁移模式同路线下一班接管、容量不足兜底、
   重复封存不重复处置。
5. **重启一致**：关闭进程后以文件方式重新打开 SQLite，库存、订单状态、退款待办、
   事件顺序与幂等去重全部保持。
6. **付款/取消竞争**：多轮并发验证先付款则取消走退款、先取消则付款被拒。
