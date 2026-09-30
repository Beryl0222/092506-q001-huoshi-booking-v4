# 火柿水上采摘调度

面向“西溪火柿活动”运营后台的名额与船班调度服务。工作人员维护扁柿、火柿、
方柿三种树区的按日容量和船班，系统按采摘日同时锁定**树区采摘名额**与
**船班座位**，处理带幂等键的预订、付款确认、取消、跨日改期，并在船班被
临时封存时按规则迁移或退回未完成订单。

只依赖 Python 标准库，数据落在本地 SQLite（WAL），无需启动其它服务。

## 目录

- `src/huoshi_booking/domain.py` — 品种、订单/船班/退款状态机、事件类型与错误码。
- `src/huoshi_booking/store.py` — SQLite 表结构、`BEGIN IMMEDIATE` 串行事务与读写。
- `src/huoshi_booking/service.py` — 应用服务：容量维护、预订、付款、取消、改期、封存处置。
- `src/huoshi_booking/api.py` — 进程内 JSON 适配层（`handle(json, service)`）。
- `tests/test_acceptance.py` — 并发预订、跨日改期、重复回调、封存与重启一致性验收。

## 并发与一致性保证

- 每个写操作都在单个立即写事务内完成“校验 + 扣减 + 状态流转 + 事件追加”，
  配合 `busy_timeout`，跨线程/跨连接请求在数据库层串行化，树区名额与船座位
  不会超卖。
- 预订/付款/改期/取消必须携带幂等键：首次请求落库响应，同键重放（请求体
  哈希一致）直接返回首次结果并标记 `replayed: true`；请求体不一致或键被
  其它游客/动作使用时返回 `idempotency_conflict`。
- 同一游客同一品种同一采摘日只允许一个未完成订单（应用校验 + 部分唯一索引
  双保险），并发重复提交不会产生重复扣减。
- 事件与业务状态同事务写入（事务性发件箱），`event_log.seq` 全局严格递增；
  服务重启后库存、订单状态、退款待办与事件顺序保持一致。

## 船班封存规则

封存某船班时，对其上所有未完成（`reserved`/`paid`）订单逐笔处理：

1. 优先迁移到**当日同路线**的开放船班，同路线满员后迁移到当日任意有余座的
   开放船班；订单状态保持不变，名额原子转移，记录 `booking.migrated`。
2. 找不到承接船班：已付款订单置为 `refund_pending` 并生成退款待办
   （原因 `seal_no_capacity` 或当日无其它船班时的 `seal_no_trip`）；
   未付款占位直接 `cancelled` 释放名额。

退款待办可由运营标记 `refund_paid`（订单终结为 `refunded`）或
`refund_failed`（挂起重试，订单为 `refund_failed`），同样支持幂等。

## JSON 接口

请求形如 `{"action": "...", ...}`，成功直接返回数据 JSON；业务错误返回
`{"error": {"code", "message"}}`。

| action | 说明 |
| --- | --- |
| `zone_maintain` | 维护树区（zone_id/variety∈flat,fire,square/name/daily_capacity，可重复调用更新） |
| `zone_list` / `zone_view` | 树区查询 |
| `trip_schedule` | 排船班（trip_id/date/route/seat_capacity） |
| `trip_update` | 调整座位数（不得低于已占位人数） |
| `trip_list` / `trip_view` | 船班查询（含 held/available/status） |
| `capacity` | 某品种某日的 capacity/held/available |
| `booking_create` | 幂等预订（visitor_id/variety/date/trip_id/idempotency_key，可选 amount/booking_id） |
| `payment_confirm` | 付款回调确认（booking_id/amount/idempotency_key），重复回调只入账一次 |
| `booking_cancel` | 取消（可选 idempotency_key），已付款自动生成退款待办 |
| `booking_reschedule` | 跨日/换船改期（booking_id/new_date/new_trip_id/idempotency_key） |
| `trip_seal` | 封存船班并处置未完成订单，返回 migrated/refunds/cancelled 清单 |
| `refund_list` / `refund_paid` / `refund_failed` | 退款待办查询与处理 |
| `events` | 事件流（after_seq/aggregate_type/aggregate_id），按 seq 升序 |
| `health` / `register` / `find` | 基线能力 |

## 运行

```sh
# 全部测试（含并发、跨日改期、重复回调、重启一致性）
PYTHONPATH=src python3 -m unittest discover -s tests -v

# 语法检查
python3 -m compileall src
```
