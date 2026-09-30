"""火柿水上采摘调度的基础领域对象。

业务约束：

* 品种固定为扁柿、火柿、方柿三类，每类对应若干采摘树区；
* 名额按「采摘日 + 品种树区」维度锁定，船班只按船号控制载客；
* 同一游客 (visitor_id) 对同一产品（品种+日期+船班）在任意时刻至多有一个
  未完结订单（pending_payment 或 confirmed），取消/过期后名额释放；
* 所有状态迁移都要追加一条事件，事件序号全局递增、可追溯。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

#: 允许运营维护的三个柿子品种。
VARIETIES: tuple[str, ...] = ("扁柿", "火柿", "方柿")

#: 预订未付款的最长保留分钟数（可由运营配置）。
DEFAULT_PAYMENT_TIMEOUT_MINUTES = 15


def now_iso() -> str:
    """统一使用带时区的 UTC ISO 时间。"""
    return datetime.now(timezone.utc).isoformat()


class DomainError(Exception):
    """业务规则冲突（状态非法、容量不足、迁移无可用船班等）。"""


class NotFoundError(DomainError):
    """引用的对象不存在。"""


class IdempotencyConflict(DomainError):
    """幂等键被复用于语义不同的请求。"""


# -- 订单与事件状态 ---------------------------------------------------------

#: 待付款：名额已锁定，超时未付款会被释放。
ORDER_PENDING = "pending_payment"
#: 已付款确认。
ORDER_CONFIRMED = "confirmed"
#: 已取消（运营/游客发起，或付款超时）。
ORDER_CANCELLED = "cancelled"
#: 已改期：原订单作废，由新订单接替。
ORDER_RESCHEDULED = "rescheduled"
#: 因船班封存被退回（钱待退）。
ORDER_REFUNDED = "refunded"
#: 因船班封存被迁移到新船班/日期。
ORDER_MIGRATED = "migrated"

ACTIVE_ORDER_STATES = (ORDER_PENDING, ORDER_CONFIRMED)
FINAL_ORDER_STATES = (ORDER_CANCELLED, ORDER_RESCHEDULED, ORDER_REFUNDED, ORDER_MIGRATED)

#: 退款待办：等待财务线下/线上退款。
REFUND_PENDING = "pending"
#: 退款已完成（由运营在后台确认）。
REFUND_DONE = "done"


@dataclass(frozen=True)
class TreeZone:
    """某个品种的采摘树区及其每日可售名额容量。"""

    zone_id: str
    variety: str
    name: str
    daily_capacity: int
    unit_price_fen: int = 0


@dataclass(frozen=True)
class BoatTrip:
    """某采摘日、某时段的一条船班。

    capacity 是船只核载人数；status 为 active 时可售，sealed 表示园区临时封存。
    """

    trip_id: str
    boat_route: str
    pick_date: str  # YYYY-MM-DD
    time_slot: str  # 例如 "09:00-10:00"
    capacity: int
    status: str = "active"  # active | sealed
    sealed_at: str = ""
    seal_reason: str = ""


@dataclass(frozen=True)
class Order:
    """一笔采摘名额订单。"""

    order_id: str
    visitor_id: str
    variety: str
    zone_id: str
    pick_date: str
    trip_id: str
    quantity: int
    state: str
    amount_fen: int
    idempotency_key: str
    version: int = 0
    replaced_by: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class Event:
    """订单/船班生命周期事件，seq 全局递增。"""

    seq: int
    event_type: str
    order_id: str
    trip_id: str
    payload: str  # JSON 字符串
    created_at: str


@dataclass(frozen=True)
class RefundTodo:
    """封存退回后产生的退款待办。"""

    refund_id: str
    order_id: str
    visitor_id: str
    amount_fen: int
    reason: str
    status: str
    created_at: str
    handled_at: str = ""


@dataclass(frozen=True)
class IdempotencyRecord:
    """幂等键 → 操作结果的映射。"""

    idempotency_key: str
    visitor_id: str
    request_kind: str
    request_fingerprint: str
    order_id: str
    response_json: str
    created_at: str


@dataclass
class Availability:
    """某 (日期, 品种树区, 船班) 的可售视图。"""

    pick_date: str
    zone_id: str
    variety: str
    trip_id: str
    boat_route: str
    time_slot: str
    zone_capacity: int
    zone_reserved: int
    boat_capacity: int
    boat_reserved: int

    @property
    def remaining(self) -> int:
        return min(self.zone_capacity - self.zone_reserved,
                   self.boat_capacity - self.boat_reserved)

    def as_dict(self) -> dict:
        data = {
            "pick_date": self.pick_date,
            "zone_id": self.zone_id,
            "variety": self.variety,
            "trip_id": self.trip_id,
            "boat_route": self.boat_route,
            "time_slot": self.time_slot,
            "zone_capacity": self.zone_capacity,
            "zone_reserved": self.zone_reserved,
            "boat_capacity": self.boat_capacity,
            "boat_reserved": self.boat_reserved,
            "remaining": self.remaining,
        }
        return data


@dataclass
class Record:
    """兼容旧基线测试的通用记录对象。"""

    record_id: str
    owner_id: str
    state: str = "draft"
    created_at: str = ""

    def with_timestamp(self) -> "Record":
        return Record(self.record_id, self.owner_id, self.state,
                      self.created_at or now_iso())
