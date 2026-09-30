"""船班与采摘名额的基础领域对象。

约定:
- 所有日期使用 ``YYYY-MM-DD`` 字符串，避免跨环境的日期解析差异。
- 订单与容量的状态机取值集中在本模块常量中，禁止在别处散落魔法字符串。
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

#: 三种柿子品种（树区按品种独立维护容量）
VARIETIES: tuple[str, ...] = ("flat", "fire", "square")
VARIETY_NAMES: dict[str, str] = {
    "flat": "扁柿",
    "fire": "火柿",
    "square": "方柿",
}

#: 订单状态机
BOOKING_RESERVED = "reserved"       # 已占位、待付款
BOOKING_PAID = "paid"               # 已付款确认
BOOKING_CANCELLED = "cancelled"     # 已取消（名额已释放）
# 封存迁移的订单仍占用新船班名额，状态保持 reserved/paid，迁移经过由事件留痕
BOOKING_REFUND_PENDING = "refund_pending"  # 封存后无法迁移，等待人工退款
BOOKING_REFUNDED = "refunded"              # 退款已打款，订单终结
BOOKING_TERMINAL_FAILURE = "refund_failed"  # 退款执行失败（仍挂在退款台账）

#: 未完成（封存时需要处理的）订单状态
ACTIVE_BOOKING_STATES: tuple[str, ...] = (BOOKING_RESERVED, BOOKING_PAID)

#: 船班状态
TRIP_OPEN = "open"
TRIP_SEALED = "sealed"

#: 事件类型
EVENT_ZONE_UPDATED = "zone.updated"
EVENT_TRIP_SCHEDULED = "trip.scheduled"
EVENT_TRIP_UPDATED = "trip.updated"
EVENT_TRIP_SEALED = "trip.sealed"
EVENT_BOOKING_CREATED = "booking.created"
EVENT_PAYMENT_CONFIRMED = "payment.confirmed"
EVENT_PAYMENT_DUPLICATE = "payment.duplicate"
EVENT_PAYMENT_REJECTED = "payment.rejected"
EVENT_BOOKING_CANCELLED = "booking.cancelled"
EVENT_BOOKING_SEAL_CANCELLED = "booking.seal_cancelled"
EVENT_BOOKING_RESCHEDULED = "booking.rescheduled"
EVENT_BOOKING_MIGRATED = "booking.migrated"
EVENT_BOOKING_REFUND_DUE = "booking.refund_due"
EVENT_REFUND_MARKED_PAID = "refund.marked_paid"
EVENT_REFUND_FAILED = "refund.failed"

#: 退款待办状态
REFUND_PENDING = "pending"
REFUND_PAID = "paid"
REFUND_FAILED = "failed"

#: 退款原因
REASON_CANCEL = "cancel"
REASON_SEAL_NO_CAPACITY = "seal_no_capacity"
REASON_SEAL_NO_TRIP = "seal_no_trip"


class DomainError(Exception):
    """业务规则错误，``code`` 会原样返回给运营后台。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def require_variety(variety: str) -> str:
    if variety not in VARIETIES:
        raise DomainError("unknown_variety", f"未知品种: {variety!r}")
    return variety


@dataclass(frozen=True)
class Record:
    """早期登记入口使用的通用记录（保留基线行为）。"""

    record_id: str
    owner_id: str
    state: str = "draft"
    created_at: str = ""

    def with_timestamp(self) -> "Record":
        return replace(self, created_at=self.created_at or utc_now())


@dataclass(frozen=True)
class TreeZone:
    zone_id: str
    variety: str
    name: str
    daily_capacity: int
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class BoatTrip:
    trip_id: str
    date: str
    route: str
    seat_capacity: int
    status: str = TRIP_OPEN
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class Booking:
    booking_id: str
    visitor_id: str
    variety: str
    date: str
    trip_id: str
    status: str
    amount: int
    idempotency_key: str
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class Event:
    seq: int
    event_type: str
    aggregate_type: str
    aggregate_id: str
    payload: str
    created_at: str


@dataclass(frozen=True)
class RefundTask:
    refund_id: str
    booking_id: str
    amount: int
    reason: str
    status: str
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class IdempotencyRecord:
    idempotency_key: str
    visitor_id: str
    request_hash: str
    action: str
    result_ref: str
    response_json: str
    created_at: str = ""


@dataclass(frozen=True)
class CapacityView:
    variety: str
    date: str
    capacity: int
    held: int
    booked_count: int

    @property
    def available(self) -> int:
        return self.capacity - self.held


@dataclass(frozen=True)
class TripCapacityView:
    trip_id: str
    date: str
    route: str
    seat_capacity: int
    held: int
    booked_count: int
    status: str

    @property
    def available(self) -> int:
        return self.seat_capacity - self.held
