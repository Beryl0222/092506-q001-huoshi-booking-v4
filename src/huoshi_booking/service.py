"""火柿水上采摘的应用服务入口。

每个写操作都在单个 ``BEGIN IMMEDIATE`` 事务内完成“校验 + 扣减 + 状态流转
+ 事件追加”，因此：

- 同游客的并发请求要么共享幂等结果，要么被唯一约束挡住，不会重复扣减；
- 树区名额与船座位不会超卖；
- 事件与业务状态同生共死，重启后顺序与库存仍然一致。
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import replace
from typing import Any

from . import domain as D
from .domain import (
    DomainError,
    require_variety,
    utc_now,
    Booking,
    BoatTrip,
    IdempotencyRecord,
    Record,
    RefundTask,
    TreeZone,
)
from .store import Store

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _require_date(value: str) -> str:
    if not isinstance(value, str) or not _DATE_RE.match(value):
        raise DomainError("bad_date", f"日期必须是 YYYY-MM-DD: {value!r}")
    return value


def _request_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class Service:
    def __init__(self, store: Store | None = None) -> None:
        self.store = store or Store()

    # ------------------------------------------------------------ 基线能力
    def health(self) -> dict[str, str]:
        return {"service": "huoshi_booking", "status": "ok"}

    def register(self, record_id: str, owner_id: str) -> dict[str, str]:
        record = self.store.save_record(Record(record_id, owner_id))
        return {"record_id": record.record_id, "owner_id": record.owner_id,
                "state": record.state, "created_at": record.created_at}

    def find(self, record_id: str) -> dict[str, str] | None:
        record = self.store.get_record(record_id)
        return record.__dict__.copy() if record else None

    # ------------------------------------------------------- 运营：树区维护
    def maintain_zone(self, zone_id: str, variety: str, name: str,
                      daily_capacity: int) -> dict[str, Any]:
        """维护某品种树区的日容量；zone_id 已存在则整体更新。"""
        require_variety(variety)
        if not isinstance(daily_capacity, int) or daily_capacity < 0:
            raise DomainError("bad_capacity", "日容量必须是非负整数")
        if not zone_id or not name:
            raise DomainError("bad_argument", "zone_id 与 name 不能为空")
        now = utc_now()
        existing = next((z for z in self.store.list_zones()
                         if z.zone_id == zone_id), None)
        zone = TreeZone(
            zone_id=zone_id, variety=variety, name=name,
            daily_capacity=daily_capacity,
            created_at=existing.created_at if existing else now,
            updated_at=now,
        )
        with self.store.tx() as conn:
            self.store.upsert_zone(conn, zone)
            self.store.append_event(
                conn, D.EVENT_ZONE_UPDATED, "zone", zone_id,
                {"zone_id": zone_id, "variety": variety, "name": name,
                 "daily_capacity": daily_capacity,
                 "existed": existing is not None}, now,
            )
        return self.zone_view(zone_id)

    def list_zones(self) -> list[dict[str, Any]]:
        return [self._zone_dict(z) for z in self.store.list_zones()]

    def zone_view(self, zone_id: str) -> dict[str, Any]:
        zone = next((z for z in self.store.list_zones()
                     if z.zone_id == zone_id), None)
        if zone is None:
            raise DomainError("zone_not_found", f"树区不存在: {zone_id}")
        return self._zone_dict(zone)

    @staticmethod
    def _zone_dict(zone: TreeZone) -> dict[str, Any]:
        return {"zone_id": zone.zone_id, "variety": zone.variety,
                "variety_name": D.VARIETY_NAMES.get(zone.variety, zone.variety),
                "name": zone.name, "daily_capacity": zone.daily_capacity,
                "created_at": zone.created_at, "updated_at": zone.updated_at}

    # ------------------------------------------------------- 运营：船班维护
    def schedule_trip(self, trip_id: str, date: str, route: str,
                      seat_capacity: int) -> dict[str, Any]:
        """排一个新的船班（日期 + 乘船路线 + 座位数）。"""
        _require_date(date)
        if not trip_id or not route:
            raise DomainError("bad_argument", "trip_id 与 route 不能为空")
        if not isinstance(seat_capacity, int) or seat_capacity < 0:
            raise DomainError("bad_capacity", "座位数必须是非负整数")
        if self.store.get_trip_connless(trip_id) is not None:
            raise DomainError("trip_exists", f"船班已存在: {trip_id}")
        now = utc_now()
        trip = BoatTrip(trip_id=trip_id, date=date, route=route,
                        seat_capacity=seat_capacity, status=D.TRIP_OPEN,
                        created_at=now, updated_at=now)
        with self.store.tx() as conn:
            self.store.insert_trip(conn, trip)
            self.store.append_event(
                conn, D.EVENT_TRIP_SCHEDULED, "trip", trip_id,
                {"trip_id": trip_id, "date": date, "route": route,
                 "seat_capacity": seat_capacity}, now,
            )
        return self.trip_view(trip_id)

    def update_trip(self, trip_id: str, seat_capacity: int) -> dict[str, Any]:
        """调整船班座位数；不允许缩到当前已占位人数以下。"""
        if not isinstance(seat_capacity, int) or seat_capacity < 0:
            raise DomainError("bad_capacity", "座位数必须是非负整数")
        with self.store.tx() as conn:
            trip = self.store.get_trip(conn, trip_id)
            if trip is None:
                raise DomainError("trip_not_found", f"船班不存在: {trip_id}")
            held = self.store.trip_held(conn, trip_id)
            if seat_capacity < held:
                raise DomainError(
                    "capacity_below_held",
                    f"新座位数 {seat_capacity} 小于已占位 {held}",
                )
            now = utc_now()
            self.store.append_event(
                conn, D.EVENT_TRIP_UPDATED, "trip", trip_id,
                {"trip_id": trip_id,
                 "seat_capacity": {"from": trip.seat_capacity,
                                   "to": seat_capacity}}, now,
            )
            conn.execute(
                "UPDATE boat_trip SET seat_capacity=?, updated_at=? WHERE trip_id=?",
                (seat_capacity, now, trip_id),
            )
        return self.trip_view(trip_id)

    def list_trips(self, date: str | None = None) -> list[dict[str, Any]]:
        return [self._trip_dict(t) for t in self.store.list_trips(date)]

    def trip_view(self, trip_id: str) -> dict[str, Any]:
        trip = self.store.get_trip_connless(trip_id)
        if trip is None:
            raise DomainError("trip_not_found", f"船班不存在: {trip_id}")
        held = self.store.trip_held(self.store.connection, trip_id)
        view = self._trip_dict(trip)
        view["held"] = held
        view["available"] = trip.seat_capacity - held
        return view

    @staticmethod
    def _trip_dict(trip: BoatTrip) -> dict[str, Any]:
        return {"trip_id": trip.trip_id, "date": trip.date, "route": trip.route,
                "seat_capacity": trip.seat_capacity, "status": trip.status,
                "created_at": trip.created_at, "updated_at": trip.updated_at}

    # ----------------------------------------------------------- 库存查询
    def capacity(self, variety: str, date: str) -> dict[str, Any]:
        """某品种某日的树区采摘名额总览。"""
        require_variety(variety)
        _require_date(date)
        with self.store.tx() as conn:
            total = self.store.zone_capacity(conn, variety, date)
            held = self.store.capacity_held(conn, variety, date)
        return {"variety": variety,
                "variety_name": D.VARIETY_NAMES[variety],
                "date": date, "capacity": total, "held": held,
                "available": total - held}

    # ------------------------------------------------------------- 预订
    def create_booking(self, visitor_id: str, variety: str, date: str,
                       trip_id: str, amount: int = 0,
                       idempotency_key: str | None = None,
                       booking_id: str | None = None) -> dict[str, Any]:
        """带幂等键的预订：锁定一个树区名额 + 一个船班座位（状态 reserved）。"""
        require_variety(variety)
        _require_date(date)
        if not visitor_id:
            raise DomainError("bad_argument", "visitor_id 不能为空")
        if not isinstance(amount, int) or amount < 0:
            raise DomainError("bad_argument", "amount 必须是非负整数")
        if not idempotency_key:
            raise DomainError("idempotency_required", "预订必须携带幂等键")

        request = {"visitor_id": visitor_id, "variety": variety, "date": date,
                   "trip_id": trip_id, "amount": amount}
        with self.store.tx() as conn:
            replay = self._replay_if_seen(conn, "booking", idempotency_key,
                                          visitor_id, request)
            if replay is not None:
                return replay

            trip = self.store.get_trip(conn, trip_id)
            if trip is None:
                raise DomainError("trip_not_found", f"船班不存在: {trip_id}")
            if trip.status != D.TRIP_OPEN:
                raise DomainError("trip_sealed", f"船班已封存: {trip_id}")
            if trip.date != date:
                raise DomainError(
                    "trip_date_mismatch",
                    f"船班日期 {trip.date} 与采摘日 {date} 不一致",
                )

            zone_cap = self.store.zone_capacity(conn, variety, date)
            zone_held = self.store.capacity_held(conn, variety, date)
            if zone_cap <= 0:
                raise DomainError("no_quota",
                                  f"{D.VARIETY_NAMES[variety]} 在 {date} 未配置树区容量")
            if zone_held + 1 > zone_cap:
                raise DomainError("sold_out",
                                  f"{D.VARIETY_NAMES[variety]} {date} 采摘名额已满")

            trip_held = self.store.trip_held(conn, trip_id)
            if trip_held + 1 > trip.seat_capacity:
                raise DomainError("trip_full", f"船班 {trip_id} 座位已满")

            duplicate = conn.execute(
                "SELECT booking_id FROM booking WHERE visitor_id=? AND variety=?"
                " AND date=? AND status IN (?, ?)",
                (visitor_id, variety, date,
                 D.BOOKING_RESERVED, D.BOOKING_PAID),
            ).fetchone()
            if duplicate is not None:
                raise DomainError(
                    "duplicate_active",
                    f"游客 {visitor_id} 在 {date} 已有未完成预订 {duplicate['booking_id']}",
                )

            now = utc_now()
            new_id = booking_id or f"bk-{uuid.uuid4().hex[:12]}"
            booking = Booking(
                booking_id=new_id, visitor_id=visitor_id, variety=variety,
                date=date, trip_id=trip_id, status=D.BOOKING_RESERVED,
                amount=amount, idempotency_key=idempotency_key,
                created_at=now, updated_at=now,
            )
            try:
                self.store.insert_booking(conn, booking)
            except Exception as exc:  # 唯一约束兜底，绝不出现双扣
                raise DomainError("duplicate_active", "并发重复预订被拒绝") from exc

            self.store.append_event(
                conn, D.EVENT_BOOKING_CREATED, "booking", new_id,
                {"booking_id": new_id, "visitor_id": visitor_id,
                 "variety": variety, "date": date, "trip_id": trip_id,
                 "amount": amount, "idempotency_key": idempotency_key}, now,
            )
            response = self._booking_dict(booking)
            response["replayed"] = False
            self._store_idempotency(conn, idempotency_key, visitor_id, "booking",
                                    request, new_id, response, now)
            return dict(response)

    # ----------------------------------------------------------- 付款确认
    def confirm_payment(self, booking_id: str, amount: int,
                        idempotency_key: str) -> dict[str, Any]:
        """支付平台回调确认。重复回调重放首次结果，绝不重复入账。"""
        if not idempotency_key:
            raise DomainError("idempotency_required", "付款回调必须携带幂等键")
        if not isinstance(amount, int) or amount < 0:
            raise DomainError("bad_argument", "amount 必须是非负整数")
        request = {"booking_id": booking_id, "amount": amount}
        with self.store.tx() as conn:
            replay = self._replay_if_seen(conn, "payment", idempotency_key,
                                          None, request)
            if replay is not None:
                booking = self.store.get_booking_conn(conn, booking_id)
                if booking is not None:
                    self.store.append_event(
                        conn, D.EVENT_PAYMENT_DUPLICATE, "booking", booking_id,
                        {"booking_id": booking_id, "amount": amount,
                         "idempotency_key": idempotency_key}, utc_now(),
                    )
                return replay

            booking = self.store.get_booking_conn(conn, booking_id)
            if booking is None:
                raise DomainError("booking_not_found", f"订单不存在: {booking_id}")
            now = utc_now()
            if booking.status == D.BOOKING_PAID:
                # 未带同一幂等键的重复通知：记录但不重复入账
                self.store.append_event(
                    conn, D.EVENT_PAYMENT_DUPLICATE, "booking", booking_id,
                    {"booking_id": booking_id, "amount": amount,
                     "idempotency_key": idempotency_key}, now,
                )
                response = self._booking_dict(booking)
                response["replayed"] = True
                self._store_idempotency(conn, idempotency_key,
                                        booking.visitor_id, "payment", request,
                                        booking_id, response, now)
                return response
            if booking.status != D.BOOKING_RESERVED:
                self.store.append_event(
                    conn, D.EVENT_PAYMENT_REJECTED, "booking", booking_id,
                    {"booking_id": booking_id, "amount": amount,
                     "idempotency_key": idempotency_key,
                     "reason": f"订单状态 {booking.status} 不可付款"}, now,
                )
                raise DomainError(
                    "booking_not_active",
                    f"订单状态 {booking.status} 不可付款确认",
                )
            if amount != booking.amount:
                raise DomainError(
                    "amount_mismatch",
                    f"回调金额 {amount} 与应付 {booking.amount} 不一致",
                )

            paid = replace(booking, status=D.BOOKING_PAID, updated_at=now)
            self.store.update_booking(conn, paid)
            self.store.insert_payment(conn, booking_id, amount,
                                      idempotency_key, now)
            self.store.append_event(
                conn, D.EVENT_PAYMENT_CONFIRMED, "booking", booking_id,
                {"booking_id": booking_id, "amount": amount,
                 "idempotency_key": idempotency_key}, now,
            )
            response = self._booking_dict(paid)
            response["replayed"] = False
            self._store_idempotency(conn, idempotency_key, booking.visitor_id,
                                    "payment", request, booking_id, response, now)
            return dict(response)

    # --------------------------------------------------------------- 取消
    def cancel_booking(self, booking_id: str,
                       idempotency_key: str | None = None) -> dict[str, Any]:
        """取消未完成订单：释放名额；已付款的生成退款待办。"""
        request = {"booking_id": booking_id}
        with self.store.tx() as conn:
            if idempotency_key:
                replay = self._replay_if_seen(conn, "cancel", idempotency_key,
                                              None, request)
                if replay is not None:
                    return replay
            booking = self.store.get_booking_conn(conn, booking_id)
            if booking is None:
                raise DomainError("booking_not_found", f"订单不存在: {booking_id}")
            now = utc_now()

            refund_id: str | None = None
            if booking.status in D.ACTIVE_BOOKING_STATES:
                cancelled = replace(booking, status=D.BOOKING_CANCELLED,
                                    updated_at=now)
                self.store.update_booking(conn, cancelled)
                if booking.status == D.BOOKING_PAID and booking.amount > 0:
                    refund_id = f"rf-{uuid.uuid4().hex[:12]}"
                    self.store.insert_refund(
                        conn,
                        RefundTask(refund_id=refund_id, booking_id=booking_id,
                                   amount=booking.amount,
                                   reason=D.REASON_CANCEL, status=D.REFUND_PENDING,
                                   created_at=now, updated_at=now),
                    )
                self.store.append_event(
                    conn, D.EVENT_BOOKING_CANCELLED, "booking", booking_id,
                    {"booking_id": booking_id, "prior_status": booking.status,
                     "refund_id": refund_id}, now,
                )
                booking = cancelled
            elif booking.status == D.BOOKING_CANCELLED:
                # 重复取消直接重放当前状态
                pass
            else:
                raise DomainError(
                    "booking_not_active",
                    f"订单状态 {booking.status} 不可取消",
                )

            response = self._booking_dict(booking)
            response["refund_id"] = refund_id
            response["replayed"] = False
            if idempotency_key:
                self._store_idempotency(conn, idempotency_key,
                                        booking.visitor_id, "cancel", request,
                                        booking_id, response, now)
            return response

    # --------------------------------------------------------------- 改期
    def reschedule_booking(self, booking_id: str, new_date: str,
                           new_trip_id: str,
                           idempotency_key: str) -> dict[str, Any]:
        """跨日（或同日换船）改期：原子释放旧名额、占用新名额。"""
        _require_date(new_date)
        if not idempotency_key:
            raise DomainError("idempotency_required", "改期必须携带幂等键")
        request = {"booking_id": booking_id, "new_date": new_date,
                   "new_trip_id": new_trip_id}
        with self.store.tx() as conn:
            replay = self._replay_if_seen(conn, "reschedule", idempotency_key,
                                          None, request)
            if replay is not None:
                return replay

            booking = self.store.get_booking_conn(conn, booking_id)
            if booking is None:
                raise DomainError("booking_not_found", f"订单不存在: {booking_id}")
            if booking.status not in D.ACTIVE_BOOKING_STATES:
                raise DomainError(
                    "booking_not_active",
                    f"订单状态 {booking.status} 不可改期",
                )
            target = self.store.get_trip(conn, new_trip_id)
            if target is None:
                raise DomainError("trip_not_found", f"船班不存在: {new_trip_id}")
            if target.status != D.TRIP_OPEN:
                raise DomainError("trip_sealed", f"目标船班已封存: {new_trip_id}")
            if target.date != new_date:
                raise DomainError(
                    "trip_date_mismatch",
                    f"船班日期 {target.date} 与改期日 {new_date} 不一致",
                )

            # 跨日时需要重新锁定新品种日的树区名额；计数排除订单自身。
            zone_cap = self.store.zone_capacity(conn, booking.variety, new_date)
            zone_held = self.store.capacity_held(
                conn, booking.variety, new_date, exclude_booking_id=booking_id)
            if zone_cap <= 0:
                raise DomainError(
                    "no_quota",
                    f"{D.VARIETY_NAMES[booking.variety]} 在 {new_date} 未配置树区容量",
                )
            if zone_held + 1 > zone_cap:
                raise DomainError("sold_out",
                                  f"{D.VARIETY_NAMES[booking.variety]} {new_date} 采摘名额已满")
            trip_held = self.store.trip_held(
                conn, new_trip_id, exclude_booking_id=booking_id)
            if trip_held + 1 > target.seat_capacity:
                raise DomainError("trip_full", f"船班 {new_trip_id} 座位已满")

            now = utc_now()
            moved = replace(booking, date=new_date, trip_id=new_trip_id,
                            updated_at=now)
            self.store.update_booking(conn, moved)
            self.store.append_event(
                conn, D.EVENT_BOOKING_RESCHEDULED, "booking", booking_id,
                {"booking_id": booking_id,
                 "from": {"date": booking.date, "trip_id": booking.trip_id},
                 "to": {"date": new_date, "trip_id": new_trip_id}}, now,
            )
            response = self._booking_dict(moved)
            response["replayed"] = False
            self._store_idempotency(conn, idempotency_key, booking.visitor_id,
                                    "reschedule", request, booking_id, response,
                                    now)
            return dict(response)

    # ------------------------------------------------- 船班封存与订单处置
    def seal_trip(self, trip_id: str) -> dict[str, Any]:
        """临时封存船班，按规则处置全部未完成订单。

        迁移优先级：同路线的其它开放船班 → 当日任意有余座的开放船班；
        找不到承接船班时，已付款订单进入退款待办，未付款占位直接取消。
        """
        with self.store.tx() as conn:
            trip = self.store.get_trip(conn, trip_id)
            if trip is None:
                raise DomainError("trip_not_found", f"船班不存在: {trip_id}")
            if trip.status == D.TRIP_SEALED:
                raise DomainError("trip_already_sealed",
                                  f"船班已封存: {trip_id}")
            now = utc_now()
            conn.execute(
                "UPDATE boat_trip SET status=?, updated_at=? WHERE trip_id=?",
                (D.TRIP_SEALED, now, trip_id),
            )
            self.store.append_event(
                conn, D.EVENT_TRIP_SEALED, "trip", trip_id,
                {"trip_id": trip_id, "date": trip.date, "route": trip.route},
                now,
            )

            migrated: list[dict[str, Any]] = []
            refunds: list[dict[str, Any]] = []
            cancelled: list[dict[str, Any]] = []
            for booking in self.store.list_active_bookings_for_trip(conn, trip_id):
                target = self.store.find_migration_target(
                    conn, booking.date, trip.route, trip_id)
                if target is not None:
                    moved = replace(booking, trip_id=target.trip_id,
                                    updated_at=now)
                    self.store.update_booking(conn, moved)
                    self.store.append_event(
                        conn, D.EVENT_BOOKING_MIGRATED, "booking",
                        booking.booking_id,
                        {"booking_id": booking.booking_id,
                         "from_trip_id": trip_id,
                         "to_trip_id": target.trip_id,
                         "same_route": target.route == trip.route,
                         "prior_status": booking.status}, now,
                    )
                    migrated.append({"booking_id": booking.booking_id,
                                     "to_trip_id": target.trip_id,
                                     "status": moved.status})
                    continue

                if booking.status == D.BOOKING_PAID and booking.amount > 0:
                    other_open = conn.execute(
                        "SELECT COUNT(*) AS n FROM boat_trip WHERE date=?"
                        " AND status='open' AND trip_id != ?",
                        (booking.date, trip_id),
                    ).fetchone()["n"]
                    reason = (D.REASON_SEAL_NO_TRIP if other_open == 0
                              else D.REASON_SEAL_NO_CAPACITY)
                    refund_id = f"rf-{uuid.uuid4().hex[:12]}"
                    due = replace(booking, status=D.BOOKING_REFUND_PENDING,
                                  updated_at=now)
                    self.store.update_booking(conn, due)
                    self.store.insert_refund(
                        conn,
                        RefundTask(refund_id=refund_id,
                                   booking_id=booking.booking_id,
                                   amount=booking.amount, reason=reason,
                                   status=D.REFUND_PENDING,
                                   created_at=now, updated_at=now),
                    )
                    self.store.append_event(
                        conn, D.EVENT_BOOKING_REFUND_DUE, "booking",
                        booking.booking_id,
                        {"booking_id": booking.booking_id,
                         "sealed_trip_id": trip_id, "refund_id": refund_id,
                         "amount": booking.amount, "reason": reason}, now,
                    )
                    refunds.append({"booking_id": booking.booking_id,
                                    "refund_id": refund_id,
                                    "amount": booking.amount, "reason": reason})
                else:
                    released = replace(booking, status=D.BOOKING_CANCELLED,
                                       updated_at=now)
                    self.store.update_booking(conn, released)
                    self.store.append_event(
                        conn, D.EVENT_BOOKING_SEAL_CANCELLED, "booking",
                        booking.booking_id,
                        {"booking_id": booking.booking_id,
                         "sealed_trip_id": trip_id,
                         "prior_status": booking.status}, now,
                    )
                    cancelled.append({"booking_id": booking.booking_id})

        return {"trip_id": trip_id, "status": D.TRIP_SEALED,
                "migrated": migrated, "refunds": refunds,
                "cancelled": cancelled}

    # ------------------------------------------------------- 退款待办处理
    def list_refunds(self, status: str | None = None) -> list[dict[str, Any]]:
        return [self._refund_dict(t) for t in self.store.list_refunds(status)]

    def mark_refund_paid(self, refund_id: str,
                         idempotency_key: str | None = None) -> dict[str, Any]:
        """运营确认退款已打款；待办与订单一并终结。"""
        return self._settle_refund(refund_id, D.REFUND_PAID,
                                   D.BOOKING_REFUNDED, D.EVENT_REFUND_MARKED_PAID,
                                   idempotency_key)

    def mark_refund_failed(self, refund_id: str,
                           idempotency_key: str | None = None) -> dict[str, Any]:
        """退款打款失败：待办挂起待重试，订单进入 refund_failed。"""
        return self._settle_refund(refund_id, D.REFUND_FAILED,
                                   D.BOOKING_TERMINAL_FAILURE,
                                   D.EVENT_REFUND_FAILED, idempotency_key)

    def _settle_refund(self, refund_id: str, task_status: str,
                       booking_status: str, event_type: str,
                       idempotency_key: str | None) -> dict[str, Any]:
        request = {"refund_id": refund_id, "outcome": task_status}
        with self.store.tx() as conn:
            if idempotency_key:
                replay = self._replay_if_seen(conn, "refund", idempotency_key,
                                              None, request)
                if replay is not None:
                    return replay
            row = conn.execute(
                "SELECT * FROM refund_task WHERE refund_id=?", (refund_id,)
            ).fetchone()
            if row is None:
                raise DomainError("refund_not_found", f"退款待办不存在: {refund_id}")
            now = utc_now()
            self.store.update_refund_status(conn, refund_id, task_status, now)
            booking = self.store.get_booking_conn(conn, row["booking_id"])
            if booking is not None and booking_status:
                self.store.update_booking(
                    conn, replace(booking, status=booking_status,
                                  updated_at=now))
            self.store.append_event(
                conn, event_type, "refund", refund_id,
                {"refund_id": refund_id, "booking_id": row["booking_id"],
                 "amount": row["amount"], "prior_status": row["status"]}, now,
            )
            response = {**self._refund_dict(
                RefundTask(refund_id=row["refund_id"],
                           booking_id=row["booking_id"], amount=row["amount"],
                           reason=row["reason"], status=task_status,
                           created_at=row["created_at"], updated_at=now)),
                "replayed": False}
            if idempotency_key:
                self._store_idempotency(conn, idempotency_key,
                                        booking.visitor_id if booking else "",
                                        "refund", request, refund_id, response,
                                        now)
            return response

    # ------------------------------------------------------------- 查询
    def get_booking(self, booking_id: str) -> dict[str, Any]:
        booking = self.store.get_booking(booking_id)
        if booking is None:
            raise DomainError("booking_not_found", f"订单不存在: {booking_id}")
        return self._booking_dict(booking)

    def list_events(self, after_seq: int = 0,
                    aggregate_type: str | None = None,
                    aggregate_id: str | None = None) -> list[dict[str, Any]]:
        events = self.store.list_events(after_seq, aggregate_type, aggregate_id)
        return [{"seq": e.seq, "event_type": e.event_type,
                 "aggregate_type": e.aggregate_type,
                 "aggregate_id": e.aggregate_id,
                 "payload": json.loads(e.payload),
                 "created_at": e.created_at} for e in events]

    # ------------------------------------------------------------- 内部工具
    def _replay_if_seen(self, conn, action: str, key: str,
                        visitor_id: str | None,
                        request: dict[str, Any]) -> dict[str, Any] | None:
        existing = self.store.get_idempotency_conn(conn, key)
        if existing is None:
            return None
        if existing.action != action:
            raise DomainError(
                "idempotency_conflict",
                f"幂等键 {key} 已用于动作 {existing.action}，不能用于 {action}",
            )
        if visitor_id is not None and existing.visitor_id != visitor_id:
            raise DomainError(
                "idempotency_conflict",
                f"幂等键 {key} 属于其他游客",
            )
        if existing.request_hash != _request_hash(request):
            raise DomainError(
                "idempotency_conflict",
                f"幂等键 {key} 重放时请求体与首次不一致",
            )
        response = json.loads(existing.response_json)
        response["replayed"] = True
        return response

    def _store_idempotency(self, conn, key: str, visitor_id: str, action: str,
                           request: dict[str, Any], result_ref: str,
                           response: dict[str, Any], now: str) -> None:
        self.store.insert_idempotency(
            conn,
            IdempotencyRecord(
                idempotency_key=key, visitor_id=visitor_id or "", action=action,
                request_hash=_request_hash(request), result_ref=result_ref,
                response_json=json.dumps(response, ensure_ascii=False,
                                         sort_keys=True),
                created_at=now,
            ),
        )

    @staticmethod
    def _booking_dict(booking: Booking) -> dict[str, Any]:
        return {"booking_id": booking.booking_id,
                "visitor_id": booking.visitor_id,
                "variety": booking.variety,
                "variety_name": D.VARIETY_NAMES.get(booking.variety,
                                                    booking.variety),
                "date": booking.date, "trip_id": booking.trip_id,
                "status": booking.status, "amount": booking.amount,
                "idempotency_key": booking.idempotency_key,
                "created_at": booking.created_at,
                "updated_at": booking.updated_at}

    @staticmethod
    def _refund_dict(task: RefundTask) -> dict[str, Any]:
        return {"refund_id": task.refund_id, "booking_id": task.booking_id,
                "amount": task.amount, "reason": task.reason,
                "status": task.status, "created_at": task.created_at,
                "updated_at": task.updated_at}
