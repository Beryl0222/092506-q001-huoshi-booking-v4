"""火柿水上采摘调度的应用服务。

所有写操作都在一个 ``BEGIN IMMEDIATE`` 事务内完成「校验 → 查库存 → 写订单 →
记事件」，配合订单表上的部分唯一索引，保证：

* 同一游客的并发重复请求最多产生一笔未完结订单（不会重复扣减）；
* 树区每日名额与船班载客都不会超卖；
* 每个状态迁移都在同一事务里落一条 ``event_log``，事件顺序即提交顺序，
  服务重启后仍可按 seq 重放追溯。

带幂等键的接口在 ``idempotency`` 表中保存「请求指纹 + 首次响应」，
重复回调直接返回首次结果；键被复用于不同请求则报冲突。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import timedelta

from .domain import (
    ACTIVE_ORDER_STATES,
    DEFAULT_PAYMENT_TIMEOUT_MINUTES,
    ORDER_CANCELLED,
    ORDER_CONFIRMED,
    ORDER_MIGRATED,
    ORDER_PENDING,
    ORDER_REFUNDED,
    ORDER_RESCHEDULED,
    REFUND_DONE,
    REFUND_PENDING,
    VARIETIES,
    BoatTrip,
    DomainError,
    IdempotencyConflict,
    NotFoundError,
    Order,
    RefundTodo,
    TreeZone,
    now_iso,
)
from .store import Store

KIND_BOOK = "book"
KIND_PAYMENT = "payment_confirm"
KIND_CANCEL = "cancel"
KIND_RESCHEDULE = "reschedule"


class Service:
    def __init__(self, store: Store | None = None, clock=now_iso) -> None:
        self.store = store or Store()
        self.clock = clock

    @contextmanager
    def _tx(self):
        """事务边界：把唯一约束/版本冲突统一翻译为业务错误。"""
        try:
            with self.store.tx() as conn:
                yield conn
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if "uq_active_order" in message:
                raise DomainError("该游客对此船班已有未完结订单，请勿重复提交")
            raise DomainError(f"数据约束冲突：{message}") from exc

    # -- 杂项 ----------------------------------------------------------------

    def health(self) -> dict[str, str]:
        return {"service": "huoshi_booking", "status": "ok"}

    def register(self, record_id: str, owner_id: str) -> dict[str, str]:
        from .domain import Record

        record = self.store.save(Record(record_id, owner_id))
        return {"record_id": record.record_id, "owner_id": record.owner_id,
                "state": record.state, "created_at": record.created_at}

    def find(self, record_id: str) -> dict[str, str] | None:
        record = self.store.get(record_id)
        return record.__dict__.copy() if record else None

    # -- 运营维护：树区与船班 -------------------------------------------------

    def upsert_zone(self, zone_id: str, variety: str, name: str,
                    daily_capacity: int, unit_price_fen: int = 0) -> dict:
        if variety not in VARIETIES:
            raise DomainError(f"品种必须是 {VARIETIES} 之一")
        if daily_capacity < 0:
            raise DomainError("树区容量不能为负")
        if unit_price_fen < 0:
            raise DomainError("单价不能为负")
        zone = TreeZone(zone_id=zone_id, variety=variety, name=name,
                        daily_capacity=int(daily_capacity),
                        unit_price_fen=int(unit_price_fen))
        with self._tx() as conn:
            self.store.upsert_zone_conn(conn, zone, self.clock())
            self.store.append_event_conn(
                conn, "zone.upserted", "", zone_id,
                {"zone_id": zone_id, "variety": variety, "name": name,
                 "daily_capacity": int(daily_capacity),
                 "unit_price_fen": int(unit_price_fen)},
                self.clock(),
            )
        return {
            "zone_id": zone.zone_id, "variety": zone.variety, "name": zone.name,
            "daily_capacity": zone.daily_capacity,
            "unit_price_fen": zone.unit_price_fen,
        }

    def list_zones(self, variety: str | None = None) -> list[dict]:
        return [z.__dict__.copy() for z in self.store.list_zones(variety)]

    def upsert_trip(self, trip_id: str, boat_route: str, pick_date: str,
                    time_slot: str, capacity: int) -> dict:
        if capacity <= 0:
            raise DomainError("船班载客量必须为正数")
        self._validate_date(pick_date)
        trip = BoatTrip(trip_id=trip_id, boat_route=boat_route,
                        pick_date=pick_date, time_slot=time_slot,
                        capacity=int(capacity))
        ts = self.clock()
        with self._tx() as conn:
            self.store.upsert_trip_conn(conn, trip, ts)
            self.store.append_event_conn(
                conn, "trip.upserted", "", trip_id,
                {"trip_id": trip_id, "boat_route": boat_route,
                 "pick_date": pick_date, "time_slot": time_slot,
                 "capacity": int(capacity)},
                ts)
        return {
            "trip_id": trip.trip_id, "boat_route": trip.boat_route,
            "pick_date": trip.pick_date, "time_slot": trip.time_slot,
            "capacity": trip.capacity, "status": trip.status,
        }

    def list_trips(self, pick_date: str | None = None,
                   status: str | None = None) -> list[dict]:
        return [t.__dict__.copy() for t in self.store.list_trips(pick_date, status)]

    def inventory(self, pick_date: str,
                  variety: str | None = None) -> dict:
        return self.store.inventory_snapshot(pick_date, variety)

    # -- 预订 ----------------------------------------------------------------

    def book(self, visitor_id: str, variety: str, zone_id: str, pick_date: str,
             trip_id: str, quantity: int, idempotency_key: str,
             unit_price_fen: int | None = None) -> dict:
        if not visitor_id:
            raise DomainError("缺少 visitor_id")
        if not idempotency_key:
            raise DomainError("预订必须携带 idempotency_key")
        if variety not in VARIETIES:
            raise DomainError(f"品种必须是 {VARIETIES} 之一")
        if not isinstance(quantity, int) or quantity <= 0:
            raise DomainError("数量必须为正整数")
        self._validate_date(pick_date)

        fingerprint = json.dumps(
            {"kind": KIND_BOOK, "visitor_id": visitor_id, "variety": variety,
             "zone_id": zone_id, "pick_date": pick_date, "trip_id": trip_id,
             "quantity": quantity},
            ensure_ascii=False, sort_keys=True,
        )
        ts = self.clock()
        order_id = f"O-{uuid.uuid4().hex[:12]}"

        with self._tx() as conn:
            cached = self._replay_idempotent(
                conn, idempotency_key, visitor_id, KIND_BOOK, fingerprint)
            if cached is not None:
                return cached

            zone = self.store.get_zone(zone_id)
            if zone is None:
                raise NotFoundError(f"树区 {zone_id} 不存在")
            if zone.variety != variety:
                raise DomainError(
                    f"树区 {zone_id} 品种为 {zone.variety}，与 {variety} 不符")
            trip = self.store.get_trip(trip_id)
            if trip is None:
                raise NotFoundError(f"船班 {trip_id} 不存在")
            if trip.pick_date != pick_date:
                raise DomainError(
                    f"船班 {trip_id} 日期为 {trip.pick_date}，与 {pick_date} 不符")
            if trip.status != "active":
                raise DomainError(f"船班 {trip_id} 已封存，无法预订")

            existing = self.store.find_active_order_conn(
                conn, visitor_id, variety, pick_date, trip_id)
            if existing is not None:
                raise DomainError(
                    f"游客已有未完结订单 {existing.order_id}，请勿重复提交")

            zone_used = self.store.zone_reserved_conn(conn, zone_id, pick_date)
            if zone_used + quantity > zone.daily_capacity:
                raise DomainError(
                    f"树区 {zone_id} {pick_date} 名额不足："
                    f"剩 {zone.daily_capacity - zone_used}，需 {quantity}")
            boat_used = self.store.trip_reserved_conn(conn, trip_id)
            if boat_used + quantity > trip.capacity:
                raise DomainError(
                    f"船班 {trip_id} 载客不足：剩 {trip.capacity - boat_used}，"
                    f"需 {quantity}")

            price = (zone.unit_price_fen if unit_price_fen is None
                     else int(unit_price_fen))
            order = Order(
                order_id=order_id, visitor_id=visitor_id, variety=variety,
                zone_id=zone_id, pick_date=pick_date, trip_id=trip_id,
                quantity=quantity, state=ORDER_PENDING,
                amount_fen=price * quantity,
                idempotency_key=idempotency_key, version=0,
                created_at=ts, updated_at=ts,
            )
            self.store.insert_order(order)
            self.store.append_event_conn(
                conn, "order.created", order_id, trip_id,
                {"order_id": order_id, "visitor_id": visitor_id,
                 "variety": variety, "zone_id": zone_id,
                 "pick_date": pick_date, "trip_id": trip_id,
                 "quantity": quantity, "state": ORDER_PENDING,
                 "amount_fen": order.amount_fen},
                ts,
            )
            response = self._order_dict(order)
            response["idempotent_replay"] = False
            self.store.put_idempotency_conn(
                conn, idempotency_key, visitor_id, KIND_BOOK, fingerprint,
                order_id, json.dumps(response, ensure_ascii=False), ts)
            return response

    # -- 付款确认（回调可能重复） ---------------------------------------------

    def confirm_payment(self, order_id: str, idempotency_key: str,
                        payment_ref: str = "") -> dict:
        if not idempotency_key:
            raise DomainError("付款确认必须携带 idempotency_key")
        fingerprint = json.dumps(
            {"kind": KIND_PAYMENT, "order_id": order_id},
            sort_keys=True)
        ts = self.clock()

        with self._tx() as conn:
            cached = self._replay_idempotent(
                conn, idempotency_key, "", KIND_PAYMENT, fingerprint)
            if cached is not None:
                return cached

            order = self.store.get_order_conn(conn, order_id)
            if order is None:
                raise NotFoundError(f"订单 {order_id} 不存在")

            if order.state == ORDER_CONFIRMED:
                # 未带同一幂等键的重复回调：安全地返回当前状态，不重复记账。
                response = self._order_dict(order)
                response["idempotent_replay"] = True
                response["note"] = "订单此前已确认付款"
                self.store.put_idempotency_conn(
                    conn, idempotency_key, order.visitor_id, KIND_PAYMENT,
                    fingerprint, order_id,
                    json.dumps(response, ensure_ascii=False), ts)
                return response

            if order.state != ORDER_PENDING:
                raise DomainError(
                    f"订单 {order_id} 当前状态 {order.state}，不可确认付款")

            self.store.update_order_state_conn(
                conn, order_id, ORDER_CONFIRMED, order.version, ts)
            self.store.append_event_conn(
                conn, "payment.confirmed", order_id, order.trip_id,
                {"order_id": order_id, "payment_ref": payment_ref,
                 "amount_fen": order.amount_fen},
                ts,
            )
            new_order = self.store.get_order_conn(conn, order_id)
            response = self._order_dict(new_order)
            response["idempotent_replay"] = False
            self.store.put_idempotency_conn(
                conn, idempotency_key, order.visitor_id, KIND_PAYMENT,
                fingerprint, order_id,
                json.dumps(response, ensure_ascii=False), ts)
            return response

    # -- 取消 ----------------------------------------------------------------

    def cancel_order(self, order_id: str, idempotency_key: str,
                     reason: str = "visitor_cancelled") -> dict:
        if not idempotency_key:
            raise DomainError("取消必须携带 idempotency_key")
        fingerprint = json.dumps(
            {"kind": KIND_CANCEL, "order_id": order_id, "reason": reason},
            ensure_ascii=False, sort_keys=True)
        ts = self.clock()

        with self._tx() as conn:
            cached = self._replay_idempotent(
                conn, idempotency_key, "", KIND_CANCEL, fingerprint)
            if cached is not None:
                return cached

            order = self.store.get_order_conn(conn, order_id)
            if order is None:
                raise NotFoundError(f"订单 {order_id} 不存在")
            if order.state not in ACTIVE_ORDER_STATES:
                raise DomainError(
                    f"订单 {order_id} 当前状态 {order.state}，不可取消")

            self.store.update_order_state_conn(
                conn, order_id, ORDER_CANCELLED, order.version, ts)
            payload = {"order_id": order_id, "reason": reason,
                       "quantity": order.quantity}
            refund_id = ""
            # 已付款订单取消：钱要退，生成退款待办。
            if order.state == ORDER_CONFIRMED:
                refund_id = f"R-{uuid.uuid4().hex[:12]}"
                self.store.insert_refund_conn(conn, RefundTodo(
                    refund_id=refund_id, order_id=order_id,
                    visitor_id=order.visitor_id, amount_fen=order.amount_fen,
                    reason=f"cancel:{reason}", status=REFUND_PENDING,
                    created_at=ts))
                payload["refund_id"] = refund_id
            self.store.append_event_conn(
                conn, "order.cancelled", order_id, order.trip_id, payload, ts)

            new_order = self.store.get_order_conn(conn, order_id)
            response = self._order_dict(new_order)
            if refund_id:
                response["refund_id"] = refund_id
            response["idempotent_replay"] = False
            self.store.put_idempotency_conn(
                conn, idempotency_key, order.visitor_id, KIND_CANCEL,
                fingerprint, order_id,
                json.dumps(response, ensure_ascii=False), ts)
            return response

    # -- 改期（支持跨日） -----------------------------------------------------

    def reschedule(self, order_id: str, new_pick_date: str,
                   new_trip_id: str, idempotency_key: str) -> dict:
        if not idempotency_key:
            raise DomainError("改期必须携带 idempotency_key")
        self._validate_date(new_pick_date)
        fingerprint = json.dumps(
            {"kind": KIND_RESCHEDULE, "order_id": order_id,
             "new_pick_date": new_pick_date, "new_trip_id": new_trip_id},
            ensure_ascii=False, sort_keys=True)
        ts = self.clock()

        with self._tx() as conn:
            cached = self._replay_idempotent(
                conn, idempotency_key, "", KIND_RESCHEDULE, fingerprint)
            if cached is not None:
                return cached

            order = self.store.get_order_conn(conn, order_id)
            if order is None:
                raise NotFoundError(f"订单 {order_id} 不存在")
            if order.state not in ACTIVE_ORDER_STATES:
                raise DomainError(
                    f"订单 {order_id} 当前状态 {order.state}，不可改期")

            new_trip = self.store.get_trip(new_trip_id)
            if new_trip is None:
                raise NotFoundError(f"船班 {new_trip_id} 不存在")
            if new_trip.pick_date != new_pick_date:
                raise DomainError(
                    f"船班 {new_trip_id} 日期为 {new_trip.pick_date}，"
                    f"与 {new_pick_date} 不符")
            if new_trip.status != "active":
                raise DomainError(f"船班 {new_trip_id} 已封存，无法改入")
            if new_trip_id == order.trip_id and new_pick_date == order.pick_date:
                raise DomainError("新旧船班/日期相同，无需改期")

            dup = self.store.find_active_order_conn(
                conn, order.visitor_id, order.variety, new_pick_date, new_trip_id)
            if dup is not None:
                raise DomainError(
                    f"游客在目标船班已有未完结订单 {dup.order_id}")

            self._ensure_capacity(conn, order.zone_id, new_pick_date,
                                  new_trip_id, order.quantity,
                                  exclude_order_id=order_id)

            new_id = self._next_derived_id(conn, order_id, "R")
            new_order = Order(
                order_id=new_id, visitor_id=order.visitor_id,
                variety=order.variety, zone_id=order.zone_id,
                pick_date=new_pick_date, trip_id=new_trip_id,
                quantity=order.quantity, state=order.state,
                amount_fen=order.amount_fen,
                idempotency_key=idempotency_key, version=0,
                created_at=ts, updated_at=ts,
            )
            self.store.insert_order(new_order)
            self.store.update_order_state_conn(
                conn, order_id, ORDER_RESCHEDULED, order.version, ts,
                replaced_by=new_id)
            self.store.append_event_conn(
                conn, "order.rescheduled", order_id, order.trip_id,
                {"order_id": order_id, "new_order_id": new_id,
                 "old_pick_date": order.pick_date, "old_trip_id": order.trip_id,
                 "new_pick_date": new_pick_date, "new_trip_id": new_trip_id},
                ts)
            self.store.append_event_conn(
                conn, "order.created", new_id, new_trip_id,
                {"order_id": new_id, "visitor_id": order.visitor_id,
                 "variety": order.variety, "zone_id": order.zone_id,
                 "pick_date": new_pick_date, "trip_id": new_trip_id,
                 "quantity": order.quantity, "state": new_order.state,
                 "amount_fen": new_order.amount_fen,
                 "rescheduled_from": order_id},
                ts)
            response = self._order_dict(new_order)
            response["rescheduled_from"] = order_id
            response["idempotent_replay"] = False
            self.store.put_idempotency_conn(
                conn, idempotency_key, order.visitor_id, KIND_RESCHEDULE,
                fingerprint, new_id,
                json.dumps(response, ensure_ascii=False), ts)
            return response

    # -- 船班封存：退回或迁移 -----------------------------------------------

    def seal_trip(self, trip_id: str, reason: str, mode: str = "refund",
                  target_trip_id: str = "",
                  target_pick_date: str = "") -> dict:
        """封存船班。

        mode="refund"：未付款订单直接取消；已付款订单置为 refunded 并生成退款待办。
        mode="migrate"：按时间顺序把订单迁到目标船班（或同路线其他活跃船班），
                        容量不够的订单按 refund 规则兜底。
        """
        if mode not in ("refund", "migrate"):
            raise DomainError("mode 只能是 refund 或 migrate")
        ts = self.clock()

        with self._tx() as conn:
            trip = self.store.get_trip(trip_id)
            if trip is None:
                raise NotFoundError(f"船班 {trip_id} 不存在")
            if trip.status == "sealed":
                return {"trip_id": trip_id, "already_sealed": True,
                        "sealed_at": trip.sealed_at,
                        "migrated": [], "refunded": [], "cancelled": []}

            orders = self.store.active_orders_of_trip_conn(conn, trip_id)
            self.store.seal_trip_conn(conn, trip_id, reason, ts)
            self.store.append_event_conn(
                conn, "trip.sealed", "", trip_id,
                {"trip_id": trip_id, "reason": reason, "mode": mode,
                 "affected": len(orders)},
                ts)

            migrated, refunded, cancelled = [], [], []

            if mode == "refund":
                for order in orders:
                    self._seal_refund_or_cancel(conn, order, reason, ts,
                                                refunded, cancelled)
            else:
                targets = self._migration_targets(
                    conn, trip, target_trip_id, target_pick_date)
                for order in orders:
                    placed = self._try_migrate(conn, order, trip, targets, ts,
                                               migrated)
                    if not placed:
                        self._seal_refund_or_cancel(conn, order, reason, ts,
                                                    refunded, cancelled)

            return {"trip_id": trip_id, "sealed_at": ts, "mode": mode,
                    "migrated": migrated, "refunded": refunded,
                    "cancelled": cancelled}

    def _seal_refund_or_cancel(self, conn, order: Order, reason: str, ts: str,
                               refunded: list, cancelled: list) -> None:
        if order.state == ORDER_CONFIRMED:
            refund_id = f"R-{uuid.uuid4().hex[:12]}"
            self.store.insert_refund_conn(conn, RefundTodo(
                refund_id=refund_id, order_id=order.order_id,
                visitor_id=order.visitor_id, amount_fen=order.amount_fen,
                reason=f"seal:{order.trip_id}:{reason}",
                status=REFUND_PENDING, created_at=ts))
            self.store.update_order_state_conn(
                conn, order.order_id, ORDER_REFUNDED, order.version, ts)
            self.store.append_event_conn(
                conn, "order.refunded", order.order_id, order.trip_id,
                {"order_id": order.order_id, "refund_id": refund_id,
                 "amount_fen": order.amount_fen, "reason": reason,
                 "sealed_trip_id": order.trip_id},
                ts)
            refunded.append({"order_id": order.order_id,
                             "refund_id": refund_id,
                             "amount_fen": order.amount_fen})
        else:
            self.store.update_order_state_conn(
                conn, order.order_id, ORDER_CANCELLED, order.version, ts)
            self.store.append_event_conn(
                conn, "order.cancelled", order.order_id, order.trip_id,
                {"order_id": order.order_id, "reason": f"seal:{reason}",
                 "sealed_trip_id": order.trip_id, "quantity": order.quantity},
                ts)
            cancelled.append({"order_id": order.order_id})

    def _migration_targets(self, conn, trip: BoatTrip,
                           target_trip_id: str,
                           target_pick_date: str) -> list[BoatTrip]:
        if target_trip_id:
            target = self.store.get_trip(target_trip_id)
            if target is None:
                raise NotFoundError(f"目标船班 {target_trip_id} 不存在")
            if target.status != "active":
                raise DomainError(f"目标船班 {target_trip_id} 已封存")
            if target_pick_date and target.pick_date != target_pick_date:
                raise DomainError("目标船班日期与 target_pick_date 不符")
            return [target]
        # 自动规则：优先同路线，按日期（同日优先）、时段从早到晚。
        pick_date = target_pick_date or trip.pick_date
        same_route = [
            t for t in self.store.list_active_trips_on_date_conn(
                conn, pick_date, boat_route=trip.boat_route,
                exclude_trip_id=trip.trip_id)
        ]
        other_route = [
            t for t in self.store.list_active_trips_on_date_conn(
                conn, pick_date, boat_route=None,
                exclude_trip_id=trip.trip_id)
            if t.boat_route != trip.boat_route
        ]
        return same_route + other_route

    def _try_migrate(self, conn, order: Order, sealed_trip: BoatTrip,
                     targets: list[BoatTrip], ts: str,
                     migrated: list) -> bool:
        for target in targets:
            # 该游客在目标船班已有未完结订单时不能迁过去（唯一约束），
            # 换下一个候选船班。
            if self.store.find_active_order_conn(
                    conn, order.visitor_id, order.variety,
                    target.pick_date, target.trip_id) is not None:
                continue
            try:
                self._ensure_capacity(conn, order.zone_id, target.pick_date,
                                      target.trip_id, order.quantity,
                                      exclude_order_id=order.order_id)
            except DomainError:
                continue
            new_id = self._next_derived_id(conn, order.order_id, "M")
            new_order = Order(
                order_id=new_id, visitor_id=order.visitor_id,
                variety=order.variety, zone_id=order.zone_id,
                pick_date=target.pick_date, trip_id=target.trip_id,
                quantity=order.quantity, state=order.state,
                amount_fen=order.amount_fen,
                idempotency_key="", version=0,
                created_at=ts, updated_at=ts,
            )
            self.store.insert_order(new_order)
            self.store.update_order_state_conn(
                conn, order.order_id, ORDER_MIGRATED, order.version, ts,
                replaced_by=new_id)
            self.store.append_event_conn(
                conn, "order.migrated", order.order_id, sealed_trip.trip_id,
                {"order_id": order.order_id, "new_order_id": new_id,
                 "sealed_trip_id": sealed_trip.trip_id,
                 "new_trip_id": target.trip_id,
                 "new_pick_date": target.pick_date,
                 "quantity": order.quantity},
                ts)
            self.store.append_event_conn(
                conn, "order.created", new_id, target.trip_id,
                {"order_id": new_id, "visitor_id": order.visitor_id,
                 "variety": order.variety, "zone_id": order.zone_id,
                 "pick_date": target.pick_date, "trip_id": target.trip_id,
                 "quantity": order.quantity, "state": new_order.state,
                 "amount_fen": new_order.amount_fen,
                 "migrated_from": order.order_id},
                ts)
            migrated.append({"order_id": order.order_id,
                             "new_order_id": new_id,
                             "new_trip_id": target.trip_id,
                             "new_pick_date": target.pick_date})
            return True
        return False

    # -- 退款待办 -------------------------------------------------------------

    def list_refunds(self, status: str = "") -> list[dict]:
        return [r.__dict__.copy() for r in self.store.list_refunds(status)]

    def complete_refund(self, refund_id: str) -> dict:
        ts = self.clock()
        with self._tx() as conn:
            refund = self.store.get_refund_conn(conn, refund_id)
            if refund is None:
                raise NotFoundError(f"退款待办 {refund_id} 不存在")
            if refund.status == REFUND_DONE:
                return {"refund_id": refund_id, "already_done": True,
                        "handled_at": refund.handled_at}
            self.store.mark_refund_done_conn(conn, refund_id, ts)
            self.store.append_event_conn(
                conn, "refund.completed", refund.order_id, "",
                {"refund_id": refund_id, "order_id": refund.order_id,
                 "amount_fen": refund.amount_fen},
                ts)
            return {"refund_id": refund_id, "status": REFUND_DONE,
                    "handled_at": ts}

    # -- 付款超时 -------------------------------------------------------------

    def sweep_expired(self, timeout_minutes: int | None = None) -> dict:
        """取消超过保留时长仍未付款的订单，释放名额。"""
        if timeout_minutes is None:
            timeout_minutes = int(self.store.get_config(
                "payment_timeout_minutes",
                str(DEFAULT_PAYMENT_TIMEOUT_MINUTES)))
        from datetime import datetime
        now = datetime.fromisoformat(self.clock())
        deadline = (now - timedelta(minutes=timeout_minutes)).isoformat()
        cancelled = []
        with self._tx() as conn:
            pending = self.store.list_expired_pending_conn(conn, deadline)
            for order in pending:
                self.store.update_order_state_conn(
                    conn, order.order_id, ORDER_CANCELLED, order.version,
                    self.clock())
                self.store.append_event_conn(
                    conn, "order.cancelled", order.order_id, order.trip_id,
                    {"order_id": order.order_id,
                     "reason": "payment_timeout",
                     "quantity": order.quantity},
                    self.clock())
                cancelled.append(order.order_id)
        return {"cancelled": cancelled, "deadline": deadline}

    # -- 事件 ----------------------------------------------------------------

    def list_events(self, order_id: str = "", trip_id: str = "",
                    after_seq: int = 0, limit: int = 1000) -> list[dict]:
        events = self.store.list_events(order_id, trip_id, after_seq, limit)
        result = []
        for e in events:
            result.append({
                "seq": e.seq, "event_type": e.event_type,
                "order_id": e.order_id, "trip_id": e.trip_id,
                "payload": json.loads(e.payload), "created_at": e.created_at,
            })
        return result

    def get_order(self, order_id: str) -> dict:
        order = self.store.get_order(order_id)
        if order is None:
            raise NotFoundError(f"订单 {order_id} 不存在")
        return self._order_dict(order)

    # -- 内部辅助 -------------------------------------------------------------

    def _replay_idempotent(self, conn, key: str, visitor_id: str,
                           request_kind: str, fingerprint: str):
        cached = self.store.get_idempotency_conn(conn, key)
        if cached is None:
            return None
        if (cached.request_kind != request_kind
                or cached.request_fingerprint != fingerprint
                or (visitor_id and cached.visitor_id
                    and cached.visitor_id != visitor_id)):
            raise IdempotencyConflict(
                f"幂等键 {key} 已用于其他请求，拒绝复用")
        response = json.loads(cached.response_json)
        response["idempotent_replay"] = True
        # 顺带反映订单当前状态（可能已被取消/确认）。
        if cached.order_id:
            current = self.store.get_order_conn(conn, cached.order_id)
            if current is not None:
                response["current_state"] = current.state
                response["version"] = current.version
        return response

    def _ensure_capacity(self, conn, zone_id: str, pick_date: str,
                         trip_id: str, quantity: int,
                         exclude_order_id: str = "") -> None:
        zone = self.store.get_zone(zone_id)
        if zone is None:
            raise NotFoundError(f"树区 {zone_id} 不存在")
        trip = self.store.get_trip(trip_id)
        if trip is None:
            raise NotFoundError(f"船班 {trip_id} 不存在")
        zone_used = self.store.zone_reserved_conn(
            conn, zone_id, pick_date, exclude_order_id=exclude_order_id)
        if zone_used + quantity > zone.daily_capacity:
            raise DomainError(
                f"树区 {zone_id} {pick_date} 名额不足")
        boat_used = self.store.trip_reserved_conn(
            conn, trip_id, exclude_order_id=exclude_order_id)
        if boat_used + quantity > trip.capacity:
            raise DomainError(f"船班 {trip_id} 载客不足")

    @staticmethod
    def _next_derived_id(conn, order_id: str, marker: str) -> str:
        """生成 O-xxx-R1 / -R2、-M1 形式的可追溯派生单号。"""
        n = conn.execute(
            "SELECT COUNT(*) AS c FROM orders WHERE order_id LIKE ?",
            (f"{order_id}-{marker}%",),
        ).fetchone()["c"]
        return f"{order_id}-{marker}{n + 1}"

    @staticmethod
    def _validate_date(pick_date: str) -> None:
        from datetime import date
        try:
            date.fromisoformat(pick_date)
        except (ValueError, TypeError):
            raise DomainError(f"日期格式应为 YYYY-MM-DD：{pick_date!r}")

    @staticmethod
    def _order_dict(order: Order) -> dict:
        return {
            "order_id": order.order_id,
            "visitor_id": order.visitor_id,
            "variety": order.variety,
            "zone_id": order.zone_id,
            "pick_date": order.pick_date,
            "trip_id": order.trip_id,
            "quantity": order.quantity,
            "state": order.state,
            "amount_fen": order.amount_fen,
            "version": order.version,
            "replaced_by": order.replaced_by,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
        }
