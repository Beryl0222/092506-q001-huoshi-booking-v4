"""船班与采摘名额的本地持久化边界。

并发策略:
- 连接以 autocommit 模式打开，所有写流程显式执行 ``BEGIN IMMEDIATE``，
  配合 ``busy_timeout`` 让跨线程/跨连接的写事务在 SQLite 层串行化，
  容量校验与扣减在同一个事务内完成，杜绝超卖。
- 事件记录与业务状态在同一事务内写入（事务性发件箱），
  服务重启后状态与事件顺序必然一致。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .domain import (
    ACTIVE_BOOKING_STATES,
    REFUND_PENDING,
    TRIP_OPEN,
    Booking,
    Event,
    IdempotencyRecord,
    Record,
    RefundTask,
    TreeZone,
    BoatTrip,
)

ACTIVE_STATE_LIST = ", ".join(f"'{s}'" for s in ACTIVE_BOOKING_STATES)


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(
            str(path), check_same_thread=False, isolation_level=None, timeout=10
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=10000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        # 单连接被多线程共享时，用锁把事务串起来；多连接则由 BEGIN IMMEDIATE 串行。
        self._lock = threading.RLock()
        self._init_schema()

    # ------------------------------------------------------------------ 基础
    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """开启一个立即写事务；提交/回滚成对出现，不可嵌套。"""
        conn = self.connection
        with self._lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def _init_schema(self) -> None:
        with self._lock:
            self.connection.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS boat_slot (
                    record_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tree_zone (
                    zone_id TEXT PRIMARY KEY,
                    variety TEXT NOT NULL,
                    name TEXT NOT NULL,
                    daily_capacity INTEGER NOT NULL CHECK (daily_capacity >= 0),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS boat_trip (
                    trip_id TEXT PRIMARY KEY,
                    date TEXT NOT NULL,
                    route TEXT NOT NULL,
                    seat_capacity INTEGER NOT NULL CHECK (seat_capacity >= 0),
                    status TEXT NOT NULL DEFAULT '{TRIP_OPEN}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_boat_trip_date ON boat_trip(date);

                CREATE TABLE IF NOT EXISTS booking (
                    booking_id TEXT PRIMARY KEY,
                    visitor_id TEXT NOT NULL,
                    variety TEXT NOT NULL,
                    date TEXT NOT NULL,
                    trip_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    amount INTEGER NOT NULL CHECK (amount >= 0),
                    idempotency_key TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_booking_capacity
                    ON booking(variety, date, status);
                CREATE INDEX IF NOT EXISTS idx_booking_trip
                    ON booking(trip_id, status);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_booking_active_visitor
                    ON booking(visitor_id, variety, date, trip_id)
                    WHERE status IN ({ACTIVE_STATE_LIST});

                CREATE TABLE IF NOT EXISTS payment (
                    payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    booking_id TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    idempotency_key TEXT UNIQUE,
                    paid_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS idempotency_record (
                    idempotency_key TEXT PRIMARY KEY,
                    visitor_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    result_ref TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS event_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_event_aggregate
                    ON event_log(aggregate_type, aggregate_id, seq);

                CREATE TABLE IF NOT EXISTS refund_task (
                    refund_id TEXT PRIMARY KEY,
                    booking_id TEXT NOT NULL,
                    amount INTEGER NOT NULL CHECK (amount > 0),
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT '{REFUND_PENDING}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_refund_status ON refund_task(status);
                """
            )

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    # ----------------------------------------------------------- 基线登记
    def save_record(self, record: Record) -> Record:
        value = record.with_timestamp()
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO boat_slot(record_id, owner_id, state, created_at)"
                " VALUES(?,?,?,?)",
                (value.record_id, value.owner_id, value.state, value.created_at),
            )
        return value

    def get_record(self, record_id: str) -> Record | None:
        row = self.connection.execute(
            "SELECT record_id, owner_id, state, created_at FROM boat_slot"
            " WHERE record_id=?",
            (record_id,),
        ).fetchone()
        return Record(**dict(row)) if row else None

    # ------------------------------------------------------------- 树区
    def upsert_zone(self, conn: sqlite3.Connection, zone: TreeZone) -> None:
        conn.execute(
            "INSERT INTO tree_zone(zone_id, variety, name, daily_capacity,"
            " created_at, updated_at) VALUES(?,?,?,?,?,?)"
            " ON CONFLICT(zone_id) DO UPDATE SET"
            " variety=excluded.variety, name=excluded.name,"
            " daily_capacity=excluded.daily_capacity, updated_at=excluded.updated_at",
            (zone.zone_id, zone.variety, zone.name, zone.daily_capacity,
             zone.created_at, zone.updated_at),
        )

    def get_zone(self, conn: sqlite3.Connection, zone_id: str) -> TreeZone | None:
        row = conn.execute(
            "SELECT * FROM tree_zone WHERE zone_id=?", (zone_id,)
        ).fetchone()
        return _row_to_zone(row) if row else None

    def list_zones(self) -> list[TreeZone]:
        rows = self.connection.execute(
            "SELECT * FROM tree_zone ORDER BY zone_id"
        ).fetchall()
        return [_row_to_zone(r) for r in rows]

    def zone_capacity(self, conn: sqlite3.Connection, variety: str, date: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(SUM(daily_capacity), 0) AS cap FROM tree_zone"
            " WHERE variety=?",
            (variety,),
        ).fetchone()
        return int(row["cap"])

    # ------------------------------------------------------------- 船班
    def insert_trip(self, conn: sqlite3.Connection, trip: BoatTrip) -> None:
        conn.execute(
            "INSERT INTO boat_trip(trip_id, date, route, seat_capacity, status,"
            " created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
            (trip.trip_id, trip.date, trip.route, trip.seat_capacity,
             trip.status, trip.created_at, trip.updated_at),
        )

    def get_trip_connless(self, trip_id: str) -> BoatTrip | None:
        return self.get_trip(self.connection, trip_id)

    def get_trip(self, conn: sqlite3.Connection, trip_id: str) -> BoatTrip | None:
        row = conn.execute(
            "SELECT * FROM boat_trip WHERE trip_id=?", (trip_id,)
        ).fetchone()
        return _row_to_trip(row) if row else None

    def list_trips(self, date: str | None = None) -> list[BoatTrip]:
        if date is None:
            rows = self.connection.execute(
                "SELECT * FROM boat_trip ORDER BY date, trip_id"
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM boat_trip WHERE date=? ORDER BY trip_id", (date,)
            ).fetchall()
        return [_row_to_trip(r) for r in rows]

    def find_migration_target(
        self, conn: sqlite3.Connection, date: str, route: str,
        exclude_trip_id: str, party_size: int = 1,
    ) -> BoatTrip | None:
        """封存迁移目标：优先同路线，其次当日任意开放船班，要求有余座。"""
        rows = conn.execute(
            "SELECT * FROM boat_trip WHERE date=? AND status='open'"
            " AND trip_id != ? ORDER BY (route != ?), trip_id",
            (date, exclude_trip_id, route),
        ).fetchall()
        for row in rows:
            trip = _row_to_trip(row)
            if trip.seat_capacity - self.trip_held(conn, trip.trip_id) >= party_size:
                return trip
        return None

    # ------------------------------------------------------------- 容量
    def capacity_held(self, conn: sqlite3.Connection, variety: str, date: str,
                      exclude_booking_id: str | None = None) -> int:
        sql = (
            "SELECT COUNT(*) AS n FROM booking"
            f" WHERE variety=? AND date=? AND status IN ({ACTIVE_STATE_LIST})"
        )
        params: list[Any] = [variety, date]
        if exclude_booking_id:
            sql += " AND booking_id != ?"
            params.append(exclude_booking_id)
        return int(conn.execute(sql, params).fetchone()["n"])

    def trip_held(self, conn: sqlite3.Connection, trip_id: str,
                  exclude_booking_id: str | None = None) -> int:
        sql = (
            "SELECT COUNT(*) AS n FROM booking"
            f" WHERE trip_id=? AND status IN ({ACTIVE_STATE_LIST})"
        )
        params: list[Any] = [trip_id]
        if exclude_booking_id:
            sql += " AND booking_id != ?"
            params.append(exclude_booking_id)
        return int(conn.execute(sql, params).fetchone()["n"])

    # ------------------------------------------------------------- 订单
    def insert_booking(self, conn: sqlite3.Connection, booking: Booking) -> None:
        conn.execute(
            "INSERT INTO booking(booking_id, visitor_id, variety, date, trip_id,"
            " status, amount, idempotency_key, created_at, updated_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (booking.booking_id, booking.visitor_id, booking.variety, booking.date,
             booking.trip_id, booking.status, booking.amount,
             booking.idempotency_key, booking.created_at, booking.updated_at),
        )

    def get_booking(self, booking_id: str) -> Booking | None:
        return self.get_booking_conn(self.connection, booking_id)

    def get_booking_conn(self, conn: sqlite3.Connection,
                         booking_id: str) -> Booking | None:
        row = conn.execute(
            "SELECT * FROM booking WHERE booking_id=?", (booking_id,)
        ).fetchone()
        return _row_to_booking(row) if row else None

    def update_booking(self, conn: sqlite3.Connection, booking: Booking) -> None:
        conn.execute(
            "UPDATE booking SET visitor_id=?, variety=?, date=?, trip_id=?,"
            " status=?, amount=?, updated_at=? WHERE booking_id=?",
            (booking.visitor_id, booking.variety, booking.date, booking.trip_id,
             booking.status, booking.amount, booking.updated_at,
             booking.booking_id),
        )

    def list_active_bookings_for_trip(self, conn: sqlite3.Connection,
                                      trip_id: str) -> list[Booking]:
        rows = conn.execute(
            "SELECT * FROM booking WHERE trip_id=?"
            f" AND status IN ({ACTIVE_STATE_LIST}) ORDER BY booking_id",
            (trip_id,),
        ).fetchall()
        return [_row_to_booking(r) for r in rows]

    # ------------------------------------------------------------- 付款
    def insert_payment(self, conn: sqlite3.Connection, booking_id: str,
                       amount: int, idempotency_key: str, paid_at: str) -> None:
        conn.execute(
            "INSERT INTO payment(booking_id, amount, idempotency_key, paid_at)"
            " VALUES(?,?,?,?)",
            (booking_id, amount, idempotency_key, paid_at),
        )

    # ---------------------------------------------------------- 幂等记录
    def get_idempotency(self, key: str) -> IdempotencyRecord | None:
        row = self.connection.execute(
            "SELECT * FROM idempotency_record WHERE idempotency_key=?", (key,)
        ).fetchone()
        return _row_to_idempotency(row) if row else None

    def get_idempotency_conn(self, conn: sqlite3.Connection,
                             key: str) -> IdempotencyRecord | None:
        row = conn.execute(
            "SELECT * FROM idempotency_record WHERE idempotency_key=?", (key,)
        ).fetchone()
        return _row_to_idempotency(row) if row else None

    def insert_idempotency(self, conn: sqlite3.Connection,
                           record: IdempotencyRecord) -> None:
        conn.execute(
            "INSERT INTO idempotency_record(idempotency_key, visitor_id, action,"
            " request_hash, result_ref, response_json, created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (record.idempotency_key, record.visitor_id, record.action,
             record.request_hash, record.result_ref, record.response_json,
             record.created_at),
        )

    # ------------------------------------------------------------- 事件
    def append_event(self, conn: sqlite3.Connection, event_type: str,
                     aggregate_type: str, aggregate_id: str,
                     payload: dict[str, Any], created_at: str) -> int:
        cur = conn.execute(
            "INSERT INTO event_log(event_type, aggregate_type, aggregate_id,"
            " payload, created_at) VALUES(?,?,?,?,?)",
            (event_type, aggregate_type, aggregate_id,
             json.dumps(payload, ensure_ascii=False, sort_keys=True), created_at),
        )
        return int(cur.lastrowid)

    def list_events(self, after_seq: int = 0,
                    aggregate_type: str | None = None,
                    aggregate_id: str | None = None) -> list[Event]:
        sql = "SELECT * FROM event_log WHERE seq > ?"
        params: list[Any] = [after_seq]
        if aggregate_type is not None:
            sql += " AND aggregate_type=?"
            params.append(aggregate_type)
        if aggregate_id is not None:
            sql += " AND aggregate_id=?"
            params.append(aggregate_id)
        sql += " ORDER BY seq"
        rows = self.connection.execute(sql, params).fetchall()
        return [_row_to_event(r) for r in rows]

    # ------------------------------------------------------------- 退款
    def insert_refund(self, conn: sqlite3.Connection, task: RefundTask) -> None:
        conn.execute(
            "INSERT INTO refund_task(refund_id, booking_id, amount, reason,"
            " status, created_at, updated_at) VALUES(?,?,?,?,?,?,?)",
            (task.refund_id, task.booking_id, task.amount, task.reason,
             task.status, task.created_at, task.updated_at),
        )

    def get_refund(self, conn: sqlite3.Connection, refund_id: str) -> RefundTask | None:
        row = conn.execute(
            "SELECT * FROM refund_task WHERE refund_id=?", (refund_id,)
        ).fetchone()
        return _row_to_refund(r) if (r := row) else None

    def get_refund_connless(self, refund_id: str) -> RefundTask | None:
        return self.get_refund(self.connection, refund_id)

    def list_refunds(self, status: str | None = None) -> list[RefundTask]:
        if status is None:
            rows = self.connection.execute(
                "SELECT * FROM refund_task ORDER BY refund_id"
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM refund_task WHERE status=? ORDER BY refund_id",
                (status,),
            ).fetchall()
        return [_row_to_refund(r) for r in rows]

    def update_refund_status(self, conn: sqlite3.Connection, refund_id: str,
                             status: str, updated_at: str) -> None:
        conn.execute(
            "UPDATE refund_task SET status=?, updated_at=? WHERE refund_id=?",
            (status, updated_at, refund_id),
        )


# ----------------------------------------------------------------- 行映射
def _row_to_zone(row: sqlite3.Row) -> TreeZone:
    return TreeZone(
        zone_id=row["zone_id"], variety=row["variety"], name=row["name"],
        daily_capacity=row["daily_capacity"],
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


def _row_to_trip(row: sqlite3.Row) -> BoatTrip:
    return BoatTrip(
        trip_id=row["trip_id"], date=row["date"], route=row["route"],
        seat_capacity=row["seat_capacity"], status=row["status"],
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


def _row_to_booking(row: sqlite3.Row) -> Booking:
    return Booking(
        booking_id=row["booking_id"], visitor_id=row["visitor_id"],
        variety=row["variety"], date=row["date"], trip_id=row["trip_id"],
        status=row["status"], amount=row["amount"],
        idempotency_key=row["idempotency_key"],
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        seq=row["seq"], event_type=row["event_type"],
        aggregate_type=row["aggregate_type"], aggregate_id=row["aggregate_id"],
        payload=row["payload"], created_at=row["created_at"],
    )


def _row_to_refund(row: sqlite3.Row) -> RefundTask:
    return RefundTask(
        refund_id=row["refund_id"], booking_id=row["booking_id"],
        amount=row["amount"], reason=row["reason"], status=row["status"],
        created_at=row["created_at"], updated_at=row["updated_at"],
    )


def _row_to_idempotency(row: sqlite3.Row) -> IdempotencyRecord:
    return IdempotencyRecord(
        idempotency_key=row["idempotency_key"], visitor_id=row["visitor_id"],
        request_hash=row["request_hash"], action=row["action"],
        result_ref=row["result_ref"], response_json=row["response_json"],
        created_at=row["created_at"],
    )
