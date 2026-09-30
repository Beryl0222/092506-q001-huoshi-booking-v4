"""火柿调度的本地持久化边界（SQLite）。

并发策略：

* 所有写操作在 ``BEGIN IMMEDIATE`` 事务中执行，写事务在数据库层面串行化，
  库存扣减与唯一约束在同一事务内检查，杜绝超卖与重复扣减；
* 开启 WAL、设置 busy_timeout，允许服务重启后被多个进程/线程重新打开；
* 进程内再用一把锁串行化同一连接上的事务，跨进程则由 SQLite 文件锁串行化。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

from .domain import (
    ACTIVE_ORDER_STATES,
    BoatTrip,
    Event,
    IdempotencyRecord,
    Order,
    Record,
    RefundTodo,
    TreeZone,
    now_iso,
)

ACTIVE_STATE_LIST = ",".join("?" for _ in ACTIVE_ORDER_STATES)


class Store:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._tx_depth = threading.local()
        self.connection = sqlite3.connect(
            self.path,
            check_same_thread=False,
            isolation_level=None,  # 手工管理事务
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    # -- 基础 ----------------------------------------------------------------

    def _create_schema(self) -> None:
        with self._lock, self.connection:
            self.connection.executescript(
                """
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
                    daily_capacity INTEGER NOT NULL,
                    unit_price_fen INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS boat_trip (
                    trip_id TEXT PRIMARY KEY,
                    boat_route TEXT NOT NULL,
                    pick_date TEXT NOT NULL,
                    time_slot TEXT NOT NULL,
                    capacity INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    sealed_at TEXT NOT NULL DEFAULT '',
                    seal_reason TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    visitor_id TEXT NOT NULL,
                    variety TEXT NOT NULL,
                    zone_id TEXT NOT NULL,
                    pick_date TEXT NOT NULL,
                    trip_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    amount_fen INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL DEFAULT '',
                    replaced_by TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                -- 同一游客对同一 品种/采摘日/船班 至多一个未完结订单
                CREATE UNIQUE INDEX IF NOT EXISTS uq_active_order
                    ON orders(visitor_id, variety, pick_date, trip_id)
                    WHERE state IN ('pending_payment','confirmed');

                CREATE INDEX IF NOT EXISTS ix_orders_trip ON orders(trip_id);
                CREATE INDEX IF NOT EXISTS ix_orders_zone_date
                    ON orders(zone_id, pick_date);
                CREATE INDEX IF NOT EXISTS ix_orders_state ON orders(state);

                CREATE TABLE IF NOT EXISTS idempotency (
                    idempotency_key TEXT PRIMARY KEY,
                    visitor_id TEXT NOT NULL,
                    request_kind TEXT NOT NULL,
                    request_fingerprint TEXT NOT NULL,
                    order_id TEXT NOT NULL DEFAULT '',
                    response_json TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS event_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    order_id TEXT NOT NULL DEFAULT '',
                    trip_id TEXT NOT NULL DEFAULT '',
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS refund_todo (
                    refund_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    visitor_id TEXT NOT NULL,
                    amount_fen INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    handled_at TEXT NOT NULL DEFAULT ''
                );

                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    @contextmanager
    def tx(self):
        """串行化的写事务（BEGIN IMMEDIATE），同线程可重入。"""
        self._lock.acquire()
        depth = getattr(self._tx_depth, "n", 0)
        try:
            if depth == 0:
                self.connection.execute("BEGIN IMMEDIATE")
                self._tx_depth.n = 1
            else:
                self._tx_depth.n = depth + 1
            try:
                yield self.connection
                if depth == 0:
                    self.connection.execute("COMMIT")
            except Exception:
                if depth == 0:
                    self.connection.execute("ROLLBACK")
                raise
        finally:
            if depth == 0:
                self._tx_depth.n = 0
            else:
                self._tx_depth.n = depth
            self._lock.release()

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    # -- 旧基线：通用记录 ----------------------------------------------------

    def save(self, record: Record) -> Record:
        value = record.with_timestamp()
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO boat_slot(record_id, owner_id, state, created_at)"
                " VALUES(?,?,?,?)",
                (value.record_id, value.owner_id, value.state, value.created_at),
            )
        return value

    def get(self, record_id: str) -> Record | None:
        row = self.connection.execute(
            "SELECT record_id, owner_id, state, created_at"
            " FROM boat_slot WHERE record_id=?",
            (record_id,),
        ).fetchone()
        return Record(**dict(row)) if row else None

    # -- 树区 ----------------------------------------------------------------

    def upsert_zone(self, zone: TreeZone) -> TreeZone:
        ts = now_iso()
        with self.tx() as conn:
            self.upsert_zone_conn(conn, zone, ts)
        return zone

    def upsert_zone_conn(self, conn, zone: TreeZone, ts: str) -> None:
        conn.execute(
            """
            INSERT INTO tree_zone(zone_id, variety, name, daily_capacity,
                                  unit_price_fen, updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(zone_id) DO UPDATE SET
                variety=excluded.variety,
                name=excluded.name,
                daily_capacity=excluded.daily_capacity,
                unit_price_fen=excluded.unit_price_fen,
                updated_at=excluded.updated_at
            """,
            (zone.zone_id, zone.variety, zone.name, zone.daily_capacity,
             zone.unit_price_fen, ts),
        )

    def get_zone(self, zone_id: str) -> TreeZone | None:
        row = self.connection.execute(
            "SELECT zone_id, variety, name, daily_capacity, unit_price_fen"
            " FROM tree_zone WHERE zone_id=?",
            (zone_id,),
        ).fetchone()
        if not row:
            return None
        return TreeZone(**dict(row))

    def list_zones(self, variety: str | None = None) -> list[TreeZone]:
        if variety is None:
            rows = self.connection.execute(
                "SELECT zone_id, variety, name, daily_capacity, unit_price_fen"
                " FROM tree_zone ORDER BY variety, zone_id"
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT zone_id, variety, name, daily_capacity, unit_price_fen"
                " FROM tree_zone WHERE variety=? ORDER BY zone_id",
                (variety,),
            ).fetchall()
        return [TreeZone(**dict(r)) for r in rows]

    # -- 船班 ----------------------------------------------------------------

    def upsert_trip(self, trip: BoatTrip) -> BoatTrip:
        ts = now_iso()
        with self.tx() as conn:
            self.upsert_trip_conn(conn, trip, ts)
        return trip

    def upsert_trip_conn(self, conn, trip: BoatTrip, ts: str) -> None:
        conn.execute(
            """
            INSERT INTO boat_trip(trip_id, boat_route, pick_date, time_slot,
                                  capacity, status, sealed_at, seal_reason,
                                  updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(trip_id) DO UPDATE SET
                boat_route=excluded.boat_route,
                pick_date=excluded.pick_date,
                time_slot=excluded.time_slot,
                capacity=excluded.capacity,
                status=CASE
                    WHEN boat_trip.status='sealed' THEN boat_trip.status
                    ELSE excluded.status END,
                updated_at=excluded.updated_at
            """,
            (trip.trip_id, trip.boat_route, trip.pick_date, trip.time_slot,
             trip.capacity, trip.status, trip.sealed_at, trip.seal_reason, ts),
        )

    def get_trip(self, trip_id: str) -> BoatTrip | None:
        row = self.connection.execute(
            "SELECT trip_id, boat_route, pick_date, time_slot, capacity, status,"
            " sealed_at, seal_reason FROM boat_trip WHERE trip_id=?",
            (trip_id,),
        ).fetchone()
        return BoatTrip(**dict(row)) if row else None

    def list_trips(self, pick_date: str | None = None,
                   status: str | None = None) -> list[BoatTrip]:
        sql = ("SELECT trip_id, boat_route, pick_date, time_slot, capacity, status,"
               " sealed_at, seal_reason FROM boat_trip")
        clauses, params = [], []
        if pick_date is not None:
            clauses.append("pick_date=?")
            params.append(pick_date)
        if status is not None:
            clauses.append("status=?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY pick_date, time_slot, trip_id"
        rows = self.connection.execute(sql, params).fetchall()
        return [BoatTrip(**dict(r)) for r in rows]

    def seal_trip(self, trip_id: str, reason: str, sealed_at: str) -> None:
        with self.tx() as conn:
            self.seal_trip_conn(conn, trip_id, reason, sealed_at)

    def seal_trip_conn(self, conn, trip_id: str, reason: str,
                       sealed_at: str) -> None:
        conn.execute(
            "UPDATE boat_trip SET status='sealed', sealed_at=?, seal_reason=?,"
            " updated_at=? WHERE trip_id=?",
            (sealed_at, reason, sealed_at, trip_id),
        )

    # -- 订单 ----------------------------------------------------------------

    def insert_order(self, order: Order) -> None:
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO orders(order_id, visitor_id, variety, zone_id, pick_date,
                                   trip_id, quantity, state, amount_fen,
                                   idempotency_key, replaced_by, version,
                                   created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (order.order_id, order.visitor_id, order.variety, order.zone_id,
                 order.pick_date, order.trip_id, order.quantity, order.state,
                 order.amount_fen, order.idempotency_key, order.replaced_by,
                 order.version, order.created_at, order.updated_at),
            )

    @staticmethod
    def _row_to_order(row: sqlite3.Row) -> Order:
        return Order(
            order_id=row["order_id"], visitor_id=row["visitor_id"],
            variety=row["variety"], zone_id=row["zone_id"],
            pick_date=row["pick_date"], trip_id=row["trip_id"],
            quantity=row["quantity"], state=row["state"],
            amount_fen=row["amount_fen"],
            idempotency_key=row["idempotency_key"],
            replaced_by=row["replaced_by"], version=row["version"],
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    def get_order(self, order_id: str) -> Order | None:
        row = self.connection.execute(
            "SELECT * FROM orders WHERE order_id=?", (order_id,)
        ).fetchone()
        return self._row_to_order(row) if row else None

    def get_order_conn(self, conn: sqlite3.Connection, order_id: str):
        row = conn.execute(
            "SELECT * FROM orders WHERE order_id=?", (order_id,)
        ).fetchone()
        return self._row_to_order(row) if row else None

    def update_order_state_conn(
        self, conn: sqlite3.Connection, order_id: str, state: str,
        version: int, updated_at: str, replaced_by: str = "",
    ) -> None:
        """乐观版本号更新；影响行数为 0 表示状态已被并发改写。"""
        cur = conn.execute(
            "UPDATE orders SET state=?, version=version+1, updated_at=?,"
            " replaced_by=COALESCE(NULLIF(?, ''), replaced_by)"
            " WHERE order_id=? AND version=?",
            (state, updated_at, replaced_by, order_id, version),
        )
        if cur.rowcount == 0:
            raise sqlite3.IntegrityError(
                f"订单 {order_id} 版本 {version} 已过期"
            )

    def active_orders_of_trip_conn(self, conn: sqlite3.Connection,
                                   trip_id: str) -> list[Order]:
        rows = conn.execute(
            f"SELECT * FROM orders WHERE trip_id=? AND state IN ({ACTIVE_STATE_LIST})"
            " ORDER BY created_at, order_id",
            (trip_id, *ACTIVE_ORDER_STATES),
        ).fetchall()
        return [self._row_to_order(r) for r in rows]

    def find_active_order_conn(self, conn: sqlite3.Connection, visitor_id: str,
                               variety: str, pick_date: str,
                               trip_id: str) -> Order | None:
        row = conn.execute(
            f"SELECT * FROM orders WHERE visitor_id=? AND variety=? AND pick_date=?"
            f" AND trip_id=? AND state IN ({ACTIVE_STATE_LIST})",
            (visitor_id, variety, pick_date, trip_id, *ACTIVE_ORDER_STATES),
        ).fetchone()
        return self._row_to_order(row) if row else None

    def zone_reserved_conn(self, conn: sqlite3.Connection, zone_id: str,
                           pick_date: str, exclude_order_id: str = "") -> int:
        row = conn.execute(
            f"SELECT COALESCE(SUM(quantity),0) AS n FROM orders"
            f" WHERE zone_id=? AND pick_date=? AND state IN ({ACTIVE_STATE_LIST})"
            f" AND order_id<>?",
            (zone_id, pick_date, *ACTIVE_ORDER_STATES, exclude_order_id),
        ).fetchone()
        return int(row["n"])

    def trip_reserved_conn(self, conn: sqlite3.Connection, trip_id: str,
                           exclude_order_id: str = "") -> int:
        row = conn.execute(
            f"SELECT COALESCE(SUM(quantity),0) AS n FROM orders"
            f" WHERE trip_id=? AND state IN ({ACTIVE_STATE_LIST})"
            f" AND order_id<>?",
            (trip_id, *ACTIVE_ORDER_STATES, exclude_order_id),
        ).fetchone()
        return int(row["n"])

    def list_active_trips_on_date_conn(self, conn: sqlite3.Connection,
                                       pick_date: str,
                                       boat_route: str | None = None,
                                       exclude_trip_id: str = "") -> list[BoatTrip]:
        if boat_route is None:
            rows = conn.execute(
                "SELECT trip_id, boat_route, pick_date, time_slot, capacity, status,"
                " sealed_at, seal_reason FROM boat_trip"
                " WHERE pick_date=? AND status='active' AND trip_id<>?"
                " ORDER BY time_slot, trip_id",
                (pick_date, exclude_trip_id),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT trip_id, boat_route, pick_date, time_slot, capacity, status,"
                " sealed_at, seal_reason FROM boat_trip"
                " WHERE pick_date=? AND status='active' AND boat_route=?"
                " AND trip_id<>? ORDER BY time_slot, trip_id",
                (pick_date, boat_route, exclude_trip_id),
            ).fetchall()
        return [BoatTrip(**dict(r)) for r in rows]

    def list_expired_pending_conn(self, conn: sqlite3.Connection,
                                  deadline_iso: str) -> list[Order]:
        rows = conn.execute(
            "SELECT * FROM orders WHERE state='pending_payment'"
            " AND created_at <= ? ORDER BY created_at, order_id",
            (deadline_iso,),
        ).fetchall()
        return [self._row_to_order(r) for r in rows]

    def inventory_snapshot(self, pick_date: str,
                           variety: str | None = None) -> dict:
        """汇总某日各树区/船班的容量与占用。"""
        zones = self.list_zones(variety)
        trips = self.list_trips(pick_date)
        zone_ids = [z.zone_id for z in zones]
        zone_used: dict[str, int] = {z: 0 for z in zone_ids}
        trip_used: dict[str, int] = {t.trip_id: 0 for t in trips}
        if zone_ids or trips:
            qmarks_z = ",".join("?" for _ in zone_ids) or "''"
            rows = self.connection.execute(
                f"SELECT zone_id, COALESCE(SUM(quantity),0) AS n FROM orders"
                f" WHERE pick_date=? AND state IN ({ACTIVE_STATE_LIST})"
                f" AND zone_id IN ({qmarks_z}) GROUP BY zone_id",
                (pick_date, *ACTIVE_ORDER_STATES, *zone_ids),
            ).fetchall()
            for r in rows:
                zone_used[r["zone_id"]] = int(r["n"])
            rows = self.connection.execute(
                f"SELECT trip_id, COALESCE(SUM(quantity),0) AS n FROM orders"
                f" WHERE state IN ({ACTIVE_STATE_LIST})"
                f" AND trip_id IN ({','.join('?' for _ in trips) or "''"})"
                f" GROUP BY trip_id",
                (*ACTIVE_ORDER_STATES, *[t.trip_id for t in trips]),
            ).fetchall() if trips else []
            for r in rows:
                trip_used[r["trip_id"]] = int(r["n"])
        return {
            "zones": [
                {
                    "zone_id": z.zone_id, "variety": z.variety, "name": z.name,
                    "capacity": z.daily_capacity,
                    "reserved": zone_used.get(z.zone_id, 0),
                    "remaining": z.daily_capacity - zone_used.get(z.zone_id, 0),
                    "unit_price_fen": z.unit_price_fen,
                }
                for z in zones
            ],
            "trips": [
                {
                    "trip_id": t.trip_id, "boat_route": t.boat_route,
                    "pick_date": t.pick_date, "time_slot": t.time_slot,
                    "capacity": t.capacity,
                    "reserved": trip_used.get(t.trip_id, 0),
                    "remaining": t.capacity - trip_used.get(t.trip_id, 0),
                    "status": t.status, "seal_reason": t.seal_reason,
                }
                for t in trips
            ],
        }

    # -- 幂等 ----------------------------------------------------------------

    def get_idempotency_conn(self, conn: sqlite3.Connection,
                             idempotency_key: str) -> IdempotencyRecord | None:
        row = conn.execute(
            "SELECT * FROM idempotency WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if not row:
            return None
        return IdempotencyRecord(
            idempotency_key=row["idempotency_key"], visitor_id=row["visitor_id"],
            request_kind=row["request_kind"],
            request_fingerprint=row["request_fingerprint"],
            order_id=row["order_id"], response_json=row["response_json"],
            created_at=row["created_at"],
        )

    def put_idempotency_conn(self, conn: sqlite3.Connection, key: str,
                             visitor_id: str, request_kind: str,
                             fingerprint: str, order_id: str,
                             response_json: str, created_at: str) -> None:
        conn.execute(
            "INSERT INTO idempotency(idempotency_key, visitor_id, request_kind,"
            " request_fingerprint, order_id, response_json, created_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (key, visitor_id, request_kind, fingerprint, order_id,
             response_json, created_at),
        )

    # -- 事件 ----------------------------------------------------------------

    def append_event_conn(self, conn: sqlite3.Connection, event_type: str,
                          order_id: str, trip_id: str, payload: dict,
                          created_at: str) -> int:
        cur = conn.execute(
            "INSERT INTO event_log(event_type, order_id, trip_id, payload,"
            " created_at) VALUES(?,?,?,?,?)",
            (event_type, order_id, trip_id,
             json.dumps(payload, ensure_ascii=False, sort_keys=True), created_at),
        )
        return int(cur.lastrowid)

    def list_events(self, order_id: str = "", trip_id: str = "",
                    after_seq: int = 0, limit: int = 1000) -> list[Event]:
        sql = "SELECT seq, event_type, order_id, trip_id, payload, created_at FROM event_log"
        clauses, params = [], []
        if order_id:
            clauses.append("order_id=?")
            params.append(order_id)
        if trip_id:
            clauses.append("trip_id=?")
            params.append(trip_id)
        if after_seq:
            clauses.append("seq>?")
            params.append(after_seq)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY seq ASC LIMIT ?"
        params.append(limit)
        rows = self.connection.execute(sql, params).fetchall()
        return [
            Event(seq=r["seq"], event_type=r["event_type"], order_id=r["order_id"],
                  trip_id=r["trip_id"], payload=r["payload"],
                  created_at=r["created_at"])
            for r in rows
        ]

    def latest_seq(self) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(seq),0) AS s FROM event_log"
        ).fetchone()
        return int(row["s"])

    # -- 退款待办 -------------------------------------------------------------

    def insert_refund_conn(self, conn: sqlite3.Connection, refund: RefundTodo) -> None:
        conn.execute(
            "INSERT INTO refund_todo(refund_id, order_id, visitor_id, amount_fen,"
            " reason, status, created_at, handled_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (refund.refund_id, refund.order_id, refund.visitor_id,
             refund.amount_fen, refund.reason, refund.status,
             refund.created_at, refund.handled_at),
        )

    def list_refunds(self, status: str = "") -> list[RefundTodo]:
        if status:
            rows = self.connection.execute(
                "SELECT * FROM refund_todo WHERE status=? ORDER BY created_at, refund_id",
                (status,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM refund_todo ORDER BY created_at, refund_id"
            ).fetchall()
        return [RefundTodo(**dict(r)) for r in rows]

    def mark_refund_done_conn(self, conn: sqlite3.Connection, refund_id: str,
                              handled_at: str) -> bool:
        cur = conn.execute(
            "UPDATE refund_todo SET status='done', handled_at=?"
            " WHERE refund_id=? AND status='pending'",
            (handled_at, refund_id),
        )
        return cur.rowcount > 0

    def get_refund_conn(self, conn: sqlite3.Connection,
                        refund_id: str) -> RefundTodo | None:
        row = conn.execute(
            "SELECT * FROM refund_todo WHERE refund_id=?", (refund_id,)
        ).fetchone()
        return RefundTodo(**dict(row)) if row else None

    # -- 配置 ----------------------------------------------------------------

    def get_config(self, key: str, default: str = "") -> str:
        row = self.connection.execute(
            "SELECT value FROM config WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def set_config(self, key: str, value: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "INSERT INTO config(key, value) VALUES(?,?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
