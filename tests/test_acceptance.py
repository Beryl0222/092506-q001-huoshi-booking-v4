"""火柿调度验收测试：并发预订、跨日改期、重复回调、封存处置、重启一致性。

并发用例为每个线程建立独立的 SQLite 连接（同一个文件），由 BEGIN IMMEDIATE
在文件锁层面串行化，模拟多实例/多请求的真实竞争。
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer

from huoshi_booking.api import handle
from huoshi_booking.domain import now_iso
from huoshi_booking.server import build_handler
from huoshi_booking.service import Service
from huoshi_booking.store import Store

DATE1 = "2026-10-01"
DATE2 = "2026-10-02"


def call(service: Service, **body) -> dict:
    return json.loads(handle(json.dumps(body, ensure_ascii=False), service))


def seed(service: Service) -> None:
    """在给定服务上搭建三个品种树区 + 若干船班的最小运营数据。"""
    service.upsert_zone("z-bs", "扁柿", "扁柿A区", daily_capacity=6,
                        unit_price_fen=5000)
    service.upsert_zone("z-hs", "火柿", "火柿B区", daily_capacity=8,
                        unit_price_fen=6000)
    service.upsert_zone("z-fs", "方柿", "方柿C区", daily_capacity=4,
                        unit_price_fen=7000)
    service.upsert_trip("t-1001", "深潭口线", DATE1, "09:00-10:00", capacity=5)
    service.upsert_trip("t-1002", "深潭口线", DATE1, "10:00-11:00", capacity=5)
    service.upsert_trip("t-2001", "深潭口线", DATE2, "09:00-10:00", capacity=5)
    service.upsert_trip("t-2009", "茭芦田庄线", DATE2, "14:00-15:00", capacity=2)


class Fixture:
    """搭建三个品种树区 + 若干船班的最小运营数据。"""

    def __init__(self, path: str = ":memory:"):
        self.service = Service(Store(path))
        seed(self.service)


class 基础维护与库存测试(unittest.TestCase):
    def setUp(self):
        self.fx = Fixture()
        self.s = self.fx.service

    def test_只能维护三个品种(self):
        ok = call(self.s, action="zone.upsert", zone_id="z-x", variety="甜柿",
                  name="X", daily_capacity=1)
        self.assertEqual(ok["error_type"], "domain_error")

    def test_容量与船班库存视图(self):
        inv = call(self.s, action="inventory", pick_date=DATE1)
        zone = next(z for z in inv["zones"] if z["zone_id"] == "z-bs")
        self.assertEqual((zone["capacity"], zone["reserved"], zone["remaining"]),
                         (6, 0, 6))
        trip = next(t for t in inv["trips"] if t["trip_id"] == "t-1001")
        self.assertEqual((trip["capacity"], trip["remaining"]), (5, 5))

    def test_树区名额与船班载客双重限制(self):
        # 树区 6、船 5：先来 5 个占满船，第 6 个被船班拦下
        for i in range(5):
            r = call(self.s, action="order.book", visitor_id=f"u{i}",
                     variety="扁柿", zone_id="z-bs", pick_date=DATE1,
                     trip_id="t-1001", quantity=1, idempotency_key=f"k{i}")
            self.assertNotIn("error", r)
        blocked = call(self.s, action="order.book", visitor_id="u5",
                       variety="扁柿", zone_id="z-bs", pick_date=DATE1,
                       trip_id="t-1001", quantity=1, idempotency_key="k5")
        self.assertEqual(blocked["error_type"], "domain_error")
        self.assertIn("船班", blocked["error"])
        inv = call(self.s, action="inventory", pick_date=DATE1)
        trip = next(t for t in inv["trips"] if t["trip_id"] == "t-1001")
        self.assertEqual(trip["reserved"], 5)

    def test_同树区跨船班共享日容量(self):
        # 每条船 5 个位置但树区日容量只有 6：两船合计不能超过 6
        for i in range(3):
            self.assertNotIn("error", call(
                self.s, action="order.book", visitor_id=f"a{i}",
                variety="扁柿", zone_id="z-bs", pick_date=DATE1,
                trip_id="t-1001", quantity=1, idempotency_key=f"a{i}"))
        for i in range(3):
            self.assertNotIn("error", call(
                self.s, action="order.book", visitor_id=f"b{i}",
                variety="扁柿", zone_id="z-bs", pick_date=DATE1,
                trip_id="t-1002", quantity=1, idempotency_key=f"b{i}"))
        blocked = call(self.s, action="order.book", visitor_id="c0",
                       variety="扁柿", zone_id="z-bs", pick_date=DATE1,
                       trip_id="t-1002", quantity=1, idempotency_key="c0")
        self.assertIn("名额不足", blocked["error"])


class 并发预订测试(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "huoshi.db")
        Fixture(self.db_path)  # 初始化基础数据

    def tearDown(self):
        self.tmp.cleanup()

    def test_多连接并发不超卖船班与树区(self):
        def book(i):
            svc = Service(Store(self.db_path))
            return call(svc, action="order.book", visitor_id=f"v{i}",
                        variety="扁柿", zone_id="z-bs", pick_date=DATE1,
                        trip_id="t-1001", quantity=1,
                        idempotency_key=f"parallel-{i}")

        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(book, range(20)))

        successes = [r for r in results if "error" not in r]
        failures = [r for r in results if "error" in r]
        self.assertEqual(len(successes), 5)  # 船班容量 5
        self.assertEqual(len(failures), 15)
        order_ids = {r["order_id"] for r in successes}
        self.assertEqual(len(order_ids), 5)

        svc = Service(Store(self.db_path))
        inv = svc.inventory(DATE1)
        trip = next(t for t in inv["trips"] if t["trip_id"] == "t-1001")
        self.assertEqual(trip["reserved"], 5)
        self.assertEqual(trip["remaining"], 0)

    def test_同一游客并发相同幂等键只产生一笔订单(self):
        barrier = threading.Barrier(8)

        def book(_):
            svc = Service(Store(self.db_path))
            barrier.wait()
            return call(svc, action="order.book", visitor_id="same-visitor",
                        variety="火柿", zone_id="z-hs", pick_date=DATE1,
                        trip_id="t-1001", quantity=1,
                        idempotency_key="same-key")

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(book, range(8)))

        order_ids = {r["order_id"] for r in results if "error" not in r}
        self.assertEqual(order_ids.__len__(), 1)
        self.assertTrue(all("error" not in r for r in results),
                        f"同键重放不应报错：{results}")
        first = [r for r in results if not r.get("idempotent_replay")]
        replays = [r for r in results if r.get("idempotent_replay")]
        self.assertEqual(len(first), 1)
        self.assertEqual(len(replays), 7)
        # 只扣减一次
        svc = Service(Store(self.db_path))
        inv = svc.inventory(DATE1, "火柿")
        self.assertEqual(inv["zones"][0]["reserved"], 1)

    def test_同一游客并发不同幂等键不产生重复扣减(self):
        barrier = threading.Barrier(6)

        def book(i):
            svc = Service(Store(self.db_path))
            barrier.wait()
            return call(svc, action="order.book", visitor_id="dup-visitor",
                        variety="方柿", zone_id="z-fs", pick_date=DATE1,
                        trip_id="t-1001", quantity=1,
                        idempotency_key=f"dup-key-{i}")

        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(book, range(6)))

        successes = [r for r in results if "error" not in r]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len([r for r in results if "error" in r]), 5)
        svc = Service(Store(self.db_path))
        inv = svc.inventory(DATE1, "方柿")
        self.assertEqual(inv["zones"][0]["reserved"], 1)

    def test_幂等键复用于不同请求被拒绝(self):
        r1 = call(Service(Store(self.db_path)), action="order.book",
                  visitor_id="reuse", variety="扁柿", zone_id="z-bs",
                  pick_date=DATE1, trip_id="t-1001", quantity=1,
                  idempotency_key="REUSE")
        self.assertNotIn("error", r1)
        r2 = call(Service(Store(self.db_path)), action="order.book",
                  visitor_id="reuse", variety="扁柿", zone_id="z-bs",
                  pick_date=DATE1, trip_id="t-1002", quantity=1,
                  idempotency_key="REUSE")
        self.assertEqual(r2["error_type"], "idempotency_conflict")


class 付款重复回调测试(unittest.TestCase):
    def setUp(self):
        self.fx = Fixture()
        self.s = self.fx.service
        self.order = self.s.book("payer", "扁柿", "z-bs", DATE1, "t-1001",
                                 1, "book-key")["order_id"]

    def test_重复付款回调只确认一次(self):
        p1 = call(self.s, action="order.pay", order_id=self.order,
                  idempotency_key="pay-key", payment_ref="WX-1")
        self.assertEqual(p1["state"], "confirmed")
        # 相同幂等键：原样重放
        p2 = call(self.s, action="order.pay", order_id=self.order,
                  idempotency_key="pay-key", payment_ref="WX-1")
        self.assertTrue(p2["idempotent_replay"])
        self.assertEqual(p2["state"], "confirmed")
        # 支付网关换了回调 ID 又推一次：幂等返回，不重复记账
        p3 = call(self.s, action="order.pay", order_id=self.order,
                  idempotency_key="pay-key-retry", payment_ref="WX-1")
        self.assertTrue(p3["idempotent_replay"])
        self.assertIn("已确认", p3["note"])
        events = self.s.list_events(self.order)
        self.assertEqual(
            [e["event_type"] for e in events],
            ["order.created", "payment.confirmed"])

    def test_取消后付款回调被拒绝(self):
        call(self.s, action="order.pay", order_id=self.order,
             idempotency_key="pay-key")
        call(self.s, action="order.cancel", order_id=self.order,
             idempotency_key="cancel-key", reason="行程变更")
        late = call(self.s, action="order.pay", order_id=self.order,
                    idempotency_key="pay-late", payment_ref="WX-2")
        self.assertEqual(late["error_type"], "domain_error")

    def test_已付款取消产生退款待办且取消幂等(self):
        call(self.s, action="order.pay", order_id=self.order,
             idempotency_key="pay-key")
        c1 = call(self.s, action="order.cancel", order_id=self.order,
                  idempotency_key="cancel-key")
        self.assertIn("refund_id", c1)
        c2 = call(self.s, action="order.cancel", order_id=self.order,
                  idempotency_key="cancel-key")
        self.assertTrue(c2["idempotent_replay"])
        refunds = self.s.list_refunds("pending")
        self.assertEqual(len(refunds), 1)
        self.assertEqual(refunds[0]["amount_fen"], 5000)
        done = call(self.s, action="refund.complete",
                    refund_id=refunds[0]["refund_id"])
        self.assertEqual(done["status"], "done")
        again = call(self.s, action="refund.complete",
                     refund_id=refunds[0]["refund_id"])
        self.assertTrue(again.get("already_done"))
        # 取消后名额释放
        inv = self.s.inventory(DATE1)
        trip = next(t for t in inv["trips"] if t["trip_id"] == "t-1001")
        self.assertEqual(trip["reserved"], 0)


class 跨日改期测试(unittest.TestCase):
    def setUp(self):
        self.fx = Fixture()
        self.s = self.fx.service
        r = self.s.book("mover", "扁柿", "z-bs", DATE1, "t-1001", 2,
                        "move-book")
        self.old_id = r["order_id"]
        self.s.confirm_payment(self.old_id, "move-pay")

    def test_跨日改期释放旧名额占用新名额(self):
        r = call(self.s, action="order.reschedule", order_id=self.old_id,
                 new_pick_date=DATE2, new_trip_id="t-2001",
                 idempotency_key="move-1")
        self.assertNotIn("error", r)
        new_id = r["order_id"]
        self.assertTrue(new_id.startswith(f"{self.old_id}-R"))
        self.assertEqual(r["state"], "confirmed")
        self.assertEqual(r["rescheduled_from"], self.old_id)

        old = self.s.get_order(self.old_id)
        self.assertEqual(old["state"], "rescheduled")
        self.assertEqual(old["replaced_by"], new_id)

        inv1 = self.s.inventory(DATE1)
        inv2 = self.s.inventory(DATE2)
        self.assertEqual(
            next(t for t in inv1["trips"] if t["trip_id"] == "t-1001")[
                "reserved"], 0)
        self.assertEqual(
            next(z for z in inv1["zones"] if z["zone_id"] == "z-bs")[
                "reserved"], 0)
        self.assertEqual(
            next(t for t in inv2["trips"] if t["trip_id"] == "t-2001")[
                "reserved"], 2)

    def test_改期请求可幂等重放且事件链完整(self):
        r1 = call(self.s, action="order.reschedule", order_id=self.old_id,
                  new_pick_date=DATE2, new_trip_id="t-2001",
                  idempotency_key="move-key")
        r2 = call(self.s, action="order.reschedule", order_id=self.old_id,
                  new_pick_date=DATE2, new_trip_id="t-2001",
                  idempotency_key="move-key")
        self.assertTrue(r2["idempotent_replay"])
        self.assertEqual(r1["order_id"], r2["order_id"])

        events = self.s.list_events()
        types = [e["event_type"] for e in events]
        self.assertEqual(types.count("order.rescheduled"), 1)
        chain = [e for e in events
                 if e["order_id"] in (self.old_id, r1["order_id"])]
        self.assertEqual(
            [e["event_type"] for e in chain],
            ["order.created", "payment.confirmed", "order.rescheduled",
             "order.created"])
        # 全局事件 seq 严格递增
        seqs = [e["seq"] for e in events]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(seqs), len(set(seqs)))

    def test_改期到满载船班失败且原订单不受影响(self):
        for i in range(5):
            r = self.s.book(f"full{i}", "扁柿", "z-bs", DATE2, "t-2001", 1,
                            f"fill-{i}")
            self.assertNotIn("error", r)
        failed = call(self.s, action="order.reschedule",
                      order_id=self.old_id, new_pick_date=DATE2,
                      new_trip_id="t-2001", idempotency_key="move-fail")
        self.assertIn("不足", failed["error"])
        self.assertEqual(self.s.get_order(self.old_id)["state"], "confirmed")

    def test_已改期订单可继续改到其他日期(self):
        r1 = call(self.s, action="order.reschedule", order_id=self.old_id,
                  new_pick_date=DATE2, new_trip_id="t-2001",
                  idempotency_key="move-1")
        r2 = call(self.s, action="order.reschedule", order_id=r1["order_id"],
                  new_pick_date=DATE1, new_trip_id="t-1002",
                  idempotency_key="move-2")
        self.assertNotIn("error", r2)
        self.assertEqual(r2["pick_date"], DATE1)
        self.assertEqual(self.s.get_order(self.old_id)["state"], "rescheduled")
        self.assertEqual(self.s.get_order(r1["order_id"])["state"],
                         "rescheduled")
        self.assertEqual(self.s.get_order(r2["order_id"])["state"],
                         "confirmed")


class 船班封存测试(unittest.TestCase):
    def setUp(self):
        self.fx = Fixture()
        self.s = self.fx.service

    def _seed(self):
        paid = self.s.book("paid-v", "扁柿", "z-bs", DATE1, "t-1001", 2,
                           "seal-book-1")
        self.s.confirm_payment(paid["order_id"], "seal-pay-1")
        unpaid = self.s.book("unpaid-v", "火柿", "z-hs", DATE1, "t-1001", 1,
                             "seal-book-2")
        return paid["order_id"], unpaid["order_id"]

    def test_封存退回模式_已付款退单_未付款取消(self):
        paid_id, unpaid_id = self._seed()
        result = call(self.s, action="trip.seal", trip_id="t-1001",
                      reason="水位警戒线封航", mode="refund")
        self.assertEqual({r["order_id"] for r in result["refunded"]},
                         {paid_id})
        self.assertEqual({c["order_id"] for c in result["cancelled"]},
                         {unpaid_id})

        self.assertEqual(self.s.get_order(paid_id)["state"], "refunded")
        self.assertEqual(self.s.get_order(unpaid_id)["state"], "cancelled")
        refunds = self.s.list_refunds("pending")
        self.assertEqual(len(refunds), 1)
        self.assertEqual(refunds[0]["order_id"], paid_id)
        self.assertEqual(refunds[0]["amount_fen"], 10000)
        self.assertIn("水位警戒线封航", refunds[0]["reason"])

        # 封存后无法再订
        blocked = call(self.s, action="order.book", visitor_id="late",
                       variety="扁柿", zone_id="z-bs", pick_date=DATE1,
                       trip_id="t-1001", quantity=1, idempotency_key="late")
        self.assertIn("封存", blocked["error"])
        # 库存已全部释放
        inv = self.s.inventory(DATE1)
        self.assertEqual(
            next(t for t in inv["trips"] if t["trip_id"] == "t-1001")[
                "reserved"], 0)
        # 封存事件与逐单事件齐全
        events = self.s.list_events(trip_id="t-1001")
        types = [e["event_type"] for e in events]
        self.assertIn("trip.sealed", types)
        self.assertIn("order.refunded", types)
        self.assertIn("order.cancelled", types)

    def test_封存迁移模式_同路线船班接管(self):
        paid_id, _ = self._seed()
        result = call(self.s, action="trip.seal", trip_id="t-1001",
                      reason="临时交通管制", mode="migrate")
        self.assertEqual(len(result["migrated"]), 2)
        self.assertEqual(result["refunded"], [])
        moved = next(m for m in result["migrated"] if m["order_id"] == paid_id)
        self.assertEqual(moved["new_trip_id"], "t-1002")  # 同日同路线下一班
        new_order = self.s.get_order(moved["new_order_id"])
        self.assertEqual(new_order["state"], "confirmed")
        self.assertEqual(new_order["quantity"], 2)
        self.assertEqual(self.s.get_order(paid_id)["state"], "migrated")
        inv = self.s.inventory(DATE1)
        self.assertEqual(
            next(t for t in inv["trips"] if t["trip_id"] == "t-1001")[
                "reserved"], 0)
        self.assertEqual(
            next(t for t in inv["trips"] if t["trip_id"] == "t-1002")[
                "reserved"], 3)

    def test_封存迁移到指定跨日船班(self):
        paid_id, unpaid_id = self._seed()
        result = call(self.s, action="trip.seal", trip_id="t-1001",
                      reason="台风", mode="migrate",
                      target_trip_id="t-2009", target_pick_date=DATE2)
        self.assertEqual({m["new_trip_id"] for m in result["migrated"]},
                         {"t-2009"})
        # 目标船容量 2：两单合计 3，第二单兜底退回/取消
        migrated_orders = {m["order_id"] for m in result["migrated"]}
        self.assertEqual(len(migrated_orders), 1)
        if paid_id in migrated_orders:
            self.assertEqual({r["order_id"] for r in result["refunded"]},
                             set())
        # 未付款单迁不进则被取消，已付款单迁不进则进退款待办
        all_settled = migrated_orders | {r["order_id"] for r in
                                         result["refunded"]} | \
            {c["order_id"] for c in result["cancelled"]}
        self.assertEqual(all_settled, {paid_id, unpaid_id})

    def test_重复封存幂等不重复处置(self):
        self._seed()
        first = call(self.s, action="trip.seal", trip_id="t-1001",
                     reason="封航", mode="refund")
        second = call(self.s, action="trip.seal", trip_id="t-1001",
                      reason="封航", mode="refund")
        self.assertTrue(second.get("already_sealed"))
        self.assertEqual(len(self.s.list_refunds("pending")),
                         len([r for r in first["refunded"]]))

    def test_迁移时游客已占用备选船班则兜底退回不报错(self):
        # paid-v 在 t-1001（将封存），同时在备选船 t-1002 已有订单
        paid = self.s.book("paid-v", "扁柿", "z-bs", DATE1, "t-1001", 1,
                           "dup-mig-1")
        self.s.confirm_payment(paid["order_id"], "dup-mig-pay")
        other = self.s.book("paid-v", "扁柿", "z-bs", DATE1, "t-1002", 1,
                            "dup-mig-2")
        self.s.confirm_payment(other["order_id"], "dup-mig-pay-2")
        result = call(self.s, action="trip.seal", trip_id="t-1001",
                      reason="水位警戒", mode="migrate")
        self.assertNotIn("error", result)
        # 无法迁到 t-1002（同一游客已有单），已付款 → 退款兜底
        self.assertEqual([r["order_id"] for r in result["refunded"]],
                         [paid["order_id"]])
        self.assertEqual(result["migrated"], [])
        self.assertEqual(self.s.get_order(paid["order_id"])["state"],
                         "refunded")
        # 备选船订单不受影响
        self.assertEqual(self.s.get_order(other["order_id"])["state"],
                         "confirmed")


class 超时释放测试(unittest.TestCase):
    def test_超时未付款订单被扫出并释放名额(self):
        current = {"t": datetime(2026, 10, 1, 8, 0, tzinfo=timezone.utc)}

        def clock():
            return current["t"].isoformat()

        s = Service(Store(), clock=clock)
        seed(s)
        r = s.book("slow", "扁柿", "z-bs", DATE1, "t-1001", 2, "slow-book")
        self.assertEqual(r["state"], "pending_payment")
        current["t"] += timedelta(minutes=20)
        sweep = s.sweep_expired(timeout_minutes=15)
        self.assertEqual(sweep["cancelled"], [r["order_id"]])
        self.assertEqual(s.get_order(r["order_id"])["state"], "cancelled")
        inv = s.inventory(DATE1)
        self.assertEqual(
            next(t for t in inv["trips"] if t["trip_id"] == "t-1001")[
                "reserved"], 0)


class 重启一致性测试(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "huoshi.db")
        fx = Fixture(self.db_path)
        s = fx.service
        paid = s.book("restart-paid", "扁柿", "z-bs", DATE1, "t-1001", 2,
                      "r-book-1")
        s.confirm_payment(paid["order_id"], "r-pay-1")
        pending = s.book("restart-pending", "火柿", "z-hs", DATE1,
                         "t-1002", 1, "r-book-2")
        s.seal_trip("t-2001", "航道维修", mode="refund")
        self.paid_id = paid["order_id"]
        self.pending_id = pending["order_id"]
        seq_before = s.store.latest_seq()
        s.store.close()
        self.seq_before = seq_before

    def tearDown(self):
        self.tmp.cleanup()

    def test_重启后库存订单退款事件全部保持一致(self):
        s = Service(Store(self.db_path))
        inv1 = s.inventory(DATE1)
        self.assertEqual(
            next(t for t in inv1["trips"] if t["trip_id"] == "t-1001")[
                "reserved"], 2)
        inv2 = s.inventory(DATE2)
        self.assertEqual(
            next(t for t in inv2["trips"] if t["trip_id"] == "t-2001")[
                "status"], "sealed")
        self.assertEqual(s.get_order(self.paid_id)["state"], "confirmed")
        self.assertEqual(s.get_order(self.pending_id)["state"],
                         "pending_payment")

        # 封存导致的退款待办/事件在重启后仍在
        events = s.list_events()
        self.assertTrue(all(e["seq"] <= self.seq_before for e in events))
        types = [e["event_type"] for e in events]
        self.assertIn("trip.sealed", types)
        seal = next(e for e in events if e["event_type"] == "trip.sealed")
        self.assertEqual(seal["payload"]["reason"], "航道维修")
        # seq 连续提交顺序：同一订单 create 必在 pay 之前
        paid_events = [e for e in events if e["order_id"] == self.paid_id]
        self.assertEqual([e["event_type"] for e in paid_events][:2],
                         ["order.created", "payment.confirmed"])
        self.assertLess(paid_events[0]["seq"], paid_events[1]["seq"])

    def test_重启后幂等键仍可去重(self):
        s = Service(Store(self.db_path))
        replay = s.book("restart-paid", "扁柿", "z-bs", DATE1, "t-1001", 2,
                        "r-book-1")
        self.assertTrue(replay.get("idempotent_replay"))
        self.assertEqual(replay["order_id"], self.paid_id)


class 付款与取消竞争测试(unittest.TestCase):
    """付款回调与游客取消并发：只允许一个结果，且事件/退款账实相符。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "huoshi.db")
        Fixture(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_多轮付款取消竞争状态机一致(self):
        for i in range(12):
            svc = Service(Store(self.db_path))
            order_id = svc.book(
                f"race-{i}", "火柿", "z-hs", DATE2, "t-2009", 1,
                f"race-book-{i}")["order_id"]
            svc.store.close()

            barrier = threading.Barrier(2)

            def do(kind):
                s = Service(Store(self.db_path))
                barrier.wait()
                return call(s, action=kind, order_id=order_id,
                            idempotency_key=f"race-{kind}-{i}")

            with ThreadPoolExecutor(max_workers=2) as pool:
                pay_result, cancel_result = list(
                    pool.map(do, ["order.pay", "order.cancel"]))

            check = Service(Store(self.db_path))
            final = check.get_order(order_id)
            events = check.list_events(order_id)
            pays = [e for e in events
                    if e["event_type"] == "payment.confirmed"]
            cancels = [e for e in events
                       if e["event_type"] == "order.cancelled"]
            self.assertLessEqual(len(pays), 1)
            self.assertLessEqual(len(cancels), 1)

            if pays and cancels:
                # 付款先提交：取消的针对的是已付款单，必须生成退款待办
                self.assertIn("refund_id", cancel_result)
                self.assertEqual(final["state"], "cancelled")
                self.assertLess(pays[0]["seq"], cancels[0]["seq"])
                pending = [r for r in check.list_refunds("pending")
                           if r["order_id"] == order_id]
                self.assertEqual(len(pending), 1)
            else:
                # 取消先提交：付款必须被拒
                self.assertTrue(cancels)
                self.assertIn("error", pay_result)
                self.assertEqual(final["state"], "cancelled")


class HTTP接口重放测试(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "huoshi.db")
        Fixture(self.db_path)
        self.service = Service(Store(self.db_path))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0),
                                          build_handler(self.service))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.service.store.close()
        self.tmp.cleanup()

    def _post(self, body: dict) -> dict:
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/",
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_通过HTTP并发预订不超卖(self):
        barrier = threading.Barrier(10)

        def post(i):
            barrier.wait()
            return self._post({
                "action": "order.book", "visitor_id": f"http-{i}",
                "variety": "方柿", "zone_id": "z-fs", "pick_date": DATE2,
                "trip_id": "t-2009", "quantity": 1,
                "idempotency_key": f"http-key-{i}"})

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(post, range(10)))
        self.assertEqual(len([r for r in results if "error" not in r]), 2)

        # 重复付款回调
        order_id = next(r["order_id"] for r in results if "error" not in r)
        p1 = self._post({"action": "order.pay", "order_id": order_id,
                         "idempotency_key": "http-pay"})
        p2 = self._post({"action": "order.pay", "order_id": order_id,
                         "idempotency_key": "http-pay"})
        self.assertEqual(p1["state"], "confirmed")
        self.assertTrue(p2["idempotent_replay"])


if __name__ == "__main__":
    unittest.main()
