"""验收测试：并发预订、跨日改期、重复回调、封存处置与重启一致性。

直接运行：PYTHONPATH=src python3 -m unittest tests.test_acceptance -v
"""
from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from huoshi_booking.api import handle
from huoshi_booking.domain import DomainError
from huoshi_booking.service import Service
from huoshi_booking.store import Store


def boot(path: str = ":memory:") -> Service:
    service = Service(Store(path))
    # 扁柿、火柿、方柿各一个树区，日容量 2
    for zone_id, variety in (("z-flat", "flat"), ("z-fire", "fire"),
                             ("z-square", "square")):
        service.maintain_zone(zone_id, variety, f"{zone_id} 区", 2)
    return service


def schedule_day(service: Service, date: str) -> None:
    service.schedule_trip(f"t-{date}-a", date, "route-A", 2)
    service.schedule_trip(f"t-{date}-b", date, "route-B", 1)


class 容量与预订测试(unittest.TestCase):
    def setUp(self) -> None:
        self.service = boot()
        schedule_day(self.service, "2026-10-01")

    def test_库存视图随预订变化(self):
        cap = self.service.capacity("fire", "2026-10-01")
        self.assertEqual((cap["capacity"], cap["held"], cap["available"]),
                         (2, 0, 2))
        self.service.create_booking("v1", "fire", "2026-10-01",
                                    "t-2026-10-01-a", amount=50,
                                    idempotency_key="k1")
        cap = self.service.capacity("fire", "2026-10-01")
        self.assertEqual((cap["held"], cap["available"]), (1, 1))

    def test_树区名额与船座位都不会超卖(self):
        # 火柿日容量 2，A 船座位 2：第三个人必须失败
        self.service.create_booking("v1", "fire", "2026-10-01",
                                    "t-2026-10-01-a", idempotency_key="k1")
        self.service.create_booking("v2", "fire", "2026-10-01",
                                    "t-2026-10-01-a", idempotency_key="k2")
        with self.assertRaises(DomainError) as cm:
            self.service.create_booking("v3", "fire", "2026-10-01",
                                        "t-2026-10-01-a", idempotency_key="k3")
        self.assertEqual(cm.exception.code, "sold_out")

        # 船座位比树区容量先耗尽的场景：扁柿容量 2，但 B 船只有 1 座
        self.service.create_booking("v4", "flat", "2026-10-01",
                                    "t-2026-10-01-b", idempotency_key="k4")
        with self.assertRaises(DomainError) as cm:
            self.service.create_booking("v5", "flat", "2026-10-01",
                                        "t-2026-10-01-b", idempotency_key="k5")
        self.assertEqual(cm.exception.code, "trip_full")

    def test_同游客不同幂等键不能重复占位(self):
        self.service.create_booking("v1", "fire", "2026-10-01",
                                    "t-2026-10-01-a", idempotency_key="k1")
        with self.assertRaises(DomainError) as cm:
            self.service.create_booking("v1", "fire", "2026-10-01",
                                        "t-2026-10-01-b", idempotency_key="k2")
        self.assertEqual(cm.exception.code, "duplicate_active")

    def test_幂等键重放请求体不一致被拒绝(self):
        self.service.create_booking("v1", "fire", "2026-10-01",
                                    "t-2026-10-01-a", amount=50,
                                    idempotency_key="k1")
        with self.assertRaises(DomainError) as cm:
            self.service.create_booking("v1", "flat", "2026-10-01",
                                        "t-2026-10-01-a", amount=50,
                                        idempotency_key="k1")
        self.assertEqual(cm.exception.code, "idempotency_conflict")

    def test_封存船班不能再下单(self):
        self.service.seal_trip("t-2026-10-01-b")
        with self.assertRaises(DomainError) as cm:
            self.service.create_booking("v9", "flat", "2026-10-01",
                                        "t-2026-10-01-b", idempotency_key="k9")
        self.assertEqual(cm.exception.code, "trip_sealed")


class 并发预订测试(unittest.TestCase):
    def setUp(self) -> None:
        self.service = boot()
        schedule_day(self.service, "2026-10-01")

    def test_同一幂等键并发只产生一次扣减(self):
        def submit(_: int) -> dict:
            return self.service.create_booking(
                "v1", "fire", "2026-10-01", "t-2026-10-01-a",
                amount=50, idempotency_key="same-key")

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(submit, range(16)))

        booking_ids = {r["booking_id"] for r in results}
        self.assertEqual(len(booking_ids), 1)
        self.assertEqual(self.service.capacity("fire", "2026-10-01")["held"], 1)
        first = [r for r in results if not r["replayed"]]
        self.assertEqual(len(first), 1)
        self.assertEqual(sum(1 for r in results if r["replayed"]), 15)

    def test_不同游客争抢最后名额不超卖(self):
        # 火柿只剩 1 个名额，10 个游客并发抢
        self.service.create_booking("v-hold", "fire", "2026-10-01",
                                    "t-2026-10-01-a", idempotency_key="hold")

        def submit(i: int) -> str:
            try:
                self.service.create_booking(
                    f"v{i}", "fire", "2026-10-01", "t-2026-10-01-a",
                    idempotency_key=f"key-{i}")
                return "ok"
            except DomainError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(submit, range(10)))

        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(outcomes.count("sold_out"), 9)
        self.assertEqual(self.service.capacity("fire", "2026-10-01")["held"], 2)
        trip = self.service.trip_view("t-2026-10-01-a")
        self.assertEqual(trip["held"], 2)
        self.assertEqual(trip["available"], 0)

    def test_多连接并发仍由数据库串行化(self):
        tmp = Path(tempfile.mkdtemp()) / "busy.db"
        seed = boot(str(tmp))
        schedule_day(seed, "2026-10-01")
        seed.store.connection.close()

        def submit(i: int) -> str:
            service = Service(Store(str(tmp)))
            try:
                service.create_booking(
                    f"w{i}", "square", "2026-10-01", "t-2026-10-01-a",
                    idempotency_key=f"wkey-{i}")
                return "ok"
            except DomainError as exc:
                return exc.code
            finally:
                service.store.connection.close()

        with ThreadPoolExecutor(max_workers=6) as pool:
            outcomes = list(pool.map(submit, range(6)))

        checker = Service(Store(str(tmp)))
        try:
            self.assertEqual(outcomes.count("ok"), 2)
            self.assertEqual(outcomes.count("sold_out"), 4)
            self.assertEqual(checker.capacity("square", "2026-10-01")["held"], 2)
        finally:
            checker.store.connection.close()


class 付款与重复回调测试(unittest.TestCase):
    def setUp(self) -> None:
        self.service = boot()
        schedule_day(self.service, "2026-10-01")
        self.booking = self.service.create_booking(
            "v1", "fire", "2026-10-01", "t-2026-10-01-a",
            amount=88, idempotency_key="bk-1")

    def test_重复回调只入账一次(self):
        for _ in range(3):
            result = self.service.confirm_payment(
                self.booking["booking_id"], 88, idempotency_key="pay-1")
        self.assertEqual(result["status"], "paid")
        self.assertTrue(result["replayed"])

        row = self.service.store.connection.execute(
            "SELECT COUNT(*) AS n FROM payment").fetchone()
        self.assertEqual(row["n"], 1)
        events = self.service.list_events(
            aggregate_type="booking", aggregate_id=self.booking["booking_id"])
        types = [e["event_type"] for e in events]
        self.assertEqual(types.count("payment.confirmed"), 1)
        self.assertEqual(types.count("payment.duplicate"), 2)

    def test_金额不一致拒绝付款(self):
        with self.assertRaises(DomainError) as cm:
            self.service.confirm_payment(self.booking["booking_id"], 1,
                                         idempotency_key="pay-bad")
        self.assertEqual(cm.exception.code, "amount_mismatch")
        self.assertEqual(self.service.get_booking(
            self.booking["booking_id"])["status"], "reserved")

    def test_并发重复回调安全(self):
        def pay(_: int) -> str:
            return self.service.confirm_payment(
                self.booking["booking_id"], 88,
                idempotency_key="pay-race")["status"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(pay, range(8)))
        self.assertEqual(set(statuses), {"paid"})
        row = self.service.store.connection.execute(
            "SELECT COUNT(*) AS n FROM payment").fetchone()
        self.assertEqual(row["n"], 1)


class 取消与改期测试(unittest.TestCase):
    def setUp(self) -> None:
        self.service = boot()
        schedule_day(self.service, "2026-10-01")
        schedule_day(self.service, "2026-10-02")

    def test_取消已付款订单生成退款待办并释放名额(self):
        booking = self.service.create_booking(
            "v1", "fire", "2026-10-01", "t-2026-10-01-a",
            amount=88, idempotency_key="bk-1")
        self.service.confirm_payment(booking["booking_id"], 88,
                                     idempotency_key="pay-1")
        result = self.service.cancel_booking(booking["booking_id"],
                                             idempotency_key="cancel-1")
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(self.service.capacity("fire", "2026-10-01")["held"], 0)
        refunds = self.service.list_refunds("pending")
        self.assertEqual(len(refunds), 1)
        self.assertEqual(refunds[0]["amount"], 88)
        self.assertEqual(refunds[0]["reason"], "cancel")

    def test_跨日改期原子迁移名额且可重放(self):
        booking = self.service.create_booking(
            "v1", "fire", "2026-10-01", "t-2026-10-01-a",
            amount=88, idempotency_key="bk-1")

        for i in range(2):  # 第二次是重复提交
            moved = self.service.reschedule_booking(
                booking["booking_id"], "2026-10-02", "t-2026-10-02-a",
                idempotency_key="rs-1")
        self.assertTrue(moved["replayed"])
        self.assertEqual(moved["date"], "2026-10-02")
        self.assertEqual(moved["trip_id"], "t-2026-10-02-a")

        self.assertEqual(self.service.capacity("fire", "2026-10-01")["held"], 0)
        self.assertEqual(self.service.capacity("fire", "2026-10-02")["held"], 1)
        events = self.service.list_events(
            aggregate_type="booking", aggregate_id=booking["booking_id"])
        self.assertEqual(
            [e["event_type"] for e in events].count("booking.rescheduled"), 1)

    def test_目标日满员时改期失败且订单不动(self):
        self.service.create_booking("a", "fire", "2026-10-02",
                                    "t-2026-10-02-a", idempotency_key="a")
        self.service.create_booking("b", "fire", "2026-10-02",
                                    "t-2026-10-02-a", idempotency_key="b")
        booking = self.service.create_booking(
            "v1", "fire", "2026-10-01", "t-2026-10-01-a", idempotency_key="c")
        with self.assertRaises(DomainError) as cm:
            self.service.reschedule_booking(
                booking["booking_id"], "2026-10-02", "t-2026-10-02-a",
                idempotency_key="rs-fail")
        self.assertEqual(cm.exception.code, "sold_out")
        still = self.service.get_booking(booking["booking_id"])
        self.assertEqual((still["date"], still["trip_id"]),
                         ("2026-10-01", "t-2026-10-01-a"))
        self.assertEqual(self.service.capacity("fire", "2026-10-01")["held"], 1)


class 船班封存测试(unittest.TestCase):
    def setUp(self) -> None:
        self.service = boot()
        # 封存场景需要 4 个已付款 + 1 个占位同在 A 船，放大火柿日容量
        self.service.maintain_zone("z-fire", "fire", "火柿区", 5)
        self.date = "2026-10-01"
        # A 船（route-A，5 座）将被封存；B 同路线 2 座；C 其它路线 1 座
        self.service.schedule_trip("t-A", self.date, "route-A", 5)
        self.service.schedule_trip("t-B", self.date, "route-A", 2)
        self.service.schedule_trip("t-C", self.date, "route-C", 1)

    def _book(self, bid: str, visitor: str, variety: str = "fire",
              amount: int = 88) -> str:
        self.service.create_booking(
            visitor, variety, self.date, "t-A", amount=amount,
            idempotency_key=f"k-{bid}", booking_id=bid)
        self.service.confirm_payment(bid, amount, idempotency_key=f"p-{bid}")
        return bid

    def test_按同路线优先迁移_满员后退款或取消(self):
        b1 = self._book("bk-01", "v1")
        b2 = self._book("bk-02", "v2")
        b3 = self._book("bk-03", "v3")
        b4 = self._book("bk-04", "v4")
        # 未付款占位：无处可去时直接取消
        self.service.create_booking(
            "v5", "fire", self.date, "t-A", idempotency_key="k-bk-05",
            booking_id="bk-05")

        result = self.service.seal_trip("t-A")

        self.assertEqual(result["status"], "sealed")
        migrated = {m["booking_id"]: m["to_trip_id"]
                    for m in result["migrated"]}
        self.assertEqual(migrated[b1], "t-B")   # 同路线优先
        self.assertEqual(migrated[b2], "t-B")
        self.assertEqual(migrated[b3], "t-C")   # B 满，转其它路线
        self.assertNotIn(b4, migrated)

        self.assertEqual([r["booking_id"] for r in result["refunds"]], [b4])
        refund = result["refunds"][0]
        self.assertEqual(refund["reason"], "seal_no_capacity")
        self.assertEqual([c["booking_id"] for c in result["cancelled"]],
                         ["bk-05"])

        # 库存视角：A 已清空，B 两座、C 一座；火柿总占位 3（b4 进退款、b5 取消）
        self.assertEqual(self.service.trip_view("t-A")["held"], 0)
        self.assertEqual(self.service.trip_view("t-B")["held"], 2)
        self.assertEqual(self.service.trip_view("t-C")["held"], 1)
        self.assertEqual(self.service.capacity("fire", self.date)["held"], 3)
        self.assertEqual(self.service.get_booking(b4)["status"],
                         "refund_pending")

        # 退款打款后待办与订单都终结
        self.service.mark_refund_paid(refund["refund_id"],
                                      idempotency_key="rf-1")
        self.assertEqual(self.service.list_refunds("pending"), [])
        paid_task = self.service.list_refunds("paid")[0]
        self.assertEqual(paid_task["refund_id"], refund["refund_id"])
        self.assertEqual(self.service.get_booking(b4)["status"], "refunded")

    def test_当日无其它船班时退款原因为_no_trip(self):
        # 单独的日子，只有一艘船
        self.service.schedule_trip("t-solo", "2026-10-03", "route-X", 2)
        bid = self.service.create_booking(
            "v9", "square", "2026-10-03", "t-solo", amount=66,
            idempotency_key="k-solo", booking_id="bk-solo")["booking_id"]
        self.service.confirm_payment(bid, 66, idempotency_key="p-solo")
        result = self.service.seal_trip("t-solo")
        self.assertEqual(result["refunds"][0]["reason"], "seal_no_trip")

    def test_重复封存被拒绝(self):
        self.service.seal_trip("t-C")
        with self.assertRaises(DomainError) as cm:
            self.service.seal_trip("t-C")
        self.assertEqual(cm.exception.code, "trip_already_sealed")


class 事件顺序与重启一致性测试(unittest.TestCase):
    def test_服务重启后库存订单退款与事件顺序保持一致(self):
        tmp = Path(tempfile.mkdtemp()) / "persist.db"

        service = Service(Store(str(tmp)))
        service.maintain_zone("z-fire", "fire", "火柿区", 2)
        service.schedule_trip("t1", "2026-10-01", "route-A", 2)
        service.schedule_trip("t2", "2026-10-02", "route-A", 2)
        booking = service.create_booking(
            "v1", "fire", "2026-10-01", "t1", amount=88,
            idempotency_key="k1", booking_id="bk-x")["booking_id"]
        service.confirm_payment(booking, 88, idempotency_key="p1")
        service.reschedule_booking(booking, "2026-10-02", "t2",
                                   idempotency_key="rs1")
        service.seal_trip("t2")
        service.store.connection.close()

        # —— 重启 ——
        revived = Service(Store(str(tmp)))
        try:
            self.assertEqual(revived.capacity("fire", "2026-10-01")["held"], 0)
            self.assertEqual(revived.capacity("fire", "2026-10-02")["held"], 0)
            order = revived.get_booking(booking)
            self.assertEqual(order["status"], "refund_pending")
            self.assertEqual(order["date"], "2026-10-02")
            refunds = revived.list_refunds("pending")
            self.assertEqual(len(refunds), 1)
            self.assertEqual(refunds[0]["booking_id"], booking)
            self.assertEqual(refunds[0]["amount"], 88)

            events = revived.list_events(
                aggregate_type="booking", aggregate_id=booking)
            self.assertEqual(
                [e["event_type"] for e in events],
                ["booking.created", "payment.confirmed",
                 "booking.rescheduled", "booking.refund_due"],
            )
            # 全局事件 seq 严格递增、无缺口
            all_events = revived.list_events()
            seqs = [e["seq"] for e in all_events]
            self.assertEqual(seqs, list(range(1, len(seqs) + 1)))

            # 重启后用旧幂等键重放，得到同一结果且不产生新扣减
            replay = revived.create_booking(
                "v1", "fire", "2026-10-01", "t1", amount=88,
                idempotency_key="k1", booking_id="bk-x")
            self.assertTrue(replay["replayed"])
            self.assertEqual(replay["booking_id"], booking)
        finally:
            revived.store.connection.close()


class API适配层测试(unittest.TestCase):
    def test_完整链路通过JSON接口重放(self):
        service = boot()
        schedule_day(service, "2026-10-01")

        def call(body: dict) -> dict:
            return json.loads(handle(json.dumps(body, ensure_ascii=False),
                                     service))

        booking = call({"action": "booking_create", "visitor_id": "v1",
                        "variety": "fire", "date": "2026-10-01",
                        "trip_id": "t-2026-10-01-a", "amount": 88,
                        "idempotency_key": "k1"})
        self.assertEqual(booking["status"], "reserved")

        paid = call({"action": "payment_confirm",
                     "booking_id": booking["booking_id"], "amount": 88,
                     "idempotency_key": "p1"})
        self.assertEqual(paid["status"], "paid")

        # 业务错误走错误信封而不是抛异常
        bad = call({"action": "booking_create", "visitor_id": "v2",
                    "variety": "fire", "date": "2026-10-01",
                    "trip_id": "t-2026-10-01-a", "idempotency_key": "k2"})
        # 容量 2、船 2 座，再来两位：第二位起售罄
        self.assertNotIn("error", bad)
        bad2 = call({"action": "booking_create", "visitor_id": "v3",
                     "variety": "fire", "date": "2026-10-01",
                     "trip_id": "t-2026-10-01-a", "idempotency_key": "k3"})
        self.assertEqual(bad2["error"]["code"], "sold_out")

        self.assertEqual(call({"action": "capacity", "variety": "fire",
                               "date": "2026-10-01"})["held"], 2)
        self.assertEqual(call({"action": "events", "after_seq": 0})[0]["seq"],
                         1)
        self.assertEqual(call({"action": "unknown"})["error"]["code"],
                         "unknown_action")
        self.assertEqual(call({"action": "booking_create"})["error"]["code"],
                         "bad_request")


if __name__ == "__main__":
    unittest.main()
