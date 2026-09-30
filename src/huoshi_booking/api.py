"""供进程内调用的轻量请求适配层（JSON 字符串进、JSON 字符串出）。

支持的 action：

* health、register、find（旧基线）
* zone.upsert / zone.list
* trip.upsert / trip.list / trip.seal
* inventory
* order.book / order.pay / order.cancel / order.reschedule / order.get
* refund.list / refund.complete
* events
* sweep.expired

业务错误统一返回 ``{"error": ..., "error_type": ...}``，HTTP 风格的状态由
调用方（如后续接入的 HTTP 服务器）自行映射。
"""
from __future__ import annotations

import json

from .domain import DomainError, IdempotencyConflict, NotFoundError
from .service import Service


def handle(payload: str, service: Service | None = None) -> str:
    service = service or Service()
    body = json.loads(payload)
    action = body.get("action")
    try:
        result = _dispatch(service, action, body)
    except NotFoundError as exc:
        result = {"error": str(exc), "error_type": "not_found"}
    except IdempotencyConflict as exc:
        result = {"error": str(exc), "error_type": "idempotency_conflict"}
    except DomainError as exc:
        result = {"error": str(exc), "error_type": "domain_error"}
    return json.dumps(result, ensure_ascii=False)


def _dispatch(service: Service, action: str, body: dict):
    if action == "health":
        return service.health()

    if action == "register":
        return service.register(str(body["record_id"]), str(body["owner_id"]))
    if action == "find":
        result = service.find(str(body["record_id"]))
        return result if result is not None else {"error": "记录不存在",
                                                  "error_type": "not_found"}

    if action == "zone.upsert":
        return service.upsert_zone(
            str(body["zone_id"]), str(body["variety"]), str(body["name"]),
            int(body["daily_capacity"]),
            int(body.get("unit_price_fen", 0)))
    if action == "zone.list":
        return {"zones": service.list_zones(body.get("variety"))}

    if action == "trip.upsert":
        return service.upsert_trip(
            str(body["trip_id"]), str(body["boat_route"]),
            str(body["pick_date"]), str(body["time_slot"]),
            int(body["capacity"]))
    if action == "trip.list":
        return {"trips": service.list_trips(body.get("pick_date"),
                                            body.get("status"))}
    if action == "trip.seal":
        return service.seal_trip(
            str(body["trip_id"]), str(body.get("reason", "园区临时封存")),
            str(body.get("mode", "refund")),
            str(body.get("target_trip_id", "")),
            str(body.get("target_pick_date", "")))

    if action == "inventory":
        return service.inventory(str(body["pick_date"]), body.get("variety"))

    if action == "order.book":
        return service.book(
            visitor_id=str(body["visitor_id"]),
            variety=str(body["variety"]),
            zone_id=str(body["zone_id"]),
            pick_date=str(body["pick_date"]),
            trip_id=str(body["trip_id"]),
            quantity=int(body["quantity"]),
            idempotency_key=str(body["idempotency_key"]),
            unit_price_fen=(int(body["unit_price_fen"])
                            if body.get("unit_price_fen") is not None else None))
    if action == "order.pay":
        return service.confirm_payment(
            str(body["order_id"]), str(body["idempotency_key"]),
            str(body.get("payment_ref", "")))
    if action == "order.cancel":
        return service.cancel_order(
            str(body["order_id"]), str(body["idempotency_key"]),
            str(body.get("reason", "visitor_cancelled")))
    if action == "order.reschedule":
        return service.reschedule(
            str(body["order_id"]), str(body["new_pick_date"]),
            str(body["new_trip_id"]), str(body["idempotency_key"]))
    if action == "order.get":
        return service.get_order(str(body["order_id"]))

    if action == "refund.list":
        return {"refunds": service.list_refunds(str(body.get("status", "")))}
    if action == "refund.complete":
        return service.complete_refund(str(body["refund_id"]))

    if action == "events":
        return {"events": service.list_events(
            str(body.get("order_id", "")), str(body.get("trip_id", "")),
            int(body.get("after_seq", 0)), int(body.get("limit", 1000)))}

    if action == "sweep.expired":
        kwargs = {}
        if body.get("timeout_minutes") is not None:
            kwargs["timeout_minutes"] = int(body["timeout_minutes"])
        return service.sweep_expired(**kwargs)

    raise ValueError(f"不支持的请求动作：{action!r}")
