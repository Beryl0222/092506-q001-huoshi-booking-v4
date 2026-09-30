"""供进程内调用的轻量请求适配层。

请求为 ``{"action": ..., ...参数}`` 的 JSON 字符串；成功时直接返回服务结果
JSON，业务错误返回 ``{"error": {"code", "message"}}``，参数缺失返回
``bad_request``，方便运营后台与测试统一判定。
"""
from __future__ import annotations

import json
from typing import Any, Callable

from .domain import DomainError
from .service import Service


def _ok(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def _err(code: str, message: str) -> str:
    return json.dumps({"error": {"code": code, "message": message}},
                      ensure_ascii=False)


# action -> (处理函数, 必填参数列表, 可选参数映射(名->默认值))
def _routes(service: Service) -> dict[str, tuple[Callable[..., Any], list[str],
                                                 list[str]]]:
    return {
        "health": (service.health, [], []),
        "register": (service.register, ["record_id", "owner_id"], []),
        "find": (service.find, ["record_id"], []),
        "zone_maintain": (service.maintain_zone,
                          ["zone_id", "variety", "name", "daily_capacity"], []),
        "zone_list": (service.list_zones, [], []),
        "zone_view": (service.zone_view, ["zone_id"], []),
        "trip_schedule": (service.schedule_trip,
                          ["trip_id", "date", "route", "seat_capacity"], []),
        "trip_update": (service.update_trip, ["trip_id", "seat_capacity"], []),
        "trip_list": (service.list_trips, [], ["date"]),
        "trip_view": (service.trip_view, ["trip_id"], []),
        "capacity": (service.capacity, ["variety", "date"], []),
        "booking_create": (
            service.create_booking,
            ["visitor_id", "variety", "date", "trip_id", "idempotency_key"],
            ["amount", "booking_id"],
        ),
        "payment_confirm": (service.confirm_payment,
                            ["booking_id", "amount", "idempotency_key"], []),
        "booking_cancel": (service.cancel_booking, ["booking_id"],
                           ["idempotency_key"]),
        "booking_reschedule": (
            service.reschedule_booking,
            ["booking_id", "new_date", "new_trip_id", "idempotency_key"], []),
        "booking_get": (service.get_booking, ["booking_id"], []),
        "trip_seal": (service.seal_trip, ["trip_id"], []),
        "refund_list": (service.list_refunds, [], ["status"]),
        "refund_paid": (service.mark_refund_paid, ["refund_id"],
                        ["idempotency_key"]),
        "refund_failed": (service.mark_refund_failed, ["refund_id"],
                          ["idempotency_key"]),
        "events": (service.list_events, [],
                   ["after_seq", "aggregate_type", "aggregate_id"]),
    }


def handle(payload: str, service: Service | None = None) -> str:
    service = service or Service()
    try:
        body = json.loads(payload)
    except json.JSONDecodeError as exc:
        return _err("bad_json", f"请求不是合法 JSON: {exc}")
    if not isinstance(body, dict) or "action" not in body:
        return _err("bad_request", "请求必须包含 action")

    action = body["action"]
    route = _routes(service).get(action)
    if route is None:
        return _err("unknown_action", f"不支持的请求动作: {action}")
    func, required, optional = route

    kwargs: dict[str, Any] = {}
    try:
        for name in required:
            if name not in body or body[name] is None:
                return _err("bad_request", f"缺少必填参数: {name}")
            kwargs[name] = body[name]
        for name in optional:
            if name in body and body[name] is not None:
                kwargs[name] = body[name]
        result = func(**kwargs)
    except DomainError as exc:
        return _err(exc.code, str(exc))
    except (TypeError, ValueError) as exc:
        return _err("bad_argument", str(exc))
    return _ok(result)
