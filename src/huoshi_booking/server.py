"""基于标准库 http.server 的运营后台 RPC 入口。

启动：``python -m huoshi_booking.server --db data/huoshi.db --port 8080``

所有请求为 ``POST /``，body 与 :func:`huoshi_booking.api.handle` 的 JSON 协议一致。
仅用于内网运营后台/验收演示，生产环境应放到带鉴权的网关之后。
"""
from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .api import handle
from .service import Service
from .store import Store


def build_handler(service: Service):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path not in ("/", "/rpc"):
                self.send_error(404)
                return
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            try:
                out = handle(raw, service)
                code = 200
            except Exception as exc:  # noqa: BLE001 - 适配层兜底
                out = json.dumps(
                    {"error": str(exc), "error_type": "bad_request"},
                    ensure_ascii=False)
                code = 400
            data = out.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST

        def log_message(self, fmt, *args):  # 安静一点
            return

    return Handler


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="火柿采摘调度运营后台")
    parser.add_argument("--db", default="huoshi.db", help="SQLite 文件路径")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)

    service = Service(Store(args.db))
    server = ThreadingHTTPServer((args.host, args.port), build_handler(service))
    print(f"huoshi_booking listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        service.store.close()


if __name__ == "__main__":
    main()
