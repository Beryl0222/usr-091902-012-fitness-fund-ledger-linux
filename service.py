"""公共健身设施运营服务。

HTTP 入口把场地、器材、开放时段、无障碍能力、公益金批次与维保责任串起来：
闸机/巡检事件幂等摄取，闭馆与天气预警联动容量并通知预约，
公益金以哈希链式台账记录用途、验收证据与资产变化。
"""

import argparse
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from domain import DomainError, Store

SERVICE_ID = "fitness-fund-ledger"
SERVICE_NAME = "公共健身设施运营"

DATA_DIR = os.environ.get("FITNESS_DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


class Handler(BaseHTTPRequestHandler):
    store: Store = None  # 由 main 注入，测试中可覆盖类属性

    # ---------- HTTP 基础 ----------

    def _send_json(self, payload, status: int = 200):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise DomainError("bad_json", "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise DomainError("bad_payload", "请求体须为 JSON 对象")
        return data

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _resolve_route(self, method: str, path: str):
        """返回 (handler, kwargs)；未知路由返回 None（与存储是否就绪无关，一律 404）。"""
        routes = self._routes()
        for allowed, pattern, handler in routes:
            if method != allowed:
                continue
            match = pattern.fullmatch(path)
            if match:
                return handler, match.groupdict()
        return None

    def _routes(self):
        return [
            ("GET", re.compile(r"/health"), lambda **_: health_payload()),
            ("POST", re.compile(r"/admin/venues"), lambda **_: (201, self.store.create_venue(self._read_json()))),
            ("POST", re.compile(r"/venues/(?P<venue_id>[^/]+)/equipment"),
             lambda venue_id, **_: (201, self.store.add_equipment(venue_id, self._read_json()))),
            ("POST", re.compile(r"/events"), lambda **_: (200, self.store.ingest_event(self._read_json()))),
            ("POST", re.compile(r"/bookings"), lambda **_: (201, self.store.create_booking(self._read_json()))),
            ("POST", re.compile(r"/bookings/(?P<booking_id>[^/]+)/cancel"),
             lambda booking_id, **_: (200, self.store.cancel_booking(booking_id))),
            ("GET", re.compile(r"/notifications"), lambda **_: (200, {
                "notifications": self.store.list_notifications(self._query.get("contact", ""))})),
            ("POST", re.compile(r"/closures"), lambda **_: (201, self.store.add_closure(self._read_json()))),
            ("GET", re.compile(r"/venues/(?P<venue_id>[^/]+)/availability"),
             lambda venue_id, **_: (200, self.store.availability(
                 venue_id, self._query.get("start"), self._query.get("end")))),
            ("GET", re.compile(r"/tickets"), lambda **_: (200, {
                "tickets": self.store.list_tickets(self._query.get("status"), self._query.get("venue_id"))})),
            ("POST", re.compile(r"/tickets/(?P<ticket_id>[^/]+)/advance"),
             lambda ticket_id, **_: (200, self.store.advance_ticket(ticket_id, self._read_json()))),
            ("POST", re.compile(r"/funds/batches"), lambda **_: (201, self.store.create_batch(self._read_json()))),
            ("GET", re.compile(r"/funds/batches/(?P<batch_id>[^/]+)/balance"),
             lambda batch_id, **_: (200, self.store.batch_balance(batch_id))),
            ("POST", re.compile(r"/funds/entries"), lambda **_: (201, self.store.append_entry(self._read_json()))),
            ("GET", re.compile(r"/funds/audit"), lambda **_: (200, self.store.audit_trail(
                self._query.get("batch_id"), self._query.get("venue_id")))),
            ("GET", re.compile(r"/funds/verify"), lambda **_: (200, self.store.verify_ledger())),
            ("POST", re.compile(r"/footfall"), lambda **_: (201, self.store.record_footfall(self._read_json()))),
            ("GET", re.compile(r"/footfall/stats"),
             lambda **_: (200, self.store.footfall_stats(self._query.get("venue_id")))),
            ("POST", re.compile(r"/admin/districts"), lambda **_: (201, self.store.upsert_district(self._read_json()))),
            ("GET", re.compile(r"/coverage"), lambda **_: (200, self.store.coverage_report())),
        ]

    def _dispatch(self, method: str):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        self._query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        try:
            resolved = self._resolve_route(method, path)
            if resolved is None:
                self.send_error(404)
                return
            handler, kwargs = resolved
            if not path.startswith("/health") and self.store is None:
                raise DomainError("not_configured", "存储未初始化", 503)
            result = handler(**kwargs)
            if isinstance(result, tuple):
                status, payload = result
            else:
                status, payload = 200, result
            self._send_json(payload, status)
        except DomainError as error:
            self._send_json({"error": error.code, "message": str(error)}, error.http_status)
        except BrokenPipeError:
            pass

    def log_message(self, *_args):
        return


def build_store(data_dir: str = DATA_DIR) -> Store:
    os.makedirs(data_dir, exist_ok=True)
    return Store(
        state_path=os.path.join(data_dir, "state.json"),
        ledger_path=os.path.join(data_dir, "ledger.log"),
    )


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir", default=DATA_DIR)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        store = build_store(args.data_dir)
        store.verify_ledger()
        print("基础检查通过")
        return
    Handler.store = build_store(args.data_dir)
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
