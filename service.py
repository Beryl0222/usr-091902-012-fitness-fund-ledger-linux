"""公共健身设施运营的 HTTP 入口。

仅依赖 Python 标准库。/health 保持原有稳定契约；其余路由把请求转交给
ops.OpsService 领域服务。数据目录可用 --data-dir 或环境变量
FITNESS_DATA_DIR 指定（默认 ./data）。
"""

import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from ops import (
    ConflictError,
    NotFoundError,
    OpsError,
    OpsService,
)

SERVICE_ID = "fitness-fund-ledger"
SERVICE_NAME = "公共健身设施运营"
DEFAULT_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def health_payload():
    """返回稳定的服务身份信息。"""
    return {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME}


# 同一数据目录复用一个服务实例（内存投影 + 文件句柄）
_SERVICES: dict[str, OpsService] = {}


def get_service(data_dir: str) -> OpsService:
    if data_dir not in _SERVICES:
        _SERVICES[data_dir] = OpsService(data_dir)
    return _SERVICES[data_dir]


def make_handler(data_dir: str):
    """生成绑定指定数据目录的 Handler 类，便于测试隔离。"""

    class Handler(BaseHTTPRequestHandler):
        """提供健康检查与运营领域接口，供本地联调和运维巡检使用。"""

        def _send(self, status: int, payload: dict | list):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                raise OpsError("请求体不是合法 JSON")
            if not isinstance(data, dict):
                raise OpsError("请求体必须是 JSON 对象")
            return data

        def _error(self, exc: Exception):
            if isinstance(exc, OpsError):
                self._send(400, {"error": "bad_request", "message": str(exc)})
            elif isinstance(exc, NotFoundError):
                self._send(404, {"error": "not_found", "message": str(exc)})
            elif isinstance(exc, ConflictError):
                self._send(409, {"error": "conflict", "message": str(exc)})
            else:
                raise exc

        def _not_found(self):
            self._send(404, {"error": "not_found", "message": "未知路由"})

        # -- 路由分发 -----------------------------------------------------

        def do_GET(self):
            try:
                parts = urlsplit(self.path)
                path, qs = parts.path.rstrip("/") or "/", parse_qs(parts.query)
                svc = get_service(data_dir)
                if path == "/health":
                    self._send(200, health_payload())
                elif path == "/facilities":
                    self._send(200, svc.list_facilities())
                elif path.startswith("/facilities/"):
                    self._send(200, svc.facility_detail(path.rsplit("/", 1)[1]))
                elif path == "/fund/batches":
                    self._send(200, svc.list_fund_batches())
                elif path.endswith("/trace") and path.startswith("/fund/batches/"):
                    bid = path.split("/")[3]
                    self._send(200, svc.fund_trace(bid))
                elif path == "/expenditures":
                    self._send(200, svc.list_expenditures())
                elif path == "/bookings":
                    self._send(200, {"bookings": list(svc.bookings.values())})
                elif path == "/closures":
                    self._send(200, {"closures": list(svc.closures.values())})
                elif path == "/work-orders":
                    self._send(200, svc.maintenance_queue())
                elif path == "/notifications":
                    self._send(200, svc.notifications_list(
                        qs.get("booking_id", [None])[0]))
                elif path == "/visits":
                    self._send(200, svc.visits_by_bucket(
                        qs.get("facility_id", [None])[0]))
                elif path == "/coverage":
                    radius = int(qs.get("radius_m", ["1000"])[0])
                    self._send(200, svc.coverage_analysis(radius))
                elif path == "/availability":
                    query = {
                        "start": qs.get("start", [None])[0],
                        "end": qs.get("end", [None])[0],
                        "district": qs.get("district", [None])[0],
                        "kind": qs.get("kind", [None])[0],
                        "accessible_only": qs.get("accessible_only", [""])[0]
                        in ("1", "true", "yes"),
                    }
                    self._send(200, svc.availability(query))
                elif path == "/audit":
                    self._send(200, svc.audit_overview())
                else:
                    self._not_found()
            except Exception as exc:  # noqa: BLE001 - 统一映射领域错误
                self._error(exc)

        def do_POST(self):
            try:
                parts = urlsplit(self.path)
                path = parts.path.rstrip("/")
                data = self._read_json()
                svc = get_service(data_dir)

                # 场地与器材
                if path == "/facilities":
                    self._send(201, svc.register_facility(data))
                elif path == "/equipment":
                    self._send(201, svc.register_equipment(data))
                elif path.startswith("/facilities/"):
                    fid, action = path.split("/")[2], path.split("/")[3:]
                    if action == ["schedule"]:
                        self._send(200, svc.update_facility_schedule(
                            fid, data.get("hours", [])))
                    elif action == ["accessibility"]:
                        self._send(200, svc.update_facility_accessibility(
                            fid, data.get("accessible", False),
                            data.get("note", "")))
                    elif action == ["maintenance"]:
                        self._send(200, svc.update_maintenance(fid, data))
                    elif action == ["open"]:
                        self._send(200, svc.open_facility(fid))
                    elif action == ["retire"]:
                        self._send(200, svc.retire_facility(fid))
                    else:
                        self._not_found()
                # 资金
                elif path == "/fund/batches":
                    self._send(201, svc.create_fund_batch(data))
                elif path == "/expenditures":
                    self._send(201, svc.record_expenditure(data))
                elif path.startswith("/expenditures/"):
                    eid, action = path.split("/")[2], path.split("/")[3:]
                    if action == ["accept"]:
                        data["expenditure_id"] = eid
                        self._send(200, svc.accept_expenditure(data))
                    elif action == ["return"]:
                        data["expenditure_id"] = eid
                        self._send(200, svc.return_expenditure(data))
                    else:
                        self._not_found()
                elif path.startswith("/fund/batches/") and path.endswith("/carry-over"):
                    self._send(200, svc.carry_over_batch(path.split("/")[3]))
                # 设备事件（幂等接入）
                elif path == "/events/visits":
                    self._send(200, svc.ingest_visit(data))
                elif path == "/events/inspections":
                    self._send(200, svc.ingest_inspection(data))
                # 预约 / 闭馆 / 预警
                elif path == "/bookings":
                    self._send(201, svc.create_booking(data))
                elif path.startswith("/bookings/") and path.endswith("/cancel"):
                    self._send(200, svc.cancel_booking(path.split("/")[2]))
                elif path == "/closures":
                    self._send(201, svc.add_closure(data))
                elif path.startswith("/closures/") and path.endswith("/lift"):
                    self._send(200, svc.lift_closure(path.split("/")[2]))
                elif path == "/forecasts":
                    self._send(201, svc.add_forecast(data))
                elif path.startswith("/forecasts/") and path.endswith("/lift"):
                    self._send(200, svc.lift_forecast(path.split("/")[2]))
                # 维修
                elif path == "/work-orders":
                    self._send(201, svc.create_work_order(data))
                elif path.startswith("/work-orders/"):
                    wid, action = path.split("/")[2], path.split("/")[3:]
                    if action == ["start"]:
                        self._send(200, svc.start_work_order(wid))
                    elif action == ["complete"]:
                        data["work_order_id"] = wid
                        self._send(200, svc.complete_work_order(data))
                    else:
                        self._not_found()
                # 覆盖分析
                elif path == "/demand-points":
                    self._send(201, svc.register_demand_point(data))
                else:
                    self._not_found()
            except Exception as exc:  # noqa: BLE001 - 统一映射领域错误
                self._error(exc)

        def log_message(self, *_args):
            return

    return Handler


# 默认 Handler，保持 `from service import Handler` 的既有契约
Handler = make_handler(os.environ.get("FITNESS_DATA_DIR", DEFAULT_DATA_DIR))


def main():
    parser = argparse.ArgumentParser(description=SERVICE_NAME)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--data-dir",
                        default=os.environ.get("FITNESS_DATA_DIR", DEFAULT_DATA_DIR),
                        help="事件与资金台账数据目录")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        assert health_payload()["service"] == SERVICE_ID
        # 同时验证领域内核可在临时空目录上初始化
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            OpsService(tmp)
        print("基础检查通过")
        return
    handler_cls = make_handler(args.data_dir)
    ThreadingHTTPServer(("0.0.0.0", args.port), handler_cls).serve_forever()


if __name__ == "__main__":
    main()
