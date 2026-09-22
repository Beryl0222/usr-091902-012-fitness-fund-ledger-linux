"""HTTP 端到端冒烟测试：验证路由、JSON 编解码与错误码映射。"""

import json
import os
import shutil
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from service import make_handler

SAT_09 = "2026-09-26T09:00:00Z"
SAT_11 = "2026-09-26T11:00:00Z"


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="ops-http-")
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.dir))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)
        shutil.rmtree(cls.dir, ignore_errors=True)

    def call(self, method, path, payload=None):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = Request(f"{self.base}{path}", data=data, method=method,
                      headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=3) as resp:
            return resp.status, json.load(resp)

    def call_expect_error(self, method, path, status, payload=None):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = Request(f"{self.base}{path}", data=data, method=method,
                      headers={"Content-Type": "application/json"})
        with self.assertRaises(HTTPError) as ctx:
            urlopen(req, timeout=3)
        self.assertEqual(ctx.exception.code, status)
        body = json.load(ctx.exception)
        ctx.exception.close()
        return body

    def test_full_lifecycle_over_http(self):
        # 健康检查契约不变
        status, health = self.call("GET", "/health")
        self.assertEqual(health["service"], "fitness-fund-ledger")

        # 登记场地与器材
        _, park = self.call("POST", "/facilities", {
            "id": "HP1", "name": "河滨体育公园", "kind": "体育公园",
            "district": "罗湖区", "community": "翠竹街道",
            "location": {"lat": 22.56, "lon": 114.13},
            "capacity": 80, "state": "开放中",
            "accessible": True, "maintenance_role": "物业",
            "maintenance_org": "河滨物业"})
        self.assertEqual(park["state"], "开放中")
        self.call("POST", "/equipment",
                  {"id": "HE1", "facility_id": "HP1", "name": "腰背按摩器"})

        # 资金 -> 支出 -> 验收关联资产
        self.call("POST", "/fund/batches",
                  {"id": "HB2025", "year": 2025, "amount": 200,
                   "approved_purpose": "场地建设/器材购置"})
        self.call("POST", "/expenditures",
                  {"id": "HX1", "batch_id": "HB2025", "amount": 30,
                   "approved_purpose": "器材购置", "facility_id": "HP1"})
        _, acc = self.call("POST", "/expenditures/HX1/accept", {
            "evidence_ref": "ACC-HTTP-1", "inspector": "林工",
            "asset_changes": [{"kind": "equipment_installed", "ref_id": "HE1"}]})
        self.assertEqual(acc["state"], "已验收")

        # 幂等闸机事件
        visit = {"event_id": "HG-1", "source": "gate-h1", "facility_id": "HP1",
                 "ts": SAT_09, "entries": 15, "exits": 2}
        _, first = self.call("POST", "/events/visits", visit)
        _, second = self.call("POST", "/events/visits", visit)
        self.assertFalse(first["dedup"])
        self.assertTrue(second["dedup"])

        # 预约 -> 闭馆联动通知
        _, booking = self.call("POST", "/bookings", {
            "facility_id": "HP1", "start": SAT_09, "end": SAT_11,
            "party": "河畔舞蹈队", "group_size": 20,
            "notify_contact": "陈队长"})
        _, closure = self.call("POST", "/closures", {
            "facility_id": "HP1", "kind": "临时闭馆",
            "start": SAT_09, "end": SAT_11, "reason": "大型活动彩排"})
        self.assertEqual(closure["notifications"][0]["booking_id"], booking["id"])

        # 居民查询：带关闭原因（按区过滤，避免其他场地干扰排序）
        q = urlencode({"start": SAT_09, "end": SAT_11, "district": "罗湖区"})
        _, avail = self.call("GET", f"/availability?{q}")
        hp1 = next(f for f in avail["facilities"] if f["facility_id"] == "HP1")
        self.assertEqual(hp1["status"], "closed")
        self.assertTrue(any("彩排" in r for r in hp1["reasons"]))

        # 审计：资金可追溯、链路完整
        _, audit = self.call("GET", "/audit")
        self.assertTrue(audit["ledger"]["ok"])
        self.assertTrue(audit["fund_balanced"])
        _, trace = self.call("GET", "/fund/batches/HB2025/trace")
        self.assertEqual(trace["expenditures"][0]["evidence_ref"], "ACC-HTTP-1")

    def test_error_mapping(self):
        # 400：校验失败
        body = self.call_expect_error("POST", "/facilities", 400, {"name": "x"})
        self.assertEqual(body["error"], "bad_request")
        # 404：资源不存在
        self.call_expect_error("POST", "/equipment", 404,
                               {"facility_id": "NOPE", "name": "x"})
        # 400：隐私字段
        self.call_expect_error("POST", "/events/visits", 400,
                               {"event_id": "1", "source": "g",
                                "facility_id": "HP1", "entries": 1,
                                "user_id": "u1"})
        # 404：未知路由
        self.call_expect_error("GET", "/nope", 404)

    def test_data_files_persisted(self):
        # 至少产生一条写入事件后，两份追加日志都应落盘
        self.call("POST", "/facilities", {
            "id": "HP2", "name": "北山百姓健身房", "kind": "百姓健身房",
            "district": "盐田区", "community": "海山街道",
            "location": {"lat": 22.57, "lon": 114.24},
            "capacity": 30, "state": "规划中"})
        self.assertTrue(os.path.exists(os.path.join(self.dir, "events.jsonl")))
        self.call("POST", "/fund/batches",
                  {"id": "HB2026", "year": 2026, "amount": 10,
                   "approved_purpose": "场地建设"})
        self.assertTrue(os.path.exists(os.path.join(self.dir, "fund.jsonl")))


if __name__ == "__main__":
    unittest.main()
