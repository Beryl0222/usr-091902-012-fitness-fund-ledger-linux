"""服务契约与领域场景测试。

覆盖：健康入口、事件幂等补传、闭馆/预警联动与通知、容量占用、
维修队列与资金结算、公益金台账追溯与跨年度处理、客流隐私、重启恢复、台账防篡改。
"""

import json
import os
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from domain import DomainError, Store
from service import Handler, SERVICE_ID, SERVICE_NAME, health_payload


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


class ServiceContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def test_health_payload_has_stable_identity(self):
        self.assertEqual(health_payload(), {"status": "ok", "service": SERVICE_ID, "name": SERVICE_NAME})

    def test_health_endpoint_returns_json(self):
        with urlopen(f"{self.base_url}/health", timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers.get_content_type(), "application/json")
            self.assertEqual(json.load(response), health_payload())

    def test_unknown_route_is_not_exposed(self):
        with self.assertRaises(HTTPError) as error:
            urlopen(f"{self.base_url}/unknown", timeout=2)
        self.assertEqual(error.exception.code, 404)
        error.exception.close()


class DomainTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(
            state_path=os.path.join(self.tmp.name, "state.json"),
            ledger_path=os.path.join(self.tmp.name, "ledger.log"),
        )

    def tearDown(self):
        self.tmp.cleanup()

    def new_venue(self, capacity=100, district="城东", **kwargs):
        return self.store.create_venue({
            "name": "中央体育公园", "kind": "体育公园", "district": district,
            "capacity": capacity, "service_radius_m": 1500,
            "accessibility": {"barrier_free": True}, **kwargs,
        })

    def new_batch(self, bid="TC-2025", year=2025, amount=1_000_000):
        return self.store.create_batch({
            "id": bid, "year": year, "amount": amount,
            "approved_purpose": "公共体育设施建设与维保",
        })

    def spend(self, batch_id, amount, change, purpose="建设支出", evidence=None):
        return self.store.append_entry({
            "type": "expenditure", "batch_id": batch_id, "amount": amount,
            "purpose": purpose,
            "evidence": evidence or {"acceptor": "张工", "document": "YS-001", "accepted_at": "2025-03-01"},
            "asset_change": change,
        })


class EventIdempotencyTest(DomainTestBase):
    def test_gate_offline_backfill_counts_once(self):
        venue = self.new_venue()
        payload = {"event_id": "evt-1", "type": "gate_entry", "venue_id": venue["id"],
                   "ts": iso(datetime(2026, 9, 19, 9, 0, tzinfo=timezone.utc))}
        first = self.store.ingest_event(payload)
        again = self.store.ingest_event(payload)  # 离线后补传同一条
        third = self.store.ingest_event(dict(payload))
        self.assertFalse(first["dup"])
        self.assertTrue(again["dup"])
        self.assertTrue(third["dup"])
        self.assertEqual(self.store.occupancy[venue["id"]], 1)
        stats = self.store.footfall_stats(venue["id"])
        self.assertEqual(sum(r["entries"] for r in stats["rows"]), 1)

    def test_concurrent_same_event_only_one_wins(self):
        venue = self.new_venue()
        results = []

        def fire():
            results.append(self.store.ingest_event({
                "event_id": "evt-race", "type": "gate_entry", "venue_id": venue["id"],
                "ts": iso(datetime.now(timezone.utc)),
            }))

        threads = [threading.Thread(target=fire) for _ in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sum(1 for r in results if not r["dup"]), 1)
        self.assertEqual(self.store.occupancy[venue["id"]], 1)

    def test_exit_below_zero_rejected(self):
        venue = self.new_venue()
        with self.assertRaises(DomainError) as ctx:
            self.store.ingest_event({"event_id": "x-1", "type": "gate_exit", "venue_id": venue["id"]})
        self.assertEqual(ctx.exception.code, "negative_occupancy")
        self.assertNotIn("x-1", self.store.events)  # 被拒绝事件不留计数痕迹

    def test_repeated_fault_inspection_single_ticket(self):
        venue = self.new_venue()
        equip = self.store.add_equipment(venue["id"], {"name": "太空漫步机"})
        for eid in ("insp-1", "insp-2"):
            result = self.store.ingest_event({
                "event_id": eid, "type": "inspection", "venue_id": venue["id"],
                "equipment_id": equip["id"], "fault": True, "detail": "摆动异响",
            })
            self.assertEqual(result["ticket_id"], "R0001")
        self.assertEqual(len(self.store.list_tickets()), 1)
        self.assertEqual(self.store.equipment[equip["id"]]["status"], "停用")


class ClosureAndBookingTest(DomainTestBase):
    def _book(self, venue_id, start, end, size=20, contact="token-abc"):
        return self.store.create_booking({
            "venue_id": venue_id, "start": iso(start), "end": iso(end),
            "party_size": size, "contact": contact,
        })

    def test_closure_changes_capacity_and_notifies_affected_bookings(self):
        venue = self.new_venue(capacity=100)
        start = datetime(2026, 9, 26, 9, tzinfo=timezone.utc)
        booking = self._book(venue["id"], start, start + timedelta(hours=2))
        outcome = self.store.add_closure({
            "kind": "赛事占用", "reason": "市青少年篮球赛",
            "start": iso(start - timedelta(hours=1)), "end": iso(start + timedelta(hours=3)),
            "venue_ids": [venue["id"]],
        })
        notified = [n["booking_id"] for n in outcome["notified_bookings"]]
        self.assertEqual(notified, [booking["id"]])
        self.assertEqual(self.store.bookings[booking["id"]]["status"], "affected")
        notes = self.store.list_notifications("token-abc")
        self.assertEqual(len(notes), 1)
        self.assertIn("赛事占用", notes[0]["message"])
        avail = self.store.availability(venue["id"], iso(start), iso(start + timedelta(hours=2)))
        self.assertEqual(avail["available"], 0)
        self.assertTrue(any("赛事占用" in r for r in avail["reasons"]))

    def test_rainstorm_alert_covers_whole_district(self):
        v1 = self.new_venue(district="江北")
        v2 = self.store.create_venue({"name": "江北百姓健身房", "kind": "百姓健身房",
                                      "district": "江北", "capacity": 50})
        at = datetime(2026, 9, 27, 18, tzinfo=timezone.utc)
        b1 = self._book(v1["id"], at, at + timedelta(hours=1))
        b2 = self._book(v2["id"], at, at + timedelta(hours=1))
        self.store.add_closure({
            "kind": "暴雨预警", "reason": "橙色暴雨预警",
            "start": iso(at - timedelta(minutes=30)), "end": iso(at + timedelta(hours=2)),
            "district": "江北",
        })
        self.assertEqual(self.store.bookings[b1["id"]]["status"], "affected")
        self.assertEqual(self.store.bookings[b2["id"]]["status"], "affected")

    def test_partial_capacity_override_still_admits_small_booking(self):
        venue = self.new_venue(capacity=200)
        at = datetime(2026, 9, 28, 10, tzinfo=timezone.utc)
        self.store.add_closure({
            "kind": "临时闭馆", "reason": "半场维护",
            "start": iso(at), "end": iso(at + timedelta(hours=2)),
            "capacity_override": 30, "venue_ids": [venue["id"]],
        })
        ok = self._book(venue["id"], at + timedelta(minutes=10), at + timedelta(hours=1), size=30)
        self.assertEqual(ok["status"], "confirmed")
        with self.assertRaises(DomainError) as ctx:
            self._book(venue["id"], at + timedelta(minutes=20), at + timedelta(minutes=50), size=31)
        self.assertEqual(ctx.exception.code, "capacity_exceeded")

    def test_concurrent_bookings_count_peak_overlap(self):
        venue = self.new_venue(capacity=50)
        at = datetime(2026, 9, 29, 9, tzinfo=timezone.utc)
        self._book(venue["id"], at, at + timedelta(hours=2), size=30)
        # 与上一时段重叠 1 小时，并发峰值 30+30 > 50
        with self.assertRaises(DomainError) as ctx:
            self._book(venue["id"], at + timedelta(hours=1), at + timedelta(hours=2), size=30)
        self.assertEqual(ctx.exception.code, "capacity_exceeded")
        # 首尾相接不重叠，可以预约
        later = self._book(venue["id"], at + timedelta(hours=2), at + timedelta(hours=3), size=50)
        self.assertEqual(later["status"], "confirmed")


class RepairAndFundTest(DomainTestBase):
    def test_repair_settlement_links_ticket_to_fund_entry_and_restores_asset(self):
        venue = self.new_venue()
        equip = self.store.add_equipment(venue["id"], {"name": "椭圆机"})
        batch = self.new_batch()
        self.store.ingest_event({
            "event_id": "insp-9", "type": "inspection", "venue_id": venue["id"],
            "equipment_id": equip["id"], "fault": True, "detail": "阻力失效",
        })
        ticket = self.store.list_tickets(status="queued")[0]
        self.store.advance_ticket(ticket["id"], {"action": "start", "note": "已派单"})
        self.assertEqual(self.store.equipment[equip["id"]]["status"], "维修中")
        resolved = self.store.advance_ticket(ticket["id"], {
            "action": "resolve",
            "fund_settlement": {
                "batch_id": batch["id"], "amount": 3200,
                "evidence": {"acceptor": "李站长", "document": "WX-2026-09", "accepted_at": "2026-09-22"},
            },
        })
        self.assertEqual(resolved["status"], "resolved")
        self.assertTrue(resolved["settlement_entry_id"])
        self.assertEqual(self.store.equipment[equip["id"]]["status"], "正常")
        balance = self.store.batch_balance(batch["id"])
        self.assertEqual(balance["spent"], 3200)
        self.assertEqual(balance["available"], 1_000_000 - 3200)
        trail = self.store.audit_trail(batch["id"], venue["id"])
        self.assertEqual(len(trail["asset_results"]), 1)
        result = trail["asset_results"][0]
        self.assertEqual(result["asset_now"]["status"], "正常")
        self.assertEqual(result["evidence"]["document"], "WX-2026-09")

    def test_expenditure_requires_purpose_evidence_and_asset_change(self):
        self.new_batch()
        with self.assertRaises(DomainError) as ctx:
            self.store.append_entry({"type": "expenditure", "batch_id": "TC-2025",
                                     "amount": 100, "purpose": "x"})
        self.assertEqual(ctx.exception.code, "missing_evidence")

    def test_overspending_rejected(self):
        batch = self.new_batch(amount=100)
        venue = self.new_venue()
        with self.assertRaises(DomainError):
            self.spend(batch["id"], 101, {"kind": "venue", "id": venue["id"], "action": "create"})

    def test_fund_build_opens_venue_and_audit_follows_money(self):
        batch = self.new_batch()
        venue = self.store.create_venue({
            "name": "南区公园", "kind": "体育公园", "district": "南区", "capacity": 300,
        })
        self.assertEqual(venue["status"], "规划中")
        entry = self.spend(batch["id"], 800_000,
                           {"kind": "venue", "id": venue["id"], "action": "create"},
                           purpose="南区体育公园建设（第一批）")
        self.assertEqual(self.store.venues[venue["id"]]["status"], "开放中")
        self.assertEqual(self.store.venues[venue["id"]]["funded_by"], batch["id"])
        trail = self.store.audit_trail(batch["id"])
        self.assertEqual(trail["entries"][0]["entry_id"], entry["entry_id"])
        self.assertEqual(trail["asset_results"][0]["asset_now"]["status"], "开放中")

    def test_refund_is_independent_entry_not_field_edit(self):
        batch = self.new_batch(amount=1000)
        venue = self.new_venue()
        spent = self.spend(batch["id"], 600, {"kind": "venue", "id": venue["id"], "action": "create"})
        original_amount = spent["amount"]
        self.store.append_entry({
            "type": "return", "batch_id": batch["id"], "amount": 150,
            "purpose": "项目结余缴回（审计核减）",
            "evidence": {"document": "TH-2025-1", "acceptor": "财政局"},
        })
        # 原支出分录像不可变：金额没有被改小
        self.assertEqual([e for e in self.store.ledger if e["entry_id"] == spent["entry_id"]][0]["amount"],
                         original_amount)
        balance = self.store.batch_balance(batch["id"])
        self.assertEqual(balance["spent"], 600)
        self.assertEqual(balance["returned"], 150)
        self.assertEqual(balance["available"], 550)

    def test_carryforward_only_to_later_year(self):
        venue = self.new_venue()
        self.new_batch("TC-2025", 2025, 1000)
        self.spend("TC-2025", 600, {"kind": "venue", "id": venue["id"], "action": "create"})
        self.new_batch("TC-2026", 2026, 2000)
        with self.assertRaises(DomainError) as ctx:
            self.store.append_entry({
                "type": "carryforward", "batch_id": "TC-2025", "to_batch_id": "TC-2025",
                "amount": 400, "purpose": "试图同年结转",
            })
        self.assertEqual(ctx.exception.code, "bad_carryforward")
        self.store.append_entry({
            "type": "carryforward", "batch_id": "TC-2025", "to_batch_id": "TC-2026",
            "amount": 400, "purpose": "跨年度结余结转至2026批次",
        })
        self.assertEqual(self.store.batch_balance("TC-2025")["available"], 0)
        self.assertEqual(self.store.batch_balance("TC-2026")["carry_in"], 400)
        self.assertEqual(self.store.batch_balance("TC-2026")["available"], 2400)


class FootfallPrivacyTest(DomainTestBase):
    def test_personal_identifiers_rejected(self):
        venue = self.new_venue()
        for bad in [{"device_id": "AA-BB"}, {"user": "u1"}, {"nested": {"身份证": "x"}}, {"mac": "00:11"}]:
            with self.assertRaises(DomainError) as ctx:
                self.store.record_footfall({"venue_id": venue["id"], "buckets": [
                    {"bucket": "2026-09-19T09:00", "entries": 12}], **bad})
            self.assertEqual(ctx.exception.code, "privacy_violation")

    def test_aggregate_only_and_stats(self):
        venue = self.new_venue(capacity=100)
        self.store.record_footfall({"venue_id": venue["id"], "buckets": [
            {"bucket": "2026-09-19T09:00", "entries": 40},  # 周六
            {"bucket": "2026-09-19T10:00", "entries": 95},
        ]})
        stats = self.store.footfall_stats(venue["id"])
        self.assertEqual(stats["weekend_peak"]["entries"], 95)
        self.assertAlmostEqual(stats["weekend_peak"]["utilization"], 0.95)
        # 原始数据里没有任何可还原到个人的内容
        with open(os.path.join(self.tmp.name, "state.json"), encoding="utf-8") as fh:
            raw = json.load(fh)
        self.assertEqual(raw["footfall"][f"{venue['id']}|2026-09-19T10:00"], 95)


class PersistenceTest(DomainTestBase):
    def test_restart_preserves_bookings_tickets_occupancy_and_dedup(self):
        venue = self.new_venue()
        equip = self.store.add_equipment(venue["id"], {"name": "蹬力器"})
        self.store.ingest_event({"event_id": "e-1", "type": "gate_entry", "venue_id": venue["id"]})
        self.store.ingest_event({
            "event_id": "i-1", "type": "inspection", "venue_id": venue["id"],
            "equipment_id": equip["id"], "fault": True,
        })
        at = datetime(2026, 10, 1, 8, tzinfo=timezone.utc)
        self.store.create_booking({"venue_id": venue["id"], "start": iso(at),
                                   "end": iso(at + timedelta(hours=1)), "party_size": 10})
        state_path, ledger_path = self.store.state_path, self.store.ledger_path

        reopened = Store(state_path, ledger_path)  # 模拟服务重启
        self.assertEqual(reopened.occupancy[venue["id"]], 1)
        self.assertEqual(len(reopened.list_tickets(status="queued")), 1)
        self.assertEqual(len(reopened.bookings), 1)
        # 重启后闸机/巡检补传仍然幂等
        again = reopened.ingest_event({"event_id": "e-1", "type": "gate_entry", "venue_id": venue["id"]})
        self.assertTrue(again["dup"])
        self.assertEqual(reopened.occupancy[venue["id"]], 1)
        self.assertEqual(reopened.verify_ledger()["entries"], 0)

    def test_ledger_tampering_detected_on_restart(self):
        batch = self.new_batch()
        venue = self.new_venue()
        self.spend(batch["id"], 500, {"kind": "venue", "id": venue["id"], "action": "create"})
        with open(self.store.ledger_path, encoding="utf-8") as fh:
            lines = fh.readlines()
        tampered = json.loads(lines[0])
        tampered["amount"] = 999  # 试图篡改支出金额
        lines[0] = json.dumps(tampered, ensure_ascii=False) + "\n"
        with open(self.store.ledger_path, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
        with self.assertRaises(DomainError) as ctx:
            Store(self.store.state_path, self.store.ledger_path)
        self.assertEqual(ctx.exception.code, "ledger_corrupt")


class CoverageTest(DomainTestBase):
    def test_remote_district_gaps_flagged(self):
        self.store.upsert_district({"name": "远郊山镇", "population": 80_000})
        self.store.create_venue({
            "name": "山镇健身房", "kind": "百姓健身房", "district": "远郊山镇",
            "capacity": 50, "accessibility": {"barrier_free": False},
        })
        report = self.store.coverage_report()
        row = [d for d in report["districts"] if d["district"] == "远郊山镇"][0]
        gaps = "、".join(row["coverage_gaps"])
        self.assertIn("体育公园", gaps)
        self.assertIn("山地步道", gaps)
        self.assertIn("千人容量不足", gaps)
        self.assertIn("无障碍", gaps)


class HttpFlowTest(unittest.TestCase):
    """端到端 HTTP：多公园同时上报容量与故障，闭馆后居民能查到真实可用量与原因。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._prev_store = Handler.store
        Handler.store = Store(
            os.path.join(self.tmp.name, "state.json"),
            os.path.join(self.tmp.name, "ledger.log"),
        )
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        Handler.store = self._prev_store
        self.tmp.cleanup()

    def request(self, method, path, payload=None):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        req = Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"} if data else {},
        )
        try:
            with urlopen(req, timeout=3) as resp:
                return resp.status, json.load(resp)
        except HTTPError as error:
            return error.code, json.load(error)

    def test_resident_sees_real_availability_and_closure_reason(self):
        _, park = self.request("POST", "/admin/venues", {
            "name": "河滨公园", "kind": "体育公园", "district": "河西", "capacity": 120})
        _, room = self.request("POST", "/admin/venues", {
            "name": "河西健身房", "kind": "百姓健身房", "district": "河西", "capacity": 40})
        _, equip = self.request("POST", f"/venues/{park['id']}/equipment", {"name": "双杠"})
        # 两个场地同时上报：公园器材故障、健身房入场
        status, fault = self.request("POST", "/events", {
            "event_id": "gw-1", "type": "inspection", "venue_id": park["id"],
            "equipment_id": equip["id"], "fault": True, "detail": "基座松动"})
        self.assertEqual(status, 200)
        self.request("POST", "/events", {
            "event_id": "gate-1", "type": "gate_entry", "venue_id": room["id"]})
        # 补传不重复
        status, again = self.request("POST", "/events", {
            "event_id": "gate-1", "type": "gate_entry", "venue_id": room["id"]})
        self.assertTrue(again["dup"])
        at = datetime(2026, 10, 3, 14, tzinfo=timezone.utc)
        _, booking = self.request("POST", "/bookings", {
            "venue_id": park["id"], "start": iso(at), "end": iso(at + timedelta(hours=1)),
            "party_size": 15, "contact": "resident-token-7"})
        _, closure = self.request("POST", "/closures", {
            "kind": "空气污染预警", "reason": "AQI 230 重度污染", "district": "河西",
            "start": iso(at - timedelta(minutes=30)), "end": iso(at + timedelta(hours=2))})
        self.assertEqual(len(closure["notified_bookings"]), 1)
        _, notes = self.request("GET", "/notifications?contact=resident-token-7")
        self.assertEqual(len(notes["notifications"]), 1)
        window = f"start={quote(iso(at))}&end={quote(iso(at + timedelta(hours=1)))}"
        _, avail = self.request("GET", f"/venues/{park['id']}/availability?{window}")
        self.assertEqual(avail["available"], 0)
        self.assertTrue(any("空气污染预警" in r for r in avail["reasons"]))
        self.assertEqual(avail["disabled_equipment"][0]["name"], "双杠")
        _, tickets = self.request("GET", f"/tickets?venue_id={park['id']}")
        self.assertEqual(tickets["tickets"][0]["status"], "queued")


if __name__ == "__main__":
    unittest.main()
