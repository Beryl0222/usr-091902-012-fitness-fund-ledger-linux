"""公共健身设施运营领域测试。

覆盖：幂等补传、隐私最小化、资金用途/验收/退回/结转、台账篡改检测、
容量联动与通知、预警扩散、维修队列、重启恢复、居民可用性与覆盖分析。
"""

import json
import os
import shutil
import tempfile
import unittest

from ops import (
    ConflictError,
    FundLedger,
    NotFoundError,
    OpsError,
    OpsService,
)

SAT_09 = "2026-09-26T09:00:00Z"   # 周六
SAT_11 = "2026-09-26T11:00:00Z"
SAT_12 = "2026-09-26T12:00:00Z"
SAT_14 = "2026-09-26T14:00:00Z"
WIDE_START = "2000-01-01T00:00:00Z"
WIDE_END = "2099-01-01T00:00:00Z"


class OpsTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="ops-test-")
        self.svc = OpsService(self.dir)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def restart(self) -> OpsService:
        """模拟服务重启：从磁盘日志与台账重建全部状态。"""
        self.svc = OpsService(self.dir)
        return self.svc

    # -- 构造小工具 -------------------------------------------------------

    def add_park(self, fid="P1", district="南山区", capacity=100, **kw):
        return self.svc.register_facility({
            "id": fid, "name": f"公园{fid}", "kind": "体育公园",
            "district": district, "community": "沙河街道",
            "location": {"lat": 22.54, "lon": 113.95},
            "capacity": capacity, "state": "开放中",
            "maintenance_org": "南山体育中心", "maintenance_role": "自管",
            **kw,
        })

    def add_gym(self, fid="G1", district="南山区", capacity=40, **kw):
        return self.svc.register_facility({
            "id": fid, "name": f"健身房{fid}", "kind": "百姓健身房",
            "district": district, "community": "粤海街道",
            "location": {"lat": 22.53, "lon": 113.94},
            "capacity": capacity, "state": "开放中", **kw,
        })


class IdempotentIngestTest(OpsTestBase):
    def test_visit_counts_once_under_retry_and_offline_backfill(self):
        self.add_park()
        payload = {"event_id": "G01-20260926-0915", "source": "gate-01",
                   "facility_id": "P1", "ts": SAT_09, "entries": 32, "exits": 10}
        first = self.svc.ingest_visit(payload)
        second = self.svc.ingest_visit(payload)          # 网关重试
        third = self.svc.ingest_visit(payload)           # 离线补传再到一次
        self.assertFalse(first["dedup"])
        self.assertTrue(second["dedup"])
        self.assertTrue(third["dedup"])

        rows = self.svc.visits_by_bucket("P1")["rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["entries"], 32)        # 没有重复计数
        self.assertEqual(rows[0]["exits"], 10)

    def test_same_event_id_from_different_source_is_distinct(self):
        self.add_park()
        base = {"event_id": "001", "facility_id": "P1",
                "ts": SAT_09, "entries": 1, "exits": 0}
        self.svc.ingest_visit({**base, "source": "gate-01"})
        again = self.svc.ingest_visit({**base, "source": "gate-02"})
        self.assertFalse(again["dedup"])
        self.assertEqual(len(self.svc.visits_by_bucket("P1")["rows"]), 1)
        self.assertEqual(self.svc.visits_by_bucket("P1")["rows"][0]["entries"], 2)

    def test_dedup_survives_restart(self):
        self.add_park()
        payload = {"event_id": "G01-1", "source": "gate-01",
                   "facility_id": "P1", "ts": SAT_09, "entries": 5}
        self.svc.ingest_visit(payload)
        svc = self.restart()
        again = svc.ingest_visit(payload)
        self.assertTrue(again["dedup"])
        self.assertEqual(svc.visits_by_bucket("P1")["rows"][0]["entries"], 5)

    def test_inspection_dedup_does_not_open_duplicate_work_order(self):
        self.add_park()
        insp = {"event_id": "INS-7", "source": "pad-02", "facility_id": "P1",
                "ts": SAT_09, "result": "故障", "note": "灯杆倾斜"}
        self.assertFalse(self.svc.ingest_inspection(insp)["dedup"])
        self.assertTrue(self.svc.ingest_inspection(insp)["dedup"])
        wo = self.svc.create_work_order(
            {"facility_id": "P1", "level": "场地级", "description": "灯杆倾斜",
             "source_event_id": "INS-7"})
        self.assertEqual(wo["source_inspection"], "INS-7")


class PrivacyTest(OpsTestBase):
    def test_visit_rejects_identity_fields(self):
        self.add_park()
        for bad_field, value in (("user_id", "u123"), ("id_card", "4403**"),
                                 ("phone", "13800000000"), ("mac", "AA:BB"),
                                 ("device_sn", "sn-1"), ("name", "张三")):
            with self.subTest(bad_field=bad_field):
                with self.assertRaises(OpsError):
                    self.svc.ingest_visit({
                        "event_id": "e1", "source": "g1", "facility_id": "P1",
                        "ts": SAT_09, "entries": 1, bad_field: value})

    def test_aggregates_contain_only_bucket_counts(self):
        self.add_park()
        for i, n in enumerate((12, 18, 5)):
            self.svc.ingest_visit({
                "event_id": f"e{i}", "source": "g1", "facility_id": "P1",
                "ts": SAT_09, "entries": n, "exits": 0})
        report = self.svc.visits_by_bucket()
        self.assertEqual(report["granularity"], "hour")
        for row in report["rows"]:
            self.assertEqual(set(row), {"facility_id", "time_bucket", "entries", "exits"})

    def test_inspection_cannot_carry_person_fields(self):
        self.add_park()
        with self.assertRaises(OpsError):
            self.svc.ingest_inspection({
                "event_id": "i1", "source": "p1", "facility_id": "P1",
                "result": "正常", "inspector_name": "李四"})


class FundLedgerTest(OpsTestBase):
    def _park_and_batch(self, amount=100, purpose="场地建设/器材购置"):
        self.add_park()
        return self.svc.create_fund_batch(
            {"id": "B2024", "year": 2024, "amount": amount,
             "approved_purpose": purpose})

    def test_expenditure_must_match_approved_purpose(self):
        self._park_and_batch()
        with self.assertRaises(OpsError):
            self.svc.record_expenditure({
                "batch_id": "B2024", "amount": 10,
                "approved_purpose": "发放福利"})

    def test_expenditure_cannot_exceed_available_balance(self):
        self._park_and_batch(amount=100)
        self.svc.record_expenditure({"batch_id": "B2024", "amount": 80,
                                     "approved_purpose": "场地建设"})
        with self.assertRaises(ConflictError):
            self.svc.record_expenditure({"batch_id": "B2024", "amount": 21,
                                         "approved_purpose": "场地建设"})

    def test_acceptance_needs_evidence_inspector_and_asset_change(self):
        self._park_and_batch()
        self.svc.record_expenditure({"id": "X1", "batch_id": "B2024",
                                     "amount": 60, "approved_purpose": "场地建设"})
        with self.assertRaises(OpsError):
            self.svc.accept_expenditure({"expenditure_id": "X1"})
        with self.assertRaises(OpsError):
            self.svc.accept_expenditure({"expenditure_id": "X1",
                                         "evidence_ref": "ACC-001",
                                         "inspector": "王工"})  # 缺资产变化

    def test_acceptance_links_money_to_real_assets(self):
        # 场地先以规划态登记，建成验收后才转开放
        self.svc.register_facility({
            "id": "P9", "name": "新建公园", "kind": "体育公园",
            "district": "南山区", "community": "西丽湖",
            "location": {"lat": 22.6, "lon": 113.9}, "capacity": 80,
            "state": "规划中"})
        self.svc.register_equipment({"id": "E9", "facility_id": "P9", "name": "太空漫步机"})
        self.svc.create_fund_batch({"id": "B2025", "year": 2025, "amount": 50,
                                    "approved_purpose": "场地建设/器材购置"})
        self.svc.record_expenditure({"id": "X9", "batch_id": "B2025", "amount": 50,
                                     "approved_purpose": "场地建设",
                                     "facility_id": "P9"})
        self.svc.accept_expenditure({
            "expenditure_id": "X9", "evidence_ref": "ACC-2025-09",
            "inspector": "王工",
            "asset_changes": [
                {"kind": "facility_built", "ref_id": "P9"},
                {"kind": "equipment_installed", "ref_id": "E9"},
                {"kind": "accessibility_added", "ref_id": "P9",
                 "detail": "新增无障碍坡道"}]})

        detail = self.svc.facility_detail("P9")
        self.assertEqual(detail["facility"]["state"], "开放中")
        self.assertTrue(detail["facility"]["accessible"])
        self.assertIn("B2025", detail["facility"]["funded_by"])
        self.assertEqual(self.svc.equipment["E9"].funded_by, "B2025")

        trace = self.svc.fund_trace("B2025")
        self.assertEqual(trace["batch"]["spent_amount"], 50.0)
        link = trace["expenditures"][0]
        self.assertEqual(link["evidence_ref"], "ACC-2025-09")
        kinds = {c["kind"] for c in link["asset_changes"]}
        self.assertEqual(kinds,
                         {"facility_built", "equipment_installed", "accessibility_added"})

    def test_return_leaves_trace_and_keeps_balance(self):
        self._park_and_batch(amount=100)
        self.svc.record_expenditure({"id": "X1", "batch_id": "B2024",
                                     "amount": 30, "approved_purpose": "场地建设"})
        self.svc.return_expenditure({"expenditure_id": "X1", "amount": 5,
                                      "reason": "招标结余上缴"})
        view = self.svc.fund_trace("B2024")["expenditures"][0]["expenditure"]
        self.assertEqual(view["returned_amount"], 5.0)
        self.assertEqual(view["state"], "已拨付")  # 部分退回不改变流程状态

        # 全额退回
        self.svc.return_expenditure({"expenditure_id": "X1", "full": True,
                                      "reason": "项目取消，余款退回"})
        view = self.svc.fund_trace("B2024")["expenditures"][0]["expenditure"]
        self.assertEqual(view["state"], "已退回")
        self.assertEqual(view["returned_amount"], 30.0)

        batch = self.svc.fund_trace("B2024")["batch"]
        self.assertEqual(batch["returned_amount"], 30.0)
        self.assertEqual(batch["committed_amount"], 0.0)
        audit = self.svc.audit_overview()
        self.assertTrue(audit["fund_balanced"])

    def test_return_requires_reason(self):
        self._park_and_batch()
        self.svc.record_expenditure({"id": "X1", "batch_id": "B2024",
                                     "amount": 10, "approved_purpose": "场地建设"})
        with self.assertRaises(OpsError):
            self.svc.return_expenditure({"expenditure_id": "X1", "amount": 10})

    def test_carry_over_locks_batch_but_keeps_amount_visible(self):
        self._park_and_batch(amount=100)
        self.svc.record_expenditure({"id": "X1", "batch_id": "B2024",
                                     "amount": 60, "approved_purpose": "场地建设"})
        carried = self.svc.carry_over_batch("B2024")
        self.assertEqual(carried["carried_over_amount"], 40.0)
        self.assertEqual(carried["state"], "已结转")
        with self.assertRaises(ConflictError):
            self.svc.record_expenditure({"batch_id": "B2024", "amount": 1,
                                         "approved_purpose": "场地建设"})
        # 结转后总额恒等式仍成立
        audit = self.svc.audit_overview()
        self.assertTrue(audit["fund_balanced"])

    def test_tampered_ledger_line_is_detected(self):
        self._park_and_batch()
        # 服务运行期间篡改历史行（模拟直接改字段）
        path = os.path.join(self.dir, "fund.jsonl")
        with open(path, "r", encoding="utf-8") as fh:
            line = fh.readline()
        rec = json.loads(line)
        rec["batch"]["total_cents"] += 99900  # 偷偷把 100 元改成 1099 元
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        result = self.svc.ledger.verify()
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "哈希不匹配")
        self.assertTrue(self.svc.audit_overview()["tampered"])
        # 重启直接拒绝启动，避免在被篡改的数据上继续记账
        with self.assertRaises(ConflictError):
            OpsService(self.dir)

    def test_broken_chain_is_detected(self):
        self._park_and_batch(amount=50)
        self.svc.record_expenditure({"id": "X1", "batch_id": "B2024",
                                     "amount": 10, "approved_purpose": "场地建设"})
        self.svc.record_expenditure({"id": "X2", "batch_id": "B2024",
                                     "amount": 10, "approved_purpose": "场地建设"})
        path = os.path.join(self.dir, "fund.jsonl")
        with open(path, "r", encoding="utf-8") as fh:
            lines = fh.readlines()
        self.assertEqual(len(lines), 3)
        # 删除中间记录制造断链：第 3 条的 prev_hash 将接不上
        with open(path, "w", encoding="utf-8") as fh:
            fh.writelines([lines[0], lines[2]])
        result = self.svc.ledger.verify()
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "链断裂")


class CapacityLinkageTest(OpsTestBase):
    def _booking(self, **kw):
        data = {"facility_id": "P1", "start": SAT_09, "end": SAT_11,
                "party": "晨光太极队", "group_size": 20,
                "notify_contact": "队长 138****"}
        data.update(kw)
        return self.svc.create_booking(data)

    def test_closure_changes_capacity_and_notifies_affected_bookings(self):
        self.add_park(capacity=100)
        booking = self._booking()
        # 同时段另有一个不受影响的预约
        other = self._booking(facility_id="P1", start=SAT_12, end=SAT_14,
                              party="周末足球局")

        result = self.svc.add_closure({
            "facility_id": "P1", "kind": "临时闭馆",
            "start": SAT_09, "end": SAT_12, "reason": "场地积水清理"})
        self.assertEqual(len(result["notifications"]), 1)
        note = result["notifications"][0]
        self.assertEqual(note["booking_id"], booking["id"])
        self.assertIn("场地积水清理", note["reason"])
        self.assertEqual(note["notify_contact"], "队长 138****")

        cap = self.svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "closed")
        self.assertEqual(cap["remaining"], 0)
        self.assertTrue(any("积水" in r for r in cap["reasons"]))

        # 关闭期间不能再预约；闭馆解除后恢复
        with self.assertRaises(ConflictError):
            self._booking(party="后来的球队")
        self.svc.lift_closure(result["closure"]["id"])
        cap = self.svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "open")
        self.assertEqual(cap["remaining"], 80)  # 同时段 20 人占用仍在
        self.assertEqual(other["status"], "有效")

    def test_event_occupation_partially_reduces_capacity(self):
        self.add_park(capacity=100)
        self.svc.add_closure({
            "facility_id": "P1", "kind": "赛事占用",
            "start": SAT_09, "end": SAT_12, "reason": "区青少年田径赛",
            "capacity_ratio": 0.5})
        cap = self.svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "limited")
        self.assertEqual(cap["capacity"], 50)

    def test_booking_respects_remaining_capacity(self):
        self.add_park(capacity=30)
        self._booking(group_size=20)
        with self.assertRaises(ConflictError):
            self._booking(party="篮球协会", group_size=15)

    def test_forecast_reduces_outdoor_only_and_notifies(self):
        self.add_park(capacity=100)
        self.add_gym(capacity=40)
        booking = self._booking()
        out = self.svc.add_forecast({
            "kind": "暴雨", "level": "橙", "scope": "district",
            "scope_id": "南山区", "start": SAT_09, "end": SAT_12})
        self.assertIn("P1", out["affected_facilities"])
        self.assertIn("G1", out["affected_facilities"])  # 室内场也收到提醒
        self.assertEqual(len(out["notifications"]), 1)   # 但只有户外预约受影响
        self.assertEqual(out["notifications"][0]["booking_id"], booking["id"])

        avail = self.svc.availability({"start": SAT_09, "end": SAT_11})
        park = next(f for f in avail["facilities"] if f["facility_id"] == "P1")
        gym = next(f for f in avail["facilities"] if f["facility_id"] == "G1")
        self.assertEqual(park["status"], "limited")
        self.assertEqual(park["capacity"], 50)
        self.assertEqual(gym["status"], "open")
        self.assertTrue(any("室内" in r for r in gym["reasons"]))

    def test_red_forecast_closes_outdoor_and_lift_restores(self):
        self.add_park()
        out = self.svc.add_forecast({
            "kind": "空气污染", "level": "红", "scope": "facility",
            "scope_id": "P1", "start": SAT_09, "end": SAT_12})
        cap = self.svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "closed")
        self.svc.lift_forecast(out["forecast"]["id"])
        cap = self.svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "open")

    def test_site_fault_work_order_closes_facility_until_repaired(self):
        self.add_park()
        wo = self.svc.create_work_order({
            "facility_id": "P1", "level": "场地级", "description": "跑道塌陷"})
        self.assertEqual(self.svc.facilities["P1"].state, "维修中")
        cap = self.svc.availability({"start": WIDE_START, "end": WIDE_END})["facilities"][0]
        self.assertEqual(cap["status"], "closed")
        self.assertTrue(any("跑道塌陷" in r for r in cap["reasons"]))

        self.svc.start_work_order(wo["id"])
        self.svc.complete_work_order({"work_order_id": wo["id"],
                                       "result_note": "塌陷区域已重新铺装"})
        self.assertEqual(self.svc.facilities["P1"].state, "开放中")
        cap = self.svc.availability({"start": WIDE_START, "end": WIDE_END})["facilities"][0]
        self.assertEqual(cap["status"], "open")

    def test_equipment_fault_stops_equipment_but_keeps_facility_open(self):
        self.add_park()
        self.svc.register_equipment({"id": "E1", "facility_id": "P1", "name": "划船器"})
        wo = self.svc.create_work_order({
            "facility_id": "P1", "equipment_id": "E1", "description": "拉索断裂"})
        self.assertEqual(self.svc.equipment["E1"].state, "停用")
        cap = self.svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "open")
        self.assertIn("划船器", cap["stopped_equipment"])
        self.svc.start_work_order(wo["id"])
        self.svc.complete_work_order({"work_order_id": wo["id"],
                                       "result_note": "更换拉索"})
        self.assertEqual(self.svc.equipment["E1"].state, "正常")

    def test_resident_query_filters_and_explains(self):
        self.add_park("P1", accessible=True)
        self.add_park("P2", district="福田区", accessible=False)
        result = self.svc.availability({
            "start": SAT_09, "end": SAT_11, "district": "南山区"})
        self.assertEqual([f["facility_id"] for f in result["facilities"]], ["P1"])
        result = self.svc.availability({
            "start": SAT_09, "end": SAT_11, "accessible_only": True})
        self.assertEqual([f["facility_id"] for f in result["facilities"]], ["P1"])


class MaintenanceQueueTest(OpsTestBase):
    def test_queue_groups_open_orders_first_with_responsibility(self):
        self.add_park()
        self.svc.register_equipment({"id": "E1", "facility_id": "P1", "name": "蹬力器"})
        wo1 = self.svc.create_work_order({
            "facility_id": "P1", "equipment_id": "E1", "description": "螺丝松动"})
        wo2 = self.svc.create_work_order({
            "facility_id": "P1", "level": "场地级", "description": "围网破损",
            "maintenance_org": "专业维保公司", "maintenance_role": "第三方维保"})
        queue = self.svc.maintenance_queue()["queue"]
        self.assertEqual([w["id"] for w in queue[:2]], [wo1["id"], wo2["id"]])
        self.assertEqual(wo2["maintenance_role"], "第三方维保")
        self.svc.start_work_order(wo1["id"])
        self.svc.complete_work_order({"work_order_id": wo1["id"],
                                       "result_note": "已紧固"})
        queue = self.svc.maintenance_queue()["queue"]
        self.assertEqual(queue[0]["id"], wo2["id"])       # 未完工始终排前
        self.assertEqual(queue[-1]["status"], "已完成")

    def test_complete_requires_result_note(self):
        self.add_park()
        wo = self.svc.create_work_order(
            {"facility_id": "P1", "level": "场地级", "description": "照明故障"})
        self.svc.start_work_order(wo["id"])
        with self.assertRaises(OpsError):
            self.svc.complete_work_order({"work_order_id": wo["id"]})


class CoverageAnalysisTest(OpsTestBase):
    def test_service_radius_classification(self):
        self.add_park("P1")  # (22.54, 113.95)
        # 近距离需求点：已覆盖
        self.svc.register_demand_point({
            "id": "D1", "name": "沙河社区网格A", "grid": "A-01",
            "district": "南山区", "location": {"lat": 22.541, "lon": 113.951}})
        # 偏远片区：覆盖不足
        self.svc.register_demand_point({
            "id": "D2", "name": "白芒村网格", "grid": "B-07",
            "district": "南山区", "location": {"lat": 22.68, "lon": 113.85}})
        report = self.svc.coverage_analysis(1000)
        states = {p["demand_point"]["id"]: p["state"] for p in report["points"]}
        self.assertEqual(states["D1"], "已覆盖")
        self.assertEqual(states["D2"], "覆盖不足")
        self.assertEqual(report["summary"],
                         {"待覆盖": 0, "覆盖不足": 1, "已覆盖": 1})


class RestartRecoveryTest(OpsTestBase):
    def test_restart_preserves_bookings_queue_closures_and_balances(self):
        self.add_park(capacity=100)
        self.svc.register_equipment({"id": "E1", "facility_id": "P1", "name": "单杠"})
        self.svc.create_fund_batch({"id": "B2024", "year": 2024, "amount": 100,
                                     "approved_purpose": "器材购置"})
        self.svc.record_expenditure({"id": "X1", "batch_id": "B2024", "amount": 40,
                                      "approved_purpose": "器材购置"})
        self.svc.accept_expenditure({
            "expenditure_id": "X1", "evidence_ref": "ACC-1", "inspector": "赵工",
            "asset_changes": [{"kind": "equipment_installed", "ref_id": "E1"}]})
        booking = self.svc.create_booking({
            "facility_id": "P1", "start": SAT_09, "end": SAT_11,
            "party": "晨练队", "group_size": 10, "notify_contact": "张队"})
        closure = self.svc.add_closure({
            "facility_id": "P1", "kind": "临时闭馆",
            "start": SAT_09, "end": SAT_12, "reason": "暴雨后检修"})
        wo = self.svc.create_work_order({
            "facility_id": "P1", "equipment_id": "E1", "description": "单杠松动"})
        self.svc.ingest_visit({"event_id": "g1", "source": "gate",
                               "facility_id": "P1", "ts": SAT_09, "entries": 9})

        svc = self.restart()  # ---- 服务重启 ----

        # 预约占用不丢、不重复
        self.assertEqual(svc.bookings[booking["id"]]["status"], "有效")
        # 维修队列保持在排队状态，器材仍停用
        self.assertEqual(svc.work_orders[wo["id"]]["status"], "排队中")
        self.assertEqual(svc.equipment["E1"].state, "停用")
        # 闭馆仍生效，容量联动可继续工作
        self.assertTrue(svc.closures[closure["closure"]["id"]]["lifted"] is False)
        cap = svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "closed")
        # 通知不丢失、不重复
        notes = svc.notifications_list(booking["id"])["notifications"]
        self.assertEqual(len(notes), 1)
        self.assertIn("暴雨后检修", notes[0]["reason"])
        # 客流与幂等键恢复
        self.assertEqual(svc.visits_by_bucket("P1")["rows"][0]["entries"], 9)
        self.assertTrue(svc.ingest_visit({
            "event_id": "g1", "source": "gate", "facility_id": "P1",
            "ts": SAT_09, "entries": 9})["dedup"])
        # 资金余额与资产关联恢复
        batch = svc.fund_trace("B2024")["batch"]
        self.assertEqual(batch["spent_amount"], 40.0)
        self.assertEqual(svc.equipment["E1"].funded_by, "B2024")
        self.assertTrue(svc.audit_overview()["fund_balanced"])

        # 重启后维修流程可以继续推进，完工恢复开放状态
        svc.start_work_order(wo["id"])
        svc.complete_work_order({"work_order_id": wo["id"], "result_note": "已加固"})
        self.assertEqual(svc.equipment["E1"].state, "正常")

    def test_restart_restores_forecast_and_carryover(self):
        self.add_park()
        self.svc.add_forecast({"kind": "暴雨", "level": "红", "scope": "facility",
                               "scope_id": "P1", "start": SAT_09, "end": SAT_12})
        self.svc.create_fund_batch({"id": "B2023", "year": 2023, "amount": 10,
                                     "approved_purpose": "场地建设"})
        self.svc.carry_over_batch("B2023")
        svc = self.restart()
        cap = svc.availability({"start": SAT_09, "end": SAT_11})["facilities"][0]
        self.assertEqual(cap["status"], "closed")
        self.assertEqual(svc.fund_batches["B2023"]["state"], "已结转")


class FundLedgerUnitTest(unittest.TestCase):
    def test_verify_empty(self):
        d = tempfile.mkdtemp()
        try:
            self.addCleanup(shutil.rmtree, d, True)
            ledger = FundLedger(os.path.join(d, "fund.jsonl"))
            self.assertTrue(ledger.verify()["ok"])
            ledger.append("ping", {"x": 1})
            self.assertEqual(ledger.verify()["records"], 1)
        finally:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
