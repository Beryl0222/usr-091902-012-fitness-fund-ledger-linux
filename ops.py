"""公共健身设施运营的领域内核。

设计要点（对应运营要求）：

* 事件溯源 + JSONL 哈希链：资金批次、支出、退回、验收与资产变化全部以
  追加事件落盘，每条资金记录携带前一条的哈希；跨年度结余与退回只能以新事件
  体现，改写历史记录会被 verify 发现。服务重启时重放日志即可恢复全部状态，
  预约占用与维修队列不会因重启而错乱。
* 幂等接入：闸机客流与巡检事件以“来源:事件 id”去重，离线补传乱序到达
  也不会重复计数（同一事件 id 只生效一次，重复提交返回 dedup=true）。
* 容量联动：临时闭馆、赛事占用、气象/空气预警、故障工单都会改变可用容量；
  生效时自动给受影响时段内的预约安排生成通知，居民查询可用性时能看到
  真正容量与每一条关闭/占用原因。
* 隐私最小化：客流只接受分时段计数，拒绝任何可还原个人轨迹的字段
  （白名单校验）；需求点只存网格/社区粒度的位置，服务半径分析不涉及个人。

金额一律以“分”整数存储，避免浮点误差。
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import threading
import uuid
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# 常量与校验表
# ---------------------------------------------------------------------------

EPOCH = "0" * 64

FACILITY_STATES = ("规划中", "建设中", "开放中", "临时关闭", "维修中", "已退役")
EQUIPMENT_STATES = ("正常", "停用", "维修中", "已退役")
FUND_YEAR_STATES = ("进行中", "已结转", "已关闭")
MAINTENANCE_ROLES = ("自管", "物业", "第三方维保", "厂家")
FORECAST_LEVELS = ("蓝", "黄", "橙", "红")
FORECAST_KINDS = ("暴雨", "空气污染", "高温", "雷电")
DEMAND_STATES = ("待覆盖", "覆盖不足", "已覆盖")

# 客流事件只允许这些字段——任何身份标识/设备指纹/精确位置都拒绝
VISIT_ALLOWED_FIELDS = {
    "event_id", "source", "facility_id", "ts", "time_bucket",
    "entries", "exits",
}
# 巡检事件允许携带的字段（故障描述可以，不允许关联到个人）
INSPECTION_ALLOWED_FIELDS = {
    "event_id", "source", "facility_id", "equipment_id",
    "ts", "result", "note",
}
# 预警 -> 容量折减比例（橙/红预警下户外场地受限）
FORECAST_CAPACITY_RATIO = {"蓝": 1.0, "黄": 0.75, "橙": 0.5, "红": 0.0}
# 暴雨/空气污染等预警主要影响户外场地（体育公园、山地步道）
OUTDOOR_KINDS = ("体育公园", "山地步道")


def utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hour_bucket(ts: str | None = None) -> str:
    ts = ts or utcnow()
    return ts[:13]  # YYYY-MM-DDTHH（小时桶）


def digest(payload: dict) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class OpsError(ValueError):
    """领域校验错误，HTTP 层映射为 400。"""


class NotFoundError(LookupError):
    """引用的资源不存在，HTTP 层映射为 404。"""


class ConflictError(RuntimeError):
    """状态冲突（重复处理、余额不足、台账被篡改等），HTTP 层映射为 409。"""


# ---------------------------------------------------------------------------
# 资金哈希链（独立账本，审计可单独校验）
# ---------------------------------------------------------------------------

class FundLedger:
    """公益金台账：只追加、哈希串联。"""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.Lock()
        self.head = EPOCH
        self.seq = 0
        if os.path.exists(path):
            self._replay()

    def _replay(self):
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec["prev_hash"] != self.head:
                    raise ConflictError(f"资金台账断链：第 {rec['seq']} 条")
                if rec["hash"] != digest(
                        {k: v for k, v in rec.items() if k != "hash"}):
                    raise ConflictError(
                        f"资金台账第 {rec['seq']} 条哈希不匹配（记录疑似被修改）")
                self.head = rec["hash"]
                self.seq = rec["seq"]

    def append(self, rec_type: str, payload: dict) -> dict:
        with self.lock:
            self.seq += 1
            rec = {
                "seq": self.seq,
                "ts": utcnow(),
                "type": rec_type,
                "prev_hash": self.head,
                **payload,
            }
            rec["hash"] = digest({k: v for k, v in rec.items() if k != "hash"})
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            self.head = rec["hash"]
            return copy.deepcopy(rec)

    def verify(self) -> dict:
        """从头复核整条链，供审计端点调用。"""
        head, seq = EPOCH, 0
        if not os.path.exists(self.path):
            return {"ok": True, "records": 0, "head": head}
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                seq += 1
                if rec["prev_hash"] != head:
                    return {"ok": False, "broken_at": rec["seq"], "reason": "链断裂"}
                want = digest({k: v for k, v in rec.items() if k != "hash"})
                if rec["hash"] != want:
                    return {"ok": False, "broken_at": rec["seq"], "reason": "哈希不匹配"}
                head = rec["hash"]
        return {"ok": True, "records": seq, "head": head}


# ---------------------------------------------------------------------------
# 领域对象
# ---------------------------------------------------------------------------

@dataclass
class Facility:
    id: str
    name: str
    kind: str                       # 体育公园 / 百姓健身房 / 山地步道
    district: str
    community: str
    location: dict                  # {lat, lon}
    capacity: int
    state: str = "建设中"
    accessible: bool = False        # 无障碍能力
    maintenance_org: str = ""
    maintenance_role: str = ""
    maintenance_contact: str = ""
    funded_by: list = field(default_factory=list)   # 资金批次 id
    equipment_ids: list = field(default_factory=list)
    hours: list = field(default_factory=list)       # 常规开放时段


@dataclass
class Equipment:
    id: str
    facility_id: str
    name: str
    funded_by: str = ""             # 购置批次
    state: str = "正常"
    maintenance_role: str = ""
    work_order_id: str | None = None


class OpsService:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.log_path = os.path.join(data_dir, "events.jsonl")
        self.fund_path = os.path.join(data_dir, "fund.jsonl")
        self.lock = threading.RLock()
        self.ledger = FundLedger(self.fund_path)

        # 投影状态
        self.event_seq = 0
        self.facilities: dict[str, Facility] = {}
        self.equipment: dict[str, Equipment] = {}
        self.fund_batches: dict[str, dict] = {}
        self.expenditures: dict[str, dict] = {}
        self.bookings: dict[str, dict] = {}
        self.closures: dict[str, dict] = {}
        self.forecasts: dict[str, list] = {}   # scope_id -> [forecast...]
        self.work_orders: dict[str, dict] = {}
        self.demand_points: dict[str, dict] = {}
        self.visits: list[dict] = []           # 仅分时段计数
        self.notifications: list[dict] = []
        self.seen_event_ids: set[str] = set()  # 闸机/巡检幂等键

        self._replay_events()
        # 资金投影以哈希链账本为权威来源，事件日志重放完（场地器材均已就位）
        # 后再重建，验收引发的资产变化即可安全套用。
        self._restore_fund_projection_from_ledger()

    # ----- 通用事件日志（重建预约/工单/可用性等投影） ---------------------

    def _write_event(self, etype: str, payload: dict) -> dict:
        self.event_seq += 1
        rec = {
            "seq": self.event_seq,
            "ts": utcnow(),
            "type": etype,
            "payload": payload,
        }
        with open(self.log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        return rec

    def _replay_events(self):
        if not os.path.exists(self.log_path):
            return
        with open(self.log_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                self.event_seq = rec["seq"]
                self._apply(rec)

    # ----- 小工具 ---------------------------------------------------------

    @staticmethod
    def _yuan_to_cents(amount) -> int:
        if isinstance(amount, bool):
            raise OpsError("金额必须为正数")
        try:
            cents = round(float(amount) * 100)
        except (TypeError, ValueError):
            raise OpsError("金额格式不正确")
        if cents <= 0:
            raise OpsError("金额必须为正数")
        return cents

    @staticmethod
    def _cents_yuan(cents: int) -> float:
        return round(cents / 100, 2)

    def _facility(self, facility_id: str) -> Facility:
        f = self.facilities.get(facility_id or "")
        if not f:
            raise NotFoundError(f"场地不存在：{facility_id}")
        return f

    def _equipment(self, equipment_id: str) -> Equipment:
        e = self.equipment.get(equipment_id or "")
        if not e:
            raise NotFoundError(f"器材不存在：{equipment_id}")
        return e

    @staticmethod
    def _overlaps(start_a: str, end_a: str, start_b: str, end_b: str) -> bool:
        return start_a < end_b and start_b < end_a

    # =====================================================================
    # 场地与器材
    # =====================================================================

    def register_facility(self, data: dict) -> dict:
        required = ("name", "kind", "district", "community", "location", "capacity")
        for key in required:
            if data.get(key) in (None, ""):
                raise OpsError(f"缺少必填字段：{key}")
        loc = data["location"]
        if not isinstance(loc, dict) or "lat" not in loc or "lon" not in loc:
            raise OpsError("location 必须包含 lat/lon")
        if not isinstance(data["capacity"], int) or data["capacity"] <= 0:
            raise OpsError("capacity 必须为正整数")
        fid = data.get("id") or f"F{uuid.uuid4().hex[:10]}"
        if fid in self.facilities:
            raise ConflictError(f"场地 id 已存在：{fid}")
        role = data.get("maintenance_role", "")
        if role and role not in MAINTENANCE_ROLES:
            raise OpsError(f"维保责任方类型须为：{MAINTENANCE_ROLES}")
        state = data.get("state", "规划中")
        if state not in FACILITY_STATES:
            raise OpsError(f"场地状态须为：{FACILITY_STATES}")
        hours = self._normalize_hours(data.get("hours", [])) if data.get("hours") else []
        f = Facility(
            id=fid, name=data["name"], kind=data["kind"],
            district=data["district"], community=data["community"],
            location={"lat": float(loc["lat"]), "lon": float(loc["lon"])},
            capacity=data["capacity"], state=state,
            accessible=bool(data.get("accessible", False)),
            maintenance_org=data.get("maintenance_org", ""),
            maintenance_role=role,
            maintenance_contact=data.get("maintenance_contact", ""),
            hours=hours,
        )
        with self.lock:
            self.facilities[fid] = f
            self._write_event("facility_registered", {"facility": self._facility_dict(f)})
            return self._facility_dict(f)

    @staticmethod
    def _normalize_hours(hours: list) -> list:
        norm = []
        for slot in hours:
            for key in ("weekday", "open", "close"):
                if key not in slot:
                    raise OpsError(f"开放时段缺少 {key}")
            wd = int(slot["weekday"])
            if not 1 <= wd <= 7 or not ("00:00" <= slot["open"] < slot["close"] <= "23:59"):
                raise OpsError("开放时段取值不合法")
            norm.append({"weekday": wd, "open": slot["open"], "close": slot["close"]})
        return norm

    def update_facility_schedule(self, facility_id: str, hours: list) -> dict:
        f = self._facility(facility_id)
        norm = self._normalize_hours(hours)
        with self.lock:
            f.hours = norm
            self._write_event("facility_schedule_updated",
                              {"facility_id": facility_id, "hours": norm})
            return {"facility_id": facility_id, "hours": norm}

    def update_facility_accessibility(self, facility_id: str, accessible: bool,
                                      note: str = "") -> dict:
        f = self._facility(facility_id)
        with self.lock:
            f.accessible = bool(accessible)
            self._write_event("facility_accessibility_updated",
                              {"facility_id": facility_id, "accessible": f.accessible,
                               "note": note})
            return {"facility_id": facility_id, "accessible": f.accessible}

    def update_maintenance(self, facility_id: str, data: dict) -> dict:
        f = self._facility(facility_id)
        role = data.get("maintenance_role", "")
        if role not in MAINTENANCE_ROLES:
            raise OpsError(f"维保责任方类型须为：{MAINTENANCE_ROLES}")
        with self.lock:
            f.maintenance_org = data.get("maintenance_org", "")
            f.maintenance_role = role
            f.maintenance_contact = data.get("maintenance_contact", "")
            self._write_event("facility_maintenance_updated", {
                "facility_id": facility_id,
                "maintenance_org": f.maintenance_org,
                "maintenance_role": role,
                "maintenance_contact": f.maintenance_contact,
            })
            return {"facility_id": facility_id,
                    "maintenance_org": f.maintenance_org,
                    "maintenance_role": f.maintenance_role,
                    "maintenance_contact": f.maintenance_contact}

    def open_facility(self, facility_id: str) -> dict:
        f = self._facility(facility_id)
        if f.state in ("维修中", "已退役"):
            raise ConflictError(f"场地当前为{f.state}，不能开放")
        with self.lock:
            f.state = "开放中"
            self._write_event("facility_opened", {"facility_id": facility_id})
            return {"facility_id": facility_id, "state": f.state}

    def retire_facility(self, facility_id: str) -> dict:
        f = self._facility(facility_id)
        with self.lock:
            f.state = "已退役"
            self._write_event("facility_retired", {"facility_id": facility_id})
            return {"facility_id": facility_id, "state": f.state}

    def register_equipment(self, data: dict) -> dict:
        f = self._facility(data.get("facility_id", ""))
        if not data.get("name"):
            raise OpsError("器材缺少 name")
        eid = data.get("id") or f"E{uuid.uuid4().hex[:10]}"
        if eid in self.equipment:
            raise ConflictError(f"器材 id 已存在：{eid}")
        role = data.get("maintenance_role", "")
        if role and role not in MAINTENANCE_ROLES:
            raise OpsError(f"维保责任方类型须为：{MAINTENANCE_ROLES}")
        e = Equipment(id=eid, facility_id=f.id, name=data["name"],
                      funded_by=data.get("funded_by", ""),
                      maintenance_role=role)
        with self.lock:
            self.equipment[eid] = e
            f.equipment_ids.append(eid)
            self._write_event("equipment_registered", {"equipment": self._equipment_dict(e)})
            return self._equipment_dict(e)

    # =====================================================================
    # 资金：批次 -> 支出 -> 验收/资产变化 -> 退回/结转
    # =====================================================================

    def create_fund_batch(self, data: dict) -> dict:
        for key in ("year", "approved_purpose"):
            if data.get(key) in (None, ""):
                raise OpsError(f"缺少必填字段：{key}")
        year = int(data["year"])
        bid = data.get("id") or f"B{year}-{uuid.uuid4().hex[:6]}"
        if bid in self.fund_batches:
            raise ConflictError(f"资金批次已存在：{bid}")
        cents = self._yuan_to_cents(data.get("amount"))
        batch = {
            "id": bid, "year": year,
            "approved_purpose": data["approved_purpose"],
            "total_cents": cents,
            "committed_cents": 0,
            "spent_cents": 0,
            "returned_cents": 0,
            "carried_over_cents": 0,
            "state": "进行中",
        }
        with self.lock:
            rec = self.ledger.append("fund_batch_created", {"batch": batch})
            self.fund_batches[bid] = batch
            self._write_event("fund_event", {"ledger_seq": rec["seq"], "kind": "batch_created",
                                             "batch_id": bid})
            return self._fund_batch_view(bid)

    def record_expenditure(self, data: dict) -> dict:
        """公益金支出：必须对应批次与批准用途。"""
        batch = self.fund_batches.get(data.get("batch_id", ""))
        if not batch:
            raise NotFoundError(f"资金批次不存在：{data.get('batch_id')}")
        if batch["state"] != "进行中":
            raise ConflictError(f"批次 {batch['id']} 已 {batch['state']}，不能新增支出")
        purpose = (data.get("approved_purpose") or "").strip()
        if not purpose:
            raise OpsError("支出必须填写批准用途")
        allowed = {x.strip() for x in batch["approved_purpose"].split("/")}
        if purpose not in allowed:
            raise OpsError(
                f"批准用途不符：批次 {batch['id']} 核定用途为「{batch['approved_purpose']}」")
        cents = self._yuan_to_cents(data.get("amount"))
        with self.lock:
            available = (batch["total_cents"] - batch["committed_cents"]
                         - batch["returned_cents"] - batch["carried_over_cents"])
            if cents > available:
                raise ConflictError(
                    f"超出批次可用余额（可用 {self._cents_yuan(available)} 元）")
            eid = data.get("id") or f"X{uuid.uuid4().hex[:10]}"
            if eid in self.expenditures:
                raise ConflictError(f"支出 id 已存在：{eid}")
            exp = {
                "id": eid,
                "batch_id": batch["id"],
                "approved_purpose": purpose,
                "amount_cents": cents,
                "state": "已拨付",
                "facility_id": data.get("facility_id", ""),
                "equipment_ids": list(data.get("equipment_ids", [])),
                "acceptance": None,   # {ts, evidence_ref, inspector, asset_changes}
                "returned_cents": 0,
            }
            rec = self.ledger.append("expenditure_recorded", {"expenditure": exp})
            self.expenditures[eid] = exp
            batch["committed_cents"] += cents
            self._write_event("fund_event", {"ledger_seq": rec["seq"],
                                             "kind": "expenditure_recorded",
                                             "expenditure_id": eid})
            return self._expenditure_view(eid)

    def accept_expenditure(self, data: dict) -> dict:
        """验收：必须提供验收证据，并记录真实资产变化（场地建成/器材到位）。"""
        exp = self.expenditures.get(data.get("expenditure_id", ""))
        if not exp:
            raise NotFoundError(f"支出不存在：{data.get('expenditure_id')}")
        if exp["state"] != "已拨付":
            raise ConflictError(f"支出当前为{exp['state']}，不能验收")
        evidence = (data.get("evidence_ref") or "").strip()
        if not evidence:
            raise OpsError("验收必须提供验收证据（验收单/照片/报告引用）")
        inspector = (data.get("inspector") or "").strip()
        if not inspector:
            raise OpsError("验收必须记录验收人")
        changes = data.get("asset_changes")
        if not isinstance(changes, list) or not changes:
            raise OpsError("验收必须登记真实资产变化（设施建成、器材到位等）")
        valid_kinds = ("facility_built", "facility_upgraded", "equipment_installed",
                       "accessibility_added", "maintenance_done")
        norm_changes = []
        for ch in changes:
            kind = ch.get("kind")
            if kind not in valid_kinds:
                raise OpsError(f"未知资产变化类型：{kind}")
            ref_id = ch.get("ref_id", "")
            if kind in ("facility_built", "facility_upgraded", "accessibility_added",
                        "maintenance_done"):
                self._facility(ref_id)
            else:
                self._equipment(ref_id)
            norm_changes.append({"kind": kind, "ref_id": ref_id,
                                 "detail": ch.get("detail", "")})

        with self.lock:
            acceptance = {"ts": utcnow(), "evidence_ref": evidence,
                          "inspector": inspector, "asset_changes": norm_changes}
            rec = self.ledger.append("expenditure_accepted", {
                "expenditure_id": exp["id"], "acceptance": acceptance})
            exp["state"] = "已验收"
            exp["acceptance"] = acceptance
            batch = self.fund_batches[exp["batch_id"]]
            batch["committed_cents"] -= exp["amount_cents"] - exp["returned_cents"]
            batch["spent_cents"] += exp["amount_cents"] - exp["returned_cents"]
            self._apply_asset_changes(norm_changes, exp)
            self._write_event("fund_event", {"ledger_seq": rec["seq"],
                                             "kind": "expenditure_accepted",
                                             "expenditure_id": exp["id"]})
            return self._expenditure_view(exp["id"])

    def _apply_asset_changes(self, changes: list, exp: dict):
        """验收证据对应的真实资产变化。

        场地只在“规划/建设中”时随建成验收转开放——若之后又发生维修/退役，
        重建投影时不能被更早的验收记录覆盖掉后来的状态。
        """
        for ch in changes:
            if ch["kind"] in ("facility_built", "facility_upgraded",
                              "accessibility_added"):
                f = self.facilities[ch["ref_id"]]
                if ch["kind"] == "facility_built" and f.state in ("规划中", "建设中"):
                    f.state = "开放中"
                if ch["kind"] == "accessibility_added":
                    f.accessible = True
                if exp["batch_id"] not in f.funded_by:
                    f.funded_by.append(exp["batch_id"])
            elif ch["kind"] == "equipment_installed":
                self.equipment[ch["ref_id"]].funded_by = exp["batch_id"]
            # maintenance_done 的成效由工单完成事件体现

    def return_expenditure(self, data: dict) -> dict:
        """资金退回：结余/退回必须留痕，不能通过改字段抹平。"""
        exp = self.expenditures.get(data.get("expenditure_id", ""))
        if not exp:
            raise NotFoundError(f"支出不存在：{data.get('expenditure_id')}")
        if exp["state"] not in ("已拨付", "已验收"):
            raise ConflictError(f"支出当前为{exp['state']}，不能退回")
        reason = (data.get("reason") or "").strip()
        if not reason:
            raise OpsError("退回必须填写原因（结余上缴/项目取消等）")
        full = bool(data.get("full", False))
        with self.lock:
            outstanding = exp["amount_cents"] - exp["returned_cents"]
            back = outstanding if full else self._yuan_to_cents(data.get("amount"))
            if back > outstanding:
                raise ConflictError("退回金额超过该支出剩余额")
            was_state = exp["state"]
            rec = self.ledger.append("expenditure_returned", {
                "expenditure_id": exp["id"], "returned_cents": back,
                "reason": reason,
            })
            exp["returned_cents"] += back
            batch = self.fund_batches[exp["batch_id"]]
            batch["returned_cents"] += back
            if was_state == "已拨付":
                batch["committed_cents"] -= back
            else:
                batch["spent_cents"] -= back
            if exp["returned_cents"] == exp["amount_cents"]:
                exp["state"] = "已退回"
            self._write_event("fund_event", {"ledger_seq": rec["seq"],
                                             "kind": "expenditure_returned",
                                             "expenditure_id": exp["id"]})
            return self._expenditure_view(exp["id"])

    def carry_over_batch(self, batch_id: str) -> dict:
        """跨年度结转：只允许把未承诺余额结转，留痕且批次不可再支出。"""
        batch = self.fund_batches.get(batch_id)
        if not batch:
            raise NotFoundError(f"资金批次不存在：{batch_id}")
        if batch["state"] != "进行中":
            raise ConflictError(f"批次已 {batch['state']}")
        with self.lock:
            balance = (batch["total_cents"] - batch["committed_cents"]
                       - batch["spent_cents"] - batch["returned_cents"])
            if balance <= 0:
                raise ConflictError("批次无可结转余额")
            rec = self.ledger.append("fund_batch_carried_over", {
                "batch_id": batch_id, "amount_cents": balance})
            batch["carried_over_cents"] += balance
            batch["state"] = "已结转"
            self._write_event("fund_event", {"ledger_seq": rec["seq"],
                                             "kind": "carried_over",
                                             "batch_id": batch_id})
            return self._fund_batch_view(batch_id)

    def fund_trace(self, batch_id: str) -> dict:
        """审计追溯：一笔公益金从批次到支出、验收证据、资产变化的完整链路。"""
        batch = self.fund_batches.get(batch_id)
        if not batch:
            raise NotFoundError(f"资金批次不存在：{batch_id}")
        links = []
        for exp in self.expenditures.values():
            if exp["batch_id"] != batch_id:
                continue
            asset_refs = []
            if exp["acceptance"]:
                for ch in exp["acceptance"]["asset_changes"]:
                    asset_refs.append({"kind": ch["kind"], "ref_id": ch["ref_id"],
                                       "name": self._asset_name(ch)})
            links.append({
                "expenditure": self._expenditure_view(exp["id"]),
                "evidence_ref": exp["acceptance"]["evidence_ref"] if exp["acceptance"] else None,
                "inspector": exp["acceptance"]["inspector"] if exp["acceptance"] else None,
                "asset_changes": asset_refs,
            })
        return {"batch": self._fund_batch_view(batch_id), "expenditures": links}

    def _asset_name(self, ch: dict) -> str:
        if ch["kind"] == "equipment_installed" and ch["ref_id"] in self.equipment:
            return self.equipment[ch["ref_id"]].name
        if ch["ref_id"] in self.facilities:
            return self.facilities[ch["ref_id"]].name
        return ""

    # =====================================================================
    # 闸机客流与巡检（幂等接入，离线补传不重复计数）
    # =====================================================================

    def ingest_visit(self, data: dict) -> dict:
        extra = set(data) - VISIT_ALLOWED_FIELDS
        if extra:
            raise OpsError(f"客流事件含禁止字段（隐私最小化）：{sorted(extra)}")
        return self._ingest_device_event("visit", data)

    def ingest_inspection(self, data: dict) -> dict:
        extra = set(data) - INSPECTION_ALLOWED_FIELDS
        if extra:
            raise OpsError(f"巡检事件含禁止字段：{sorted(extra)}")
        if data.get("result") not in ("正常", "故障", "停用建议"):
            raise OpsError("巡检 result 须为 正常/故障/停用建议")
        return self._ingest_device_event("inspection", data)

    def _ingest_device_event(self, kind: str, data: dict) -> dict:
        event_id = (data.get("event_id") or "").strip()
        source = (data.get("source") or "").strip()
        if not event_id or not source:
            raise OpsError("设备事件必须包含 event_id 与 source")
        if not event_id.isascii() or not all(c.isalnum() or c in "-_:." for c in event_id):
            raise OpsError("event_id 仅允许字母数字与 -_:. 字符")
        key = f"{kind}:{source}:{event_id}"
        f = self._facility(data.get("facility_id", ""))
        if kind == "visit":
            entries, exits = data.get("entries", 0), data.get("exits", 0)
            if not isinstance(entries, int) or not isinstance(exits, int) \
                    or entries < 0 or exits < 0:
                raise OpsError("entries/exits 必须为非负整数")
            bucket = data.get("time_bucket") or hour_bucket(data.get("ts"))
            payload = {"event_id": event_id, "source": source, "facility_id": f.id,
                       "ts": data.get("ts", utcnow()), "time_bucket": bucket,
                       "entries": entries, "exits": exits}
        else:
            equip = None
            if data.get("equipment_id"):
                equip = self._equipment(data["equipment_id"])
                if equip.facility_id != f.id:
                    raise OpsError("器材不属于该场地")
            payload = {"event_id": event_id, "source": source, "facility_id": f.id,
                       "equipment_id": equip.id if equip else None,
                       "ts": data.get("ts", utcnow()),
                       "result": data["result"], "note": data.get("note", "")}
        with self.lock:
            if key in self.seen_event_ids:
                # 离线补传/重试：确认幂等，不重复计数、不重复开工单
                return {"dedup": True, "event_key": key, "kind": kind}
            self.seen_event_ids.add(key)
            rec = self._write_event(f"{kind}_ingested", payload)
            if kind == "visit":
                self.visits.append(copy.deepcopy(payload))
            return {"dedup": False, "event_key": key, "kind": kind, "seq": rec["seq"]}

    # =====================================================================
    # 预约、闭馆/赛事占用、预警：可用容量联动 + 受影响通知
    # =====================================================================

    def create_booking(self, data: dict) -> dict:
        f = self._facility(data.get("facility_id", ""))
        start, end = data.get("start"), data.get("end")
        if not start or not end or not (start < end):
            raise OpsError("预约需要合法的 start/end（ISO8601 UTC，字符串比较）")
        party = (data.get("party") or "").strip()
        if not party:
            raise OpsError("预约需要使用方名称 party（无需个人身份信息）")
        size = data.get("group_size", 1)
        if not isinstance(size, int) or size <= 0:
            raise OpsError("group_size 必须为正整数")
        contact = (data.get("notify_contact") or "").strip()
        with self.lock:
            cap = self._effective_capacity(f.id, start, end)
            if cap["status"] == "closed":
                raise ConflictError(f"场地该时段不可用：{'；'.join(cap['reasons'])}")
            held = sum(b["group_size"] for b in self.bookings.values()
                       if b["facility_id"] == f.id and b["status"] == "有效"
                       and self._overlaps(start, end, b["start"], b["end"]))
            if held + size > cap["capacity"]:
                raise ConflictError(
                    f"剩余容量不足：可用 {cap['capacity'] - held}，申请 {size}")
            bid = f"R{uuid.uuid4().hex[:10]}"
            booking = {"id": bid, "facility_id": f.id, "start": start, "end": end,
                       "party": party, "group_size": size,
                       "notify_contact": contact, "status": "有效"}
            self.bookings[bid] = booking
            self._write_event("booking_created", {"booking": booking})
            return copy.deepcopy(booking)

    def cancel_booking(self, booking_id: str) -> dict:
        b = self.bookings.get(booking_id)
        if not b:
            raise NotFoundError(f"预约不存在：{booking_id}")
        with self.lock:
            if b["status"] != "有效":
                raise ConflictError(f"预约已{b['status']}")
            b["status"] = "已取消"
            self._write_event("booking_cancelled", {"booking_id": booking_id})
            return {"id": booking_id, "status": b["status"]}

    def _notify_affected(self, facility_id: str, start: str, end: str,
                         reason: str, source_ref: str) -> list[dict]:
        """找出受影响时段内的有效预约并生成通知（含联系方式便于线下触达）。"""
        made = []
        for b in self.bookings.values():
            if b["facility_id"] != facility_id or b["status"] != "有效":
                continue
            if self._overlaps(start, end, b["start"], b["end"]):
                made.append({
                    "id": f"N{uuid.uuid4().hex[:8]}", "booking_id": b["id"],
                    "facility_id": facility_id, "party": b["party"],
                    "notify_contact": b["notify_contact"],
                    "start": b["start"], "end": b["end"],
                    "reason": reason, "source_ref": source_ref, "ts": utcnow(),
                })
        self.notifications.extend(made)
        return made

    def add_closure(self, data: dict) -> dict:
        """临时闭馆 / 赛事占用：改变容量并通知受影响的使用安排。"""
        f = self._facility(data.get("facility_id", ""))
        kind = data.get("kind")
        if kind not in ("临时闭馆", "赛事占用"):
            raise OpsError("闭馆类型须为 临时闭馆/赛事占用")
        start, end = data.get("start"), data.get("end")
        if not start or not end or not (start < end):
            raise OpsError("需要合法的 start/end")
        reason = (data.get("reason") or "").strip()
        if not reason:
            raise OpsError("闭馆/占用必须填写原因")
        ratio = 0.0 if kind == "临时闭馆" else float(data.get("capacity_ratio", 0.0))
        if not 0.0 <= ratio <= 1.0:
            raise OpsError("capacity_ratio 须在 0~1 之间")
        cid = f"C{uuid.uuid4().hex[:8]}"
        with self.lock:
            closure = {"id": cid, "facility_id": f.id, "kind": kind,
                       "start": start, "end": end, "reason": reason,
                       "capacity_ratio": ratio, "lifted": False}
            notifications = self._notify_affected(
                f.id, start, end, f"{kind}：{reason}", cid)
            self.closures[cid] = closure
            self._write_event("closure_added",
                              {"closure": closure, "notifications": notifications})
            return {"closure": closure, "notifications": notifications}

    def lift_closure(self, closure_id: str) -> dict:
        c = self.closures.get(closure_id)
        if not c:
            raise NotFoundError(f"闭馆记录不存在：{closure_id}")
        with self.lock:
            if c["lifted"]:
                raise ConflictError("闭馆记录已解除")
            c["lifted"] = True
            self._write_event("closure_lifted", {"closure_id": closure_id, "ts": utcnow()})
            return {"id": closure_id, "lifted": True}

    def add_forecast(self, data: dict) -> dict:
        """暴雨/空气污染预警：按等级折减户外场地容量并通知受影响安排。"""
        kind = data.get("kind")
        if kind not in FORECAST_KINDS:
            raise OpsError(f"预警类型须为：{FORECAST_KINDS}")
        level = data.get("level")
        if level not in FORECAST_LEVELS:
            raise OpsError(f"预警等级须为：{FORECAST_LEVELS}")
        scope = data.get("scope")
        if scope not in ("district", "facility"):
            raise OpsError("scope 须为 district/facility")
        scope_id = (data.get("scope_id") or "").strip()
        if not scope_id:
            raise OpsError("缺少 scope_id（区名或场地 id）")
        if scope == "facility":
            self._facility(scope_id)
        start, end = data.get("start"), data.get("end")
        if not start or not end or not (start < end):
            raise OpsError("需要合法的 start/end")
        fid = f"W{uuid.uuid4().hex[:8]}"
        with self.lock:
            forecast = {"id": fid, "kind": kind, "level": level, "scope": scope,
                        "scope_id": scope_id, "start": start, "end": end,
                        "lifted": False}
            targets = [fac_id for fac_id, fac in self.facilities.items()
                       if (scope == "facility" and fac_id == scope_id)
                       or (scope == "district" and fac.district == scope_id)]
            notifications = []
            for fac_id in targets:
                notifications += self._notify_affected(
                    fac_id, start, end, f"{kind}{level}预警", fid)
            self.forecasts.setdefault(scope_id, []).append(forecast)
            self._write_event("forecast_added", {
                "forecast": forecast, "affected_facilities": targets,
                "notifications": notifications})
            return {"forecast": forecast, "affected_facilities": targets,
                    "notifications": notifications}

    def lift_forecast(self, forecast_id: str) -> dict:
        with self.lock:
            for lst in self.forecasts.values():
                for fc in lst:
                    if fc["id"] == forecast_id:
                        if fc["lifted"]:
                            raise ConflictError("预警已解除")
                        fc["lifted"] = True
                        self._write_event("forecast_lifted",
                                          {"forecast_id": forecast_id, "ts": utcnow()})
                        return {"id": forecast_id, "lifted": True}
        raise NotFoundError(f"预警不存在：{forecast_id}")

    def _active_forecasts(self, facility_id: str, start: str, end: str) -> list:
        f = self.facilities[facility_id]
        out = []
        for scope_id, lst in self.forecasts.items():
            for fc in lst:
                if fc["lifted"]:
                    continue
                applies = (fc["scope"] == "facility" and scope_id == facility_id) or \
                          (fc["scope"] == "district" and scope_id == f.district)
                if applies and self._overlaps(start, end, fc["start"], fc["end"]):
                    out.append(fc)
        return out

    def _effective_capacity(self, facility_id: str, start: str, end: str) -> dict:
        """计算某时段真正可用的容量，并列出每一项原因。"""
        f = self.facilities[facility_id]
        base = f.capacity
        reasons, ratio, closed = [], 1.0, False

        if f.state == "已退役":
            closed = True
            reasons.append("场地已退役")
        elif f.state == "维修中":
            closed = True
            reasons.append("场地维修中")

        open_site_wo = [w for w in self.work_orders.values()
                        if w["facility_id"] == facility_id
                        and w["level"] == "场地级"
                        and w["status"] in ("排队中", "维修中")
                        and self._overlaps(start, end, w["created_at"],
                                           w.get("finished_at") or "9999")]
        if open_site_wo:
            closed = True
            reasons.append(f"场地故障维修：{open_site_wo[0]['description']}")

        for c in self.closures.values():
            if c["facility_id"] == facility_id and not c["lifted"] \
                    and self._overlaps(start, end, c["start"], c["end"]):
                if c["capacity_ratio"] == 0.0:
                    closed = True
                ratio = min(ratio, c["capacity_ratio"])
                reasons.append(f"{c['kind']}：{c['reason']}")

        for fc in self._active_forecasts(facility_id, start, end):
            if f.kind not in OUTDOOR_KINDS:
                reasons.append(f"{fc['kind']}{fc['level']}预警（室内场地，容量不受限）")
                continue
            r = FORECAST_CAPACITY_RATIO[fc["level"]]
            ratio = min(ratio, r)
            if r == 0.0:
                closed = True
            reasons.append(f"{fc['kind']}{fc['level']}预警（容量折减至 {int(r * 100)}%）")

        stopped = [e.name for e in self.equipment.values()
                   if e.facility_id == facility_id and e.state in ("停用", "维修中")]

        if closed:
            effective = 0
        elif ratio < 1.0:
            effective = max(1, int(base * ratio))
        else:
            effective = base
        held = sum(b["group_size"] for b in self.bookings.values()
                   if b["facility_id"] == facility_id and b["status"] == "有效"
                   and self._overlaps(start, end, b["start"], b["end"]))
        return {
            "facility_id": facility_id, "name": f.name, "kind": f.kind,
            "start": start, "end": end,
            "status": "closed" if closed else ("limited" if ratio < 1.0 else "open"),
            "base_capacity": base, "capacity": effective,
            "held": held, "remaining": max(0, effective - held) if not closed else 0,
            "reasons": reasons, "stopped_equipment": stopped,
            "accessible": f.accessible,
        }

    def availability(self, query: dict) -> dict:
        """居民查询：某时段真正可用的场地及关闭原因。"""
        start, end = query.get("start"), query.get("end")
        if not start or not end or not (start < end):
            raise OpsError("查询需要合法的 start/end")
        district = query.get("district")
        kind = query.get("kind")
        accessible_only = bool(query.get("accessible_only"))
        result = []
        for fid, f in self.facilities.items():
            if district and f.district != district:
                continue
            if kind and f.kind != kind:
                continue
            if accessible_only and not f.accessible:
                continue
            result.append(self._effective_capacity(fid, start, end))
        # 开放且有余量的优先
        result.sort(key=lambda c: (c["status"] != "open", c["remaining"] == 0,
                                   -c["remaining"]))
        return {"start": start, "end": end,
                "district": district, "kind": kind,
                "facilities": result}

    # =====================================================================
    # 维修：巡检 -> 工单队列 -> 完工（维保责任与成效）
    # =====================================================================

    def create_work_order(self, data: dict) -> dict:
        f = self._facility(data.get("facility_id", ""))
        desc = (data.get("description") or "").strip()
        if not desc:
            raise OpsError("工单必须描述故障")
        level = data.get("level", "器材级")
        if level not in ("场地级", "器材级"):
            raise OpsError("level 须为 场地级/器材级")
        equip = None
        if data.get("equipment_id"):
            equip = self._equipment(data["equipment_id"])
            if equip.facility_id != f.id:
                raise OpsError("器材不属于该场地")
        with self.lock:
            wid = f"WO{uuid.uuid4().hex[:8]}"
            wo = {"id": wid, "facility_id": f.id,
                  "equipment_id": equip.id if equip else None,
                  "level": level, "description": desc,
                  "maintenance_org": data.get("maintenance_org", f.maintenance_org),
                  "maintenance_role": data.get("maintenance_role", f.maintenance_role),
                  "status": "排队中", "source_inspection": data.get("source_event_id", ""),
                  "created_at": utcnow(), "started_at": None, "finished_at": None,
                  "result_note": "", "expenditure_id": None}
            if equip:
                equip.state = "停用"
                equip.work_order_id = wid
            elif level == "场地级" and f.state == "开放中":
                f.state = "维修中"
            self.work_orders[wid] = wo
            self._write_event("work_order_created", {"work_order": wo})
            return self._work_order_view(wid)

    def start_work_order(self, work_order_id: str) -> dict:
        wo = self.work_orders.get(work_order_id)
        if not wo:
            raise NotFoundError(f"工单不存在：{work_order_id}")
        with self.lock:
            if wo["status"] != "排队中":
                raise ConflictError(f"工单当前为{wo['status']}")
            wo["status"] = "维修中"
            wo["started_at"] = utcnow()
            f = self.facilities[wo["facility_id"]]
            if wo["level"] == "场地级" and f.state != "维修中":
                f.state = "维修中"
            if wo["equipment_id"]:
                self.equipment[wo["equipment_id"]].state = "维修中"
            self._write_event("work_order_started",
                              {"work_order_id": work_order_id, "ts": wo["started_at"]})
            return self._work_order_view(work_order_id)

    def complete_work_order(self, data: dict) -> dict:
        wo = self.work_orders.get(data.get("work_order_id", ""))
        if not wo:
            raise NotFoundError(f"工单不存在：{data.get('work_order_id')}")
        if wo["status"] != "维修中":
            raise ConflictError(f"工单当前为{wo['status']}，需先开始维修")
        note = (data.get("result_note") or "").strip()
        if not note:
            raise OpsError("完工必须记录维修结果")
        with self.lock:
            if data.get("expenditure_id"):
                exp = self.expenditures.get(data["expenditure_id"])
                if not exp:
                    raise NotFoundError("关联的维修支出不存在")
                wo["expenditure_id"] = exp["id"]
            wo["status"] = "已完成"
            wo["finished_at"] = utcnow()
            wo["result_note"] = note
            f = self.facilities[wo["facility_id"]]
            if wo["level"] == "场地级" and f.state == "维修中":
                f.state = "开放中"
            if wo["equipment_id"]:
                e = self.equipment[wo["equipment_id"]]
                e.state = "正常"
                e.work_order_id = None
            self._write_event("work_order_completed", {"work_order": wo})
            return self._work_order_view(wo["id"])

    def maintenance_queue(self) -> dict:
        orders = sorted(self.work_orders.values(),
                        key=lambda w: (w["status"] == "已完成", w["created_at"]))
        return {"queue": [self._work_order_view(w["id"]) for w in orders]}

    # =====================================================================
    # 覆盖分析（匿名客流 + 需求点服务半径）
    # =====================================================================

    def register_demand_point(self, data: dict) -> dict:
        for key in ("name", "grid", "location"):
            if data.get(key) in (None, ""):
                raise OpsError(f"缺少必填字段：{key}")
        loc = data["location"]
        if not isinstance(loc, dict) or "lat" not in loc or "lon" not in loc:
            raise OpsError("location 必须包含 lat/lon")
        pid = data.get("id") or f"D{uuid.uuid4().hex[:8]}"
        if pid in self.demand_points:
            raise ConflictError(f"需求点 id 已存在：{pid}")
        with self.lock:
            point = {"id": pid, "name": data["name"], "grid": data["grid"],
                     "district": data.get("district", ""),
                     "location": {"lat": float(loc["lat"]), "lon": float(loc["lon"])}}
            self.demand_points[pid] = point
            self._write_event("demand_point_registered", {"demand_point": point})
            return copy.deepcopy(point)

    @staticmethod
    def _haversine_km(a: dict, b: dict) -> float:
        r = 6371.0
        dlat = math.radians(b["lat"] - a["lat"])
        dlon = math.radians(b["lon"] - a["lon"])
        x = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(a["lat"])) * math.cos(math.radians(b["lat"]))
             * math.sin(dlon / 2) ** 2)
        return 2 * r * math.asin(math.sqrt(x))

    def coverage_analysis(self, radius_m: int = 1000) -> dict:
        """偏远片区覆盖：需求点（网格/社区粒度）到最近在役场地的距离。"""
        if not isinstance(radius_m, int) or radius_m <= 0:
            raise OpsError("服务半径须为正整数（米）")
        points = []
        for p in self.demand_points.values():
            nearest, best = None, None
            for f in self.facilities.values():
                if f.state == "已退役":
                    continue
                d = self._haversine_km(p["location"], f.location)
                if best is None or d < best:
                    best, nearest = d, f.id
            dist_m = round(best * 1000) if best is not None else None
            state = ("待覆盖" if nearest is None else
                     "已覆盖" if dist_m <= radius_m else "覆盖不足")
            points.append({"demand_point": p, "nearest_facility_id": nearest,
                           "nearest_facility_name":
                               self.facilities[nearest].name if nearest else None,
                           "distance_m": dist_m, "state": state})
        summary = {s: 0 for s in DEMAND_STATES}
        for p in points:
            summary[p["state"]] += 1
        return {"radius_m": radius_m, "summary": summary, "points": points}

    def visits_by_bucket(self, facility_id: str | None = None) -> dict:
        """分时段匿名客流：只有聚合数，没有任何个人维度。"""
        agg: dict[tuple, dict] = {}
        for v in self.visits:
            if facility_id and v["facility_id"] != facility_id:
                continue
            key = (v["facility_id"], v["time_bucket"])
            row = agg.setdefault(key, {"entries": 0, "exits": 0})
            row["entries"] += v["entries"]
            row["exits"] += v["exits"]
        rows = [{"facility_id": k[0], "time_bucket": k[1],
                 "entries": v["entries"], "exits": v["exits"]}
                for k, v in sorted(agg.items())]
        return {"granularity": "hour", "facility_id": facility_id, "rows": rows}

    def notifications_list(self, booking_id: str | None = None) -> dict:
        items = self.notifications
        if booking_id:
            items = [n for n in items if n["booking_id"] == booking_id]
        return {"notifications": copy.deepcopy(items)}

    # =====================================================================
    # 审计与只读视图
    # =====================================================================

    def audit_overview(self) -> dict:
        fund = self.ledger.verify()
        total = sum(b["total_cents"] for b in self.fund_batches.values())
        spent = sum(b["spent_cents"] for b in self.fund_batches.values())
        committed = sum(b["committed_cents"] for b in self.fund_batches.values())
        returned = sum(b["returned_cents"] for b in self.fund_batches.values())
        carried = sum(b["carried_over_cents"] for b in self.fund_batches.values())
        # 尚未安排用途的可用额度
        available = total - spent - committed - returned - carried
        return {
            "ledger": fund,
            "fund_balanced": fund["ok"] and available >= 0
            and total == spent + committed + returned + carried + available,
            "fund_total_cents": total,
            "fund_spent_cents": spent,
            "fund_committed_cents": committed,
            "fund_returned_cents": returned,
            "fund_carried_over_cents": carried,
            "fund_available_cents": available,
            "counts": {
                "facilities": len(self.facilities),
                "equipment": len(self.equipment),
                "fund_batches": len(self.fund_batches),
                "expenditures": len(self.expenditures),
                "bookings_active": sum(1 for b in self.bookings.values()
                                       if b["status"] == "有效"),
                "closures": len(self.closures),
                "work_orders_open": sum(1 for w in self.work_orders.values()
                                        if w["status"] != "已完成"),
                "visit_records": len(self.visits),
                "ingested_event_keys": len(self.seen_event_ids),
                "notifications": len(self.notifications),
            },
            "tampered": not fund["ok"],
        }

    def list_facilities(self) -> dict:
        return {"facilities": [self._facility_dict(f) for f in self.facilities.values()]}

    def facility_detail(self, facility_id: str) -> dict:
        f = self._facility(facility_id)
        return {
            "facility": self._facility_dict(f),
            "equipment": [self._equipment_dict(self.equipment[e])
                          for e in f.equipment_ids],
            "fund_batches": [self._fund_batch_view(b) for b in f.funded_by
                             if b in self.fund_batches],
            "open_work_orders": [self._work_order_view(w["id"])
                                 for w in self.work_orders.values()
                                 if w["facility_id"] == f.id and w["status"] != "已完成"],
        }

    def list_fund_batches(self) -> dict:
        return {"batches": [self._fund_batch_view(b)
                            for b in self.fund_batches.values()]}

    def list_expenditures(self) -> dict:
        return {"expenditures": [self._expenditure_view(e)
                                 for e in self.expenditures.values()]}

    # ----- 视图与事件应用 -------------------------------------------------

    def _facility_dict(self, f: Facility) -> dict:
        return {"id": f.id, "name": f.name, "kind": f.kind, "district": f.district,
                "community": f.community, "location": f.location,
                "capacity": f.capacity, "state": f.state,
                "accessible": f.accessible,
                "maintenance_org": f.maintenance_org,
                "maintenance_role": f.maintenance_role,
                "maintenance_contact": f.maintenance_contact,
                "funded_by": list(f.funded_by),
                "equipment_ids": list(f.equipment_ids),
                "hours": copy.deepcopy(f.hours)}

    def _equipment_dict(self, e: Equipment) -> dict:
        return {"id": e.id, "facility_id": e.facility_id, "name": e.name,
                "funded_by": e.funded_by, "state": e.state,
                "maintenance_role": e.maintenance_role,
                "work_order_id": e.work_order_id}

    def _fund_batch_view(self, bid: str) -> dict:
        b = self.fund_batches[bid]
        return {**b,
                "total_amount": self._cents_yuan(b["total_cents"]),
                "spent_amount": self._cents_yuan(b["spent_cents"]),
                "committed_amount": self._cents_yuan(b["committed_cents"]),
                "returned_amount": self._cents_yuan(b["returned_cents"]),
                "carried_over_amount": self._cents_yuan(b["carried_over_cents"])}

    def _expenditure_view(self, eid: str) -> dict:
        e = self.expenditures[eid]
        return {**e, "amount": self._cents_yuan(e["amount_cents"]),
                "returned_amount": self._cents_yuan(e["returned_cents"])}

    def _work_order_view(self, wid: str) -> dict:
        return copy.deepcopy(self.work_orders[wid])

    def _apply(self, rec: dict):
        """重放事件日志重建投影。资金投影以哈希链为准，在重放后单独恢复。"""
        t, p = rec["type"], rec["payload"]
        if t == "facility_registered":
            fd = p["facility"]
            self.facilities[fd["id"]] = Facility(
                id=fd["id"], name=fd["name"], kind=fd["kind"],
                district=fd["district"], community=fd["community"],
                location=fd["location"], capacity=fd["capacity"],
                state=fd["state"], accessible=fd["accessible"],
                maintenance_org=fd["maintenance_org"],
                maintenance_role=fd["maintenance_role"],
                maintenance_contact=fd["maintenance_contact"],
                funded_by=list(fd["funded_by"]),
                equipment_ids=list(fd["equipment_ids"]),
                hours=copy.deepcopy(fd.get("hours", [])))
        elif t == "facility_schedule_updated":
            self.facilities[p["facility_id"]].hours = copy.deepcopy(p["hours"])
        elif t == "facility_accessibility_updated":
            self.facilities[p["facility_id"]].accessible = p["accessible"]
        elif t == "facility_maintenance_updated":
            f = self.facilities[p["facility_id"]]
            f.maintenance_org = p["maintenance_org"]
            f.maintenance_role = p["maintenance_role"]
            f.maintenance_contact = p["maintenance_contact"]
        elif t == "facility_opened":
            self.facilities[p["facility_id"]].state = "开放中"
        elif t == "facility_retired":
            self.facilities[p["facility_id"]].state = "已退役"
        elif t == "equipment_registered":
            ed = p["equipment"]
            e = Equipment(id=ed["id"], facility_id=ed["facility_id"], name=ed["name"],
                          funded_by=ed["funded_by"], state=ed["state"],
                          maintenance_role=ed["maintenance_role"],
                          work_order_id=ed.get("work_order_id"))
            self.equipment[e.id] = e
            if e.id not in self.facilities[e.facility_id].equipment_ids:
                self.facilities[e.facility_id].equipment_ids.append(e.id)
        elif t == "visit_ingested":
            self.seen_event_ids.add(f"visit:{p['source']}:{p['event_id']}")
            self.visits.append(copy.deepcopy(p))
        elif t == "inspection_ingested":
            self.seen_event_ids.add(f"inspection:{p['source']}:{p['event_id']}")
        elif t == "fund_event":
            pass  # 资金投影由 _restore_fund_projection_from_ledger 重建
        elif t == "booking_created":
            self.bookings[p["booking"]["id"]] = copy.deepcopy(p["booking"])
        elif t == "booking_cancelled":
            self.bookings[p["booking_id"]]["status"] = "已取消"
        elif t == "closure_added":
            self.closures[p["closure"]["id"]] = copy.deepcopy(p["closure"])
            self.notifications.extend(copy.deepcopy(p.get("notifications", [])))
        elif t == "closure_lifted":
            self.closures[p["closure_id"]]["lifted"] = True
        elif t == "forecast_added":
            fc = p["forecast"]
            self.forecasts.setdefault(fc["scope_id"], []).append(copy.deepcopy(fc))
            self.notifications.extend(copy.deepcopy(p.get("notifications", [])))
        elif t == "forecast_lifted":
            for lst in self.forecasts.values():
                for fc in lst:
                    if fc["id"] == p["forecast_id"]:
                        fc["lifted"] = True
        elif t == "work_order_created":
            wo = p["work_order"]
            self.work_orders[wo["id"]] = copy.deepcopy(wo)
            if wo["equipment_id"]:
                e = self.equipment[wo["equipment_id"]]
                e.state = "停用"
                e.work_order_id = wo["id"]
            elif wo["level"] == "场地级":
                f = self.facilities[wo["facility_id"]]
                if f.state == "开放中":
                    f.state = "维修中"
        elif t == "work_order_started":
            wo = self.work_orders[p["work_order_id"]]
            wo["status"] = "维修中"
            wo["started_at"] = p["ts"]
            if wo["equipment_id"]:
                self.equipment[wo["equipment_id"]].state = "维修中"
            if wo["level"] == "场地级":
                f = self.facilities[wo["facility_id"]]
                if f.state != "维修中":
                    f.state = "维修中"
        elif t == "work_order_completed":
            wo = copy.deepcopy(p["work_order"])
            self.work_orders[wo["id"]] = wo
            if wo["equipment_id"]:
                e = self.equipment[wo["equipment_id"]]
                e.state, e.work_order_id = "正常", None
            elif wo["level"] == "场地级":
                f = self.facilities[wo["facility_id"]]
                if f.state == "维修中":
                    f.state = "开放中"
        elif t == "demand_point_registered":
            dp = p["demand_point"]
            self.demand_points[dp["id"]] = copy.deepcopy(dp)

    def _restore_fund_projection_from_ledger(self):
        """以哈希链账本重建资金投影（批次/支出/退回/结转）与验收资产变化。"""
        if not os.path.exists(self.fund_path):
            return
        with open(self.fund_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                t = rec["type"]
                if t == "fund_batch_created":
                    b = copy.deepcopy(rec["batch"])
                    self.fund_batches[b["id"]] = b
                elif t == "expenditure_recorded":
                    e = copy.deepcopy(rec["expenditure"])
                    self.expenditures[e["id"]] = e
                    self.fund_batches[e["batch_id"]]["committed_cents"] += e["amount_cents"]
                elif t == "expenditure_accepted":
                    e = self.expenditures[rec["expenditure_id"]]
                    acceptance = copy.deepcopy(rec["acceptance"])
                    e["state"] = "已验收"
                    e["acceptance"] = acceptance
                    b = self.fund_batches[e["batch_id"]]
                    b["committed_cents"] -= e["amount_cents"] - e["returned_cents"]
                    b["spent_cents"] += e["amount_cents"] - e["returned_cents"]
                    self._apply_asset_changes(acceptance["asset_changes"], e)
                elif t == "expenditure_returned":
                    e = self.expenditures[rec["expenditure_id"]]
                    b = self.fund_batches[e["batch_id"]]
                    back = rec["returned_cents"]
                    e["returned_cents"] += back
                    b["returned_cents"] += back
                    if e["state"] == "已拨付":
                        b["committed_cents"] -= back
                    else:
                        b["spent_cents"] -= back
                    if e["returned_cents"] == e["amount_cents"]:
                        e["state"] = "已退回"
                elif t == "fund_batch_carried_over":
                    b = self.fund_batches[rec["batch_id"]]
                    b["carried_over_cents"] += rec["amount_cents"]
                    b["state"] = "已结转"
