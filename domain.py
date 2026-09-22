"""公共健身设施运营领域模型。

把场地、器材、开放时段、无障碍能力、公益金批次与维保责任关联起来。
设计约束：
- 闸机/巡检事件以 event_id 幂等去重，离线补传不重复计数；
- 临时闭馆、赛事占用、天气预警联动有效容量并通知受影响预约；
- 公益金台账只追加、哈希成链，结余结转与退回必须留下独立分录；
- 客流只接受分时段聚合计数，拒绝任何可还原个人轨迹的字段；
- 所有状态落盘，服务重启后预约占用与维修队列保持不变。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

SERVICE_ID = "fitness-fund-ledger"
SERVICE_NAME = "公共健身设施运营"

VENUE_STATUSES = ("规划中", "开放中", "已退役")
EQUIPMENT_STATUSES = ("正常", "停用", "维修中")
TICKET_STATUSES = ("queued", "in_progress", "resolved")
BOOKING_STATUSES = ("confirmed", "affected", "cancelled")
CLOSURE_KINDS = ("临时闭馆", "赛事占用", "暴雨预警", "空气污染预警", "维修闭馆")
LEDGER_TYPES = ("expenditure", "return", "carryforward")

# 客流记录中一旦出现这些键即视为可能还原个人身份，整条拒绝。
FORBIDDEN_KEYS = re.compile(
    r"(^|_)(user|person|member|device|mac|imei|id_?card|身份证|姓名|手机|phone|mobile|trajectory|track|sn)(_|$)|id_card|姓名",
    re.IGNORECASE,
)


class DomainError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.http_status = http_status


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_dt(value: str, field: str = "时间") -> datetime:
    if not isinstance(value, str):
        raise DomainError("bad_time", f"{field}必须是 ISO 8601 字符串")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise DomainError("bad_time", f"{field}格式无法解析: {value}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def dump_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def intervals_overlap(a_start: datetime, a_end: datetime, b_start, b_end) -> bool:
    return a_start < b_end and b_start < a_end


def canonical_hash(payload: dict, prev_hash: str) -> str:
    body = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256((prev_hash + body).encode("utf-8")).hexdigest()


def scan_forbidden_keys(obj, path=""):
    """递归检查客流负载中是否夹带个人标识。"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(key, str) and FORBIDDEN_KEYS.search(key):
                raise DomainError(
                    "privacy_violation",
                    f"客流数据只能是匿名聚合计数，字段 {path}{key} 可能还原个人轨迹",
                    422,
                )
            scan_forbidden_keys(value, f"{path}{key}.")
    elif isinstance(obj, list):
        for index, value in enumerate(obj):
            scan_forbidden_keys(value, f"{path}{index}.")


class Store:
    """持有全部运营状态；所有变更经同一把锁串行化并原子落盘。"""

    def __init__(self, state_path: str, ledger_path: str):
        self.lock = threading.RLock()
        self.state_path = Path(state_path)
        self.ledger_path = Path(ledger_path)
        self.venues: dict = {}
        self.equipment: dict = {}
        self.batches: dict = {}
        self.bookings: dict = {}
        self.closures: list = []
        self.tickets: dict = {}
        self.notifications: list = []
        self.events: dict = {}  # event_id -> 首次受理结果摘要
        self.occupancy: dict = {}  # venue_id -> 当前在场人数（闸机净值）
        self.footfall: dict = {}  # venue_id|YYYY-MM-DDTHH:00 -> 进入计数
        self.districts: dict = {}  # 片区 -> 服务人口等规划参数
        self.seq = {"closure": 0, "ticket": 0, "notification": 0}
        self.ledger: list = []
        self._ledger_hashes: list = []
        self._load()

    # ---------- 持久化 ----------

    def _load(self):
        if self.state_path.exists():
            with self.state_path.open(encoding="utf-8") as fh:
                state = json.load(fh)
            self.venues = state.get("venues", {})
            self.equipment = state.get("equipment", {})
            self.batches = state.get("batches", {})
            self.bookings = state.get("bookings", {})
            self.closures = state.get("closures", [])
            self.tickets = state.get("tickets", {})
            self.notifications = state.get("notifications", [])
            self.events = state.get("events", {})
            self.occupancy = state.get("occupancy", {})
            self.footfall = state.get("footfall", {})
            self.districts = state.get("districts", {})
            self.seq = state.get("seq", self.seq)
        if self.ledger_path.exists():
            with self.ledger_path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        entry = json.loads(line)
                        self._verify_entry(entry)
                        self.ledger.append(entry)
                        self._ledger_hashes.append(entry["hash"])
            self._verify_chain()

    def _save_state(self):
        tmp = self.state_path.with_suffix(".tmp")
        payload = {
            "venues": self.venues,
            "equipment": self.equipment,
            "batches": self.batches,
            "bookings": self.bookings,
            "closures": self.closures,
            "tickets": self.tickets,
            "notifications": self.notifications,
            "events": self.events,
            "occupancy": self.occupancy,
            "footfall": self.footfall,
            "districts": self.districts,
            "seq": self.seq,
        }
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.state_path)

    def _append_ledger_file(self, entry: dict):
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.ledger_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _verify_entry(self, entry: dict):
        required = {"entry_id", "seq", "ts", "type", "batch_id", "amount", "hash", "prev_hash"}
        missing = required - set(entry)
        if missing:
            raise DomainError("ledger_corrupt", f"台账分录缺少字段: {sorted(missing)}", 500)

    def _verify_chain(self):
        prev = ""
        for entry in self.ledger:
            content = {k: v for k, v in entry.items() if k != "hash"}
            if entry["prev_hash"] != prev:
                raise DomainError("ledger_corrupt", f"分录 {entry['entry_id']} 前序哈希断裂", 500)
            if canonical_hash(content, prev) != entry["hash"]:
                raise DomainError("ledger_corrupt", f"分录 {entry['entry_id']} 哈希校验失败", 500)
            prev = entry["hash"]

    def verify_ledger(self) -> dict:
        with self.lock:
            self._verify_chain()
            return {"entries": len(self.ledger), "tail_hash": self._ledger_hashes[-1] if self._ledger_hashes else ""}

    # ---------- 场地与器材 ----------

    def create_venue(self, data: dict) -> dict:
        for field in ("name", "kind", "district", "capacity"):
            if not data.get(field) and data.get(field) != 0:
                raise DomainError("missing_field", f"场地缺少字段: {field}")
        if data["kind"] not in ("体育公园", "百姓健身房", "山地步道"):
            raise DomainError("bad_kind", "场地类型须为 体育公园/百姓健身房/山地步道")
        capacity = data["capacity"]
        if not isinstance(capacity, int) or capacity <= 0:
            raise DomainError("bad_capacity", "容量须为正整数")
        venue = {
            "id": new_id("V"),
            "name": data["name"],
            "kind": data["kind"],
            "district": data["district"],
            "location": data.get("location", {}),
            "service_radius_m": int(data.get("service_radius_m", 1000)),
            "capacity": capacity,
            "accessibility": data.get("accessibility", {}),
            "hours": data.get("hours", {}),
            "status": "规划中",
            "funded_by": None,
            "upgrades": [],
            "maintainer": data.get("maintainer"),
        }
        with self.lock:
            self.venues[venue["id"]] = venue
            self.occupancy.setdefault(venue["id"], 0)
            self._save_state()
            return dict(venue)

    def add_equipment(self, venue_id: str, data: dict) -> dict:
        if not data.get("name"):
            raise DomainError("missing_field", "器材缺少 name")
        with self.lock:
            venue = self._require_venue(venue_id)
            equip = {
                "id": new_id("E"),
                "venue_id": venue_id,
                "name": data["name"],
                "status": "正常",
                "installed_by": None,
                "maintainer": data.get("maintainer", venue.get("maintainer")),
            }
            self.equipment[equip["id"]] = equip
            self._save_state()
            return dict(equip)

    def _require_venue(self, venue_id: str) -> dict:
        venue = self.venues.get(venue_id)
        if not venue:
            raise DomainError("venue_not_found", f"场地不存在: {venue_id}", 404)
        return venue

    def _require_equipment(self, equipment_id: str) -> dict:
        equip = self.equipment.get(equipment_id)
        if not equip:
            raise DomainError("equipment_not_found", f"器材不存在: {equipment_id}", 404)
        return equip

    # ---------- 事件摄取（闸机/巡检，幂等） ----------

    def ingest_event(self, data: dict) -> dict:
        event_id = data.get("event_id")
        if not event_id:
            raise DomainError("missing_event_id", "闸机/巡检事件必须带 event_id 以支持幂等补传")
        kind = data.get("type")
        if kind not in ("gate_entry", "gate_exit", "inspection"):
            raise DomainError("bad_event_type", "事件类型须为 gate_entry/gate_exit/inspection")
        with self.lock:
            if event_id in self.events:
                # 离线补传：原样返回首次受理结果，绝不重复计数。
                return {"dup": True, "event_id": event_id, "accepted": self.events[event_id]}
            venue = self._require_venue(data.get("venue_id", ""))
            result: dict = {"event_id": event_id, "type": kind, "venue_id": venue["id"]}
            ts = parse_dt(data.get("ts", now_iso()), "ts")
            if kind in ("gate_entry", "gate_exit"):
                delta = 1 if kind == "gate_entry" else -1
                current = self.occupancy.get(venue["id"], 0) + delta
                if current < 0:
                    raise DomainError("negative_occupancy", "闸机离场事件导致在场人数为负，事件被拒绝")
                self.occupancy[venue["id"]] = current
                if kind == "gate_entry":
                    bucket = ts.strftime("%Y-%m-%dT%H:00")
                    key = f"{venue['id']}|{bucket}"
                    self.footfall[key] = self.footfall.get(key, 0) + 1
                result["occupancy"] = current
            else:  # inspection
                equip_id = data.get("equipment_id")
                fault = data.get("fault")
                result["fault"] = bool(fault)
                if fault:
                    if not equip_id:
                        raise DomainError("missing_equipment", "故障巡检必须指明 equipment_id")
                    equip = self._require_equipment(equip_id)
                    if equip["venue_id"] != venue["id"]:
                        raise DomainError("equipment_mismatch", "器材不属于该场地")
                    ticket = None
                    if equip["status"] != "正常":
                        # 同一器材的在途工单不重复建档。
                        ticket = next(
                            (t for t in self.tickets.values()
                             if t["equipment_id"] == equip["id"] and t["status"] != "resolved"),
                            None,
                        )
                    if ticket is None:
                        equip["status"] = "停用"
                        ticket = self._open_ticket(venue["id"], equip["id"], data.get("detail", ""), event_id)
                    result["ticket_id"] = ticket["id"]
            self.events[event_id] = result
            self._save_state()
            return {"dup": False, **result}

    # ---------- 预约 ----------

    def create_booking(self, data: dict) -> dict:
        venue_id = data.get("venue_id", "")
        party_size = data.get("party_size")
        if not isinstance(party_size, int) or party_size <= 0:
            raise DomainError("bad_party_size", "party_size 须为正整数")
        start = parse_dt(data.get("start"), "start")
        end = parse_dt(data.get("end"), "end")
        if end <= start:
            raise DomainError("bad_interval", "结束时间必须晚于开始时间")
        with self.lock:
            venue = self._require_venue(venue_id)
            if venue["status"] == "已退役":
                raise DomainError("venue_retired", "场地已退役，无法预约", 409)
            availability = self._availability(venue, start, end)
            if availability["available"] < party_size:
                raise DomainError(
                    "capacity_exceeded",
                    f"该时段可用容量 {availability['available']}，不足 {party_size}；"
                    f"原因: {'、'.join(availability['reasons']) or '预约已满'}",
                    409,
                )
            booking = {
                "id": new_id("B"),
                "venue_id": venue_id,
                "start": dump_dt(start),
                "end": dump_dt(end),
                "party_size": party_size,
                "contact": data.get("contact", ""),  # 不透明通知句柄，不存身份信息
                "status": "confirmed",
                "created_ts": now_iso(),
            }
            self.bookings[booking["id"]] = booking
            self._save_state()
            return dict(booking)

    def cancel_booking(self, booking_id: str) -> dict:
        with self.lock:
            booking = self.bookings.get(booking_id)
            if not booking:
                raise DomainError("booking_not_found", "预约不存在", 404)
            booking["status"] = "cancelled"
            self._save_state()
            return dict(booking)

    def list_notifications(self, contact: str) -> list:
        if not contact:
            raise DomainError("missing_contact", "需要 contact 通知句柄")
        with self.lock:
            return [dict(n) for n in self.notifications if n["contact"] == contact]

    # ---------- 闭馆 / 赛事 / 天气预警 ----------

    def add_closure(self, data: dict) -> dict:
        kind = data.get("kind")
        if kind not in CLOSURE_KINDS:
            raise DomainError("bad_kind", f"闭馆类型须为 {CLOSURE_KINDS}")
        start = parse_dt(data.get("start"), "start")
        end = parse_dt(data.get("end"), "end")
        if end <= start:
            raise DomainError("bad_interval", "结束时间必须晚于开始时间")
        override = data.get("capacity_override")
        if override is not None and (not isinstance(override, int) or override < 0):
            raise DomainError("bad_override", "capacity_override 须为非负整数")
        venue_ids = data.get("venue_ids")
        district = data.get("district")
        with self.lock:
            if venue_ids:
                targets = [self._require_venue(vid) for vid in venue_ids]
            elif district:
                targets = [v for v in self.venues.values() if v["district"] == district]
                if not targets:
                    raise DomainError("district_empty", f"片区 {district} 下没有场地")
            else:
                raise DomainError("missing_target", "闭馆须指定 venue_ids 或 district")
            self.seq["closure"] += 1
            closure = {
                "id": f"C{self.seq['closure']:04d}",
                "kind": kind,
                "reason": data.get("reason", kind),
                "start": dump_dt(start),
                "end": dump_dt(end),
                "capacity_override": override,  # None/0 表示全闭
                "venue_ids": [v["id"] for v in targets],
                "district": district,
                "ts": now_iso(),
            }
            self.closures.append(closure)
            affected = self._notify_overlapping_bookings(closure, start, end)
            self._save_state()
            return {"closure": closure, "notified_bookings": affected}

    def _notify_overlapping_bookings(self, closure: dict, start: datetime, end: datetime) -> list:
        affected = []
        for booking in self.bookings.values():
            if booking["status"] != "confirmed":
                continue
            if booking["venue_id"] not in closure["venue_ids"]:
                continue
            if not intervals_overlap(start, end, parse_dt(booking["start"]), parse_dt(booking["end"])):
                continue
            booking["status"] = "affected"
            venue = self.venues[booking["venue_id"]]
            self.seq["notification"] += 1
            note = {
                "id": f"N{self.seq['notification']:05d}",
                "booking_id": booking["id"],
                "venue_id": venue["id"],
                "contact": booking.get("contact", ""),
                "message": (
                    f"您在 {venue['name']} {booking['start']} 至 {booking['end']} 的使用安排"
                    f"受{closure['kind']}影响（{closure['reason']}，{closure['start']} 至 {closure['end']}），"
                    "该时段容量已调整，请改约或咨询运营方。"
                ),
                "closure_id": closure["id"],
                "ts": now_iso(),
            }
            self.notifications.append(note)
            affected.append({"booking_id": booking["id"], "notification_id": note["id"]})
        return affected

    def _matching_closures(self, venue_id: str, start: datetime, end: datetime) -> list:
        out = []
        for closure in self.closures:
            if venue_id not in closure["venue_ids"]:
                continue
            if intervals_overlap(start, end, parse_dt(closure["start"]), parse_dt(closure["end"])):
                out.append(closure)
        return out

    # ---------- 可用性（居民查询） ----------

    def availability(self, venue_id: str, start: str | None, end: str | None) -> dict:
        with self.lock:
            venue = self._require_venue(venue_id)
            if start and end:
                s, e = parse_dt(start), parse_dt(end)
                if e <= s:
                    raise DomainError("bad_interval", "结束时间必须晚于开始时间")
            else:
                now = datetime.now(timezone.utc)
                s = e = now
            return self._availability(venue, s, e, instant=not (start and end))

    def _availability(self, venue: dict, start: datetime, end: datetime, instant: bool = False) -> dict:
        closures = self._matching_closures(venue["id"], start, end)
        effective_capacity = venue["capacity"]
        reasons = []
        for closure in closures:
            if closure["capacity_override"] is None or closure["capacity_override"] == 0:
                effective_capacity = 0
            else:
                effective_capacity = min(effective_capacity, closure["capacity_override"])
            reasons.append(f"{closure['kind']}: {closure['reason']}（{closure['start']} 至 {closure['end']}）")
        disabled_equipment = [
            {"id": e["id"], "name": e["name"], "status": e["status"], "maintainer": e.get("maintainer")}
            for e in self.equipment.values()
            if e["venue_id"] == venue["id"] and e["status"] != "正常"
        ]
        if disabled_equipment:
            reasons.append("部分器材停用/维修中")
        # 已确认预约占用（受闭馆影响的预约已释放容量）。
        # 查询窗口可能跨越多个时段，取窗口内并发预约人数的峰值。
        sweep: list = []
        overlapping = []
        for booking in self.bookings.values():
            if booking["venue_id"] != venue["id"] or booking["status"] != "confirmed":
                continue
            b_start, b_end = parse_dt(booking["start"]), parse_dt(booking["end"])
            if intervals_overlap(start, end, b_start, b_end):
                sweep.append((max(start, b_start), 1, booking["party_size"]))
                sweep.append((min(end, b_end), -1, booking["party_size"]))
                overlapping.append(booking["id"])
        booked = 0
        if sweep:
            running = 0
            for _ts, delta, size in sorted(sweep, key=lambda x: (x[0], x[1])):
                running += size if delta == 1 else -size
                booked = max(booked, running)
        current_occupancy = self.occupancy.get(venue["id"], 0)
        live_state = "临时关闭" if any(
            c["capacity_override"] in (None, 0)
            and parse_dt(c["start"]) <= datetime.now(timezone.utc) < parse_dt(c["end"])
            for c in closures
        ) else venue["status"]
        return {
            "venue_id": venue["id"],
            "name": venue["name"],
            "venue_status": live_state,
            "window": {"start": dump_dt(start), "end": dump_dt(end)} if not instant else None,
            "base_capacity": venue["capacity"],
            "effective_capacity": effective_capacity,
            "booked": booked,
            "current_occupancy": current_occupancy,
            "available": max(0, effective_capacity - booked),
            "reasons": reasons,
            "closures": [{"id": c["id"], "kind": c["kind"], "reason": c["reason"],
                          "start": c["start"], "end": c["end"]} for c in closures],
            "disabled_equipment": disabled_equipment,
            "accessibility": venue.get("accessibility", {}),
            "hours": venue.get("hours", {}),
        }

    # ---------- 维修队列 ----------

    def _open_ticket(self, venue_id: str, equipment_id: str, detail: str, source_event_id: str) -> dict:
        self.seq["ticket"] += 1
        ticket = {
            "id": f"R{self.seq['ticket']:04d}",
            "venue_id": venue_id,
            "equipment_id": equipment_id,
            "detail": detail,
            "status": "queued",
            "source_event_id": source_event_id,
            "created_ts": now_iso(),
            "history": [{"ts": now_iso(), "status": "queued", "note": "巡检上报自动入队"}],
        }
        self.tickets[ticket["id"]] = ticket
        return ticket

    def list_tickets(self, status: str | None = None, venue_id: str | None = None) -> list:
        with self.lock:
            out = []
            for ticket in self.tickets.values():
                if status and ticket["status"] != status:
                    continue
                if venue_id and ticket["venue_id"] != venue_id:
                    continue
                out.append(dict(ticket))
            return sorted(out, key=lambda t: t["id"])  # 入队编号即队列顺序

    def advance_ticket(self, ticket_id: str, data: dict) -> dict:
        action = data.get("action")
        with self.lock:
            ticket = self.tickets.get(ticket_id)
            if not ticket:
                raise DomainError("ticket_not_found", "维修工单不存在", 404)
            equip = self._require_equipment(ticket["equipment_id"])
            if action == "start":
                if ticket["status"] != "queued":
                    raise DomainError("invalid_transition", "只有 queued 工单可以开始维修", 409)
                ticket["status"] = "in_progress"
                equip["status"] = "维修中"
            elif action == "resolve":
                if ticket["status"] not in ("queued", "in_progress"):
                    raise DomainError("invalid_transition", "工单已完结", 409)
                settlement = data.get("fund_settlement")
                ledger_entry_id = None
                if settlement:
                    # 维修资金支出与资产恢复一一对应，审计可从资金追到改善结果。
                    entry = self._append_entry({
                        "type": "expenditure",
                        "batch_id": settlement["batch_id"],
                        "amount": settlement["amount"],
                        "purpose": f"维修 {equip['name']}（工单 {ticket['id']}）",
                        "evidence": settlement.get("evidence", {}),
                        "asset_change": {
                            "kind": "equipment", "id": equip["id"], "action": "repair",
                            "ticket_id": ticket["id"],
                        },
                    })
                    ledger_entry_id = entry["entry_id"]
                ticket["status"] = "resolved"
                ticket["resolved_ts"] = now_iso()
                ticket["settlement_entry_id"] = ledger_entry_id
                equip["status"] = "正常"
            else:
                raise DomainError("bad_action", "action 须为 start/resolve")
            ticket["history"].append({"ts": now_iso(), "status": ticket["status"], "note": data.get("note", "")})
            self._save_state()
            return dict(ticket)

    # ---------- 公益金批次与台账 ----------

    def create_batch(self, data: dict) -> dict:
        for field in ("id", "year", "amount", "approved_purpose"):
            if data.get(field) is None:
                raise DomainError("missing_field", f"资金批次缺少 {field}")
        if not isinstance(data["amount"], (int, float)) or data["amount"] <= 0:
            raise DomainError("bad_amount", "批次金额须为正数")
        with self.lock:
            if data["id"] in self.batches:
                raise DomainError("batch_exists", "批次编号已存在", 409)
            batch = {
                "id": data["id"],
                "year": int(data["year"]),
                "source": data.get("source", "体彩公益金"),
                "amount": data["amount"],
                "approved_purpose": data["approved_purpose"],
                "created_ts": now_iso(),
            }
            self.batches[batch["id"]] = batch
            self._save_state()
            return dict(batch)

    def batch_balance(self, batch_id: str) -> dict:
        with self.lock:
            batch = self.batches.get(batch_id)
            if not batch:
                raise DomainError("batch_not_found", "资金批次不存在", 404)
            return self._balance(batch_id, batch)

    def _balance(self, batch_id: str, batch: dict) -> dict:
        carry_in = sum(e["amount"] for e in self.ledger
                       if e["type"] == "carryforward" and e.get("to_batch_id") == batch_id)
        spent = sum(e["amount"] for e in self.ledger
                    if e["type"] == "expenditure" and e["batch_id"] == batch_id)
        returned = sum(e["amount"] for e in self.ledger
                       if e["type"] == "return" and e["batch_id"] == batch_id)
        carried_out = sum(e["amount"] for e in self.ledger
                          if e["type"] == "carryforward" and e["batch_id"] == batch_id)
        # 退回是受款方把钱缴回批次账户（同年可重新列支），必须以独立 return 分录体现，
        # 不能直接改小原支出；结转与退回在余额中各自可见。
        available = batch["amount"] + carry_in - spent + returned - carried_out
        return {
            "batch_id": batch_id,
            "year": batch["year"],
            "initial": batch["amount"],
            "carry_in": carry_in,
            "spent": spent,
            "returned": returned,
            "carry_out": carried_out,
            "available": available,
        }

    def append_entry(self, data: dict) -> dict:
        with self.lock:
            entry = self._append_entry(data)
            self._save_state()
            return entry

    def _append_entry(self, data: dict) -> dict:
        etype = data.get("type")
        if etype not in LEDGER_TYPES:
            raise DomainError("bad_ledger_type", f"分录类型须为 {LEDGER_TYPES}")
        batch_id = data.get("batch_id", "")
        batch = self.batches.get(batch_id)
        if not batch:
            raise DomainError("batch_not_found", f"资金批次不存在: {batch_id}", 404)
        amount = data.get("amount")
        if not isinstance(amount, (int, float)) or amount <= 0:
            raise DomainError("bad_amount", "分录金额须为正数")
        purpose = data.get("purpose", "")
        if not purpose:
            raise DomainError("missing_purpose", "每笔台账必须记录批准用途")
        evidence = data.get("evidence")
        asset_change = data.get("asset_change")
        to_batch_id = None
        if etype == "expenditure":
            if not evidence:
                raise DomainError("missing_evidence", "支出必须附验收证据（验收人/单据/日期）")
            if not asset_change or not asset_change.get("kind") or not asset_change.get("action"):
                raise DomainError("missing_asset_change", "支出必须对应真实资产变化")
            balance = self._balance(batch_id, batch)["available"]
            if amount > balance + 1e-9:
                raise DomainError("insufficient_fund", f"批次可用余额 {balance}，不足列支 {amount}", 409)
            self._apply_asset_change(asset_change, batch_id, evidence)
        elif etype == "return":
            spent = self._balance(batch_id, batch)["spent"]
            if amount > spent + 1e-9:
                raise DomainError("return_exceeds_spent", "退回金额不能超过累计支出", 409)
        elif etype == "carryforward":
            to_batch_id = data.get("to_batch_id")
            target = self.batches.get(to_batch_id or "")
            if not target:
                raise DomainError("batch_not_found", "结转目标批次不存在", 404)
            if target["year"] <= batch["year"]:
                raise DomainError("bad_carryforward", "跨年度结余只能结转至后续年度批次")
            balance = self._balance(batch_id, batch)["available"]
            if amount > balance + 1e-9:
                raise DomainError("insufficient_fund", f"可结转余额仅 {balance}", 409)
        seq = len(self.ledger) + 1
        entry = {
            "entry_id": f"F{batch['year']}-{seq:04d}",
            "seq": seq,
            "ts": now_iso(),
            "type": etype,
            "batch_id": batch_id,
            "to_batch_id": to_batch_id,
            "amount": amount,
            "purpose": purpose,
            "evidence": evidence,
            "asset_change": asset_change,
        }
        prev_hash = self._ledger_hashes[-1] if self._ledger_hashes else ""
        entry["prev_hash"] = prev_hash
        entry["hash"] = canonical_hash({k: v for k, v in entry.items()}, prev_hash)
        self.ledger.append(entry)
        self._ledger_hashes.append(entry["hash"])
        self._append_ledger_file(entry)
        return dict(entry)

    def _apply_asset_change(self, change: dict, batch_id: str, evidence: dict):
        kind, action = change["kind"], change["action"]
        if kind == "venue":
            venue = self._require_venue(change.get("id", ""))
            if action == "create":
                if venue["status"] != "规划中":
                    raise DomainError("asset_not_planning", "只有规划中场地可由建设支出验收开放")
                venue["status"] = "开放中"
                venue["funded_by"] = batch_id
                venue["accepted_evidence"] = evidence
            elif action == "upgrade":
                venue.setdefault("upgrades", []).append({"batch_id": batch_id, "evidence": evidence, "ts": now_iso()})
            elif action == "retire":
                venue["status"] = "已退役"
            else:
                raise DomainError("bad_asset_action", "场地资产动作须为 create/upgrade/retire")
        elif kind == "equipment":
            equip = self._require_equipment(change.get("id", ""))
            if action == "create":
                if equip.get("installed_by"):
                    raise DomainError("asset_exists", "器材已由其他批次安装")
                equip["installed_by"] = batch_id
                equip["accepted_evidence"] = evidence
            elif action == "repair":
                if equip["status"] != "维修中" and change.get("ticket_id"):
                    # 工单 resolve 路径已先置位；允许直接列支的维修支出。
                    equip["status"] = "正常"
                equip.setdefault("repairs", []).append(
                    {"batch_id": batch_id, "evidence": evidence,
                     "ticket_id": change.get("ticket_id"), "ts": now_iso()}
                )
            elif action == "retire":
                equip["status"] = "停用"
            else:
                raise DomainError("bad_asset_action", "器材资产动作须为 create/repair/retire")
        else:
            raise DomainError("bad_asset_kind", "资产种类须为 venue/equipment")

    def audit_trail(self, batch_id: str | None = None, venue_id: str | None = None) -> dict:
        """审计视角：资金去向 -> 验收证据 -> 资产现状。"""
        with self.lock:
            entries = [dict(e) for e in self.ledger]
            if batch_id:
                if batch_id not in self.batches:
                    raise DomainError("batch_not_found", "资金批次不存在", 404)
                entries = [e for e in entries if e["batch_id"] == batch_id or e.get("to_batch_id") == batch_id]
            assets = []
            for entry in entries:
                change = entry.get("asset_change")
                if not change:
                    continue
                if venue_id:
                    if change["kind"] == "venue" and change.get("id") != venue_id:
                        continue
                    if change["kind"] == "equipment":
                        equip = self.equipment.get(change.get("id", ""))
                        if not equip or equip["venue_id"] != venue_id:
                            continue
                current = None
                if change["kind"] == "venue":
                    v = self.venues.get(change.get("id"))
                    if v:
                        current = {"id": v["id"], "name": v["name"], "status": v["status"], "kind": v["kind"]}
                else:
                    e = self.equipment.get(change.get("id"))
                    if e:
                        current = {"id": e["id"], "name": e["name"], "status": e["status"],
                                   "venue_id": e["venue_id"]}
                assets.append({
                    "entry_id": entry["entry_id"],
                    "batch_id": entry["batch_id"],
                    "amount": entry["amount"],
                    "purpose": entry["purpose"],
                    "evidence": entry.get("evidence"),
                    "change": change,
                    "asset_now": current,
                })
            balances = [self._balance(bid, b) for bid, b in sorted(self.batches.items())]
            if batch_id:
                balances = [b for b in balances if b["batch_id"] == batch_id]
            return {"entries": entries, "asset_results": assets, "balances": balances}

    # ---------- 匿名客流与覆盖分析 ----------

    def record_footfall(self, data: dict) -> dict:
        venue_id = data.get("venue_id", "")
        buckets = data.get("buckets")
        if not isinstance(buckets, list) or not buckets:
            raise DomainError("missing_buckets", "客流上报须为分时段聚合计数 buckets 列表")
        scan_forbidden_keys(data)
        with self.lock:
            self._require_venue(venue_id)
            accepted = []
            for item in buckets:
                bucket = item.get("bucket")
                count = item.get("entries")
                if not isinstance(bucket, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:00", bucket):
                    raise DomainError("bad_bucket", "bucket 须为整点小时，格式 YYYY-MM-DD-DDTHH:00")
                if not isinstance(count, int) or count < 0:
                    raise DomainError("bad_count", "entries 须为非负整数")
                # 只存计数，无法回溯到人。
                key = f"{venue_id}|{bucket}"
                self.footfall[key] = self.footfall.get(key, 0) + count
                accepted.append({"bucket": bucket, "total": self.footfall[key]})
            self._save_state()
            return {"venue_id": venue_id, "buckets": accepted}

    def footfall_stats(self, venue_id: str | None = None) -> dict:
        with self.lock:
            rows = []
            for key, count in self.footfall.items():
                vid, bucket = key.split("|", 1)
                if venue_id and vid != venue_id:
                    continue
                venue = self.venues.get(vid)
                hour = int(bucket[11:13])
                weekend = _is_weekend(bucket)
                rows.append({
                    "venue_id": vid,
                    "district": venue["district"] if venue else None,
                    "bucket": bucket,
                    "hour": hour,
                    "is_weekend": weekend,
                    "entries": count,
                    "capacity": venue["capacity"] if venue else None,
                    "utilization": round(count / venue["capacity"], 3) if venue and venue["capacity"] else None,
                })
            by_hour = {}
            weekend_peak = {"entries": 0}
            for row in rows:
                by_hour.setdefault(row["hour"], 0)
                by_hour[row["hour"]] += row["entries"]
                if row["is_weekend"] and row["entries"] >= weekend_peak["entries"]:
                    weekend_peak = row
            return {
                "rows": sorted(rows, key=lambda r: r["bucket"]),
                "by_hour": [{"hour": h, "entries": by_hour[h]} for h in sorted(by_hour)],
                "weekend_peak": weekend_peak if weekend_peak["entries"] else None,
            }

    def upsert_district(self, data: dict) -> dict:
        if not data.get("name"):
            raise DomainError("missing_field", "片区缺少 name")
        with self.lock:
            self.districts[data["name"]] = {
                "name": data["name"],
                "population": data.get("population"),
                "center": data.get("center"),
            }
            self._save_state()
            return dict(self.districts[data["name"]])

    def coverage_report(self) -> dict:
        """分时段之外的服务半径分析：发现偏远片区覆盖不足。"""
        with self.lock:
            report = []
            names = set(self.districts) | {v["district"] for v in self.venues.values()}
            for name in sorted(names):
                venues = [v for v in self.venues.values()
                          if v["district"] == name and v["status"] != "已退役"]
                population = (self.districts.get(name) or {}).get("population")
                total_cap = sum(v["capacity"] for v in venues)
                accessible = sum(1 for v in venues if v.get("accessibility", {}).get("barrier_free"))
                kinds = sorted({v["kind"] for v in venues})
                gaps = []
                if not venues:
                    gaps.append("片区内无公共体育设施")
                else:
                    for needed in ("体育公园", "百姓健身房", "山地步道"):
                        if needed not in kinds:
                            gaps.append(f"缺少{needed}")
                    if population and total_cap / population < 0.002:
                        gaps.append("千人容量不足（<2/千人）")
                    if accessible == 0:
                        gaps.append("无无障碍达标场地")
                report.append({
                    "district": name,
                    "population": population,
                    "venue_count": len(venues),
                    "total_capacity": total_cap,
                    "accessible_venue_count": accessible,
                    "kinds": kinds,
                    "venues": [{"id": v["id"], "name": v["name"], "radius_m": v["service_radius_m"],
                                "status": v["status"]} for v in venues],
                    "coverage_gaps": gaps,
                })
            return {"districts": report}


def _is_weekend(bucket: str) -> bool:
    return datetime.strptime(bucket[:10], "%Y-%m-%d").weekday() >= 5
