# 公共健身设施运营

连接公益资金、体育设施、开放容量、巡检和维修成效。主管部门不只看到“建了多少”，
还能看到周末满场、器材停用、偏远片区覆盖缺口与维修资金的真实改善。

`fixtures/domain.json` 保存领域名词和状态样例；运行 `python3 service.py --check`
可检查基础配置并校验公益金台账链，执行 `npm test` 运行全部契约与场景测试。

## 核心约束的实现方式

- **事件幂等**：闸机与巡检事件必须带 `event_id`；离线补传原样返回首次受理结果（`dup: true`），
  客流、在场人数、维修工单均不重复计数（12 线程并发上报同一事件仅生效一次）。
- **容量联动**：临时闭馆、赛事占用、暴雨/空气污染预警（可按 `venue_ids` 或整个 `district`）
  即时改写有效容量（支持 `capacity_override` 部分开放），并把时段重叠的预约标记为 `affected`、
  按 `contact` 句柄发出通知；居民查询 `/venues/{id}/availability` 能看到真正可用量与关闭原因。
- **公益金 append-only 台账**：支出必须带批准用途 `purpose`、验收证据 `evidence`
  与真实 `asset_change`（场地开放/升级、器材安装/维修）；结余退回是独立 `return` 分录，
  跨年度结余只能 `carryforward` 到后续年度批次，禁止改字段掩盖。每条分录像 SHA-256
  哈希成链，篡改金额在重启或 `/funds/verify` 时即被发现。
- **维修可追溯**：巡检故障自动入队（同一器材在途工单不重复建档），工单 resolve 时的维修
  支出与器材恢复一一对应，审计可经 `/funds/audit?venue_id=…` 从资金追到设施改善现状。
- **匿名客流**：`/footfall` 只接受整点小时聚合计数，递归拒绝 user/device/mac/身份证等
  任何可能还原个人轨迹的字段；统计仅用于分时段利用率与周末峰值、服务半径覆盖分析。
- **重启安全**：状态原子落盘（`data/state.json` + 哈希链式 `data/ledger.log`），
  重启后预约占用、维修队列、事件去重表保持不变。

## 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/admin/venues`、`/admin/districts` | 登记场地（类型/容量/无障碍/开放时段/服务半径/维保方）与片区 |
| POST | `/venues/{id}/equipment` | 登记器材及维保责任 |
| POST | `/events` | 闸机 `gate_entry`/`gate_exit`、巡检 `inspection`（幂等） |
| POST | `/bookings`、`POST /bookings/{id}/cancel` | 使用安排预约/取消，按时段并发峰值校验容量 |
| POST | `/closures` | 临时闭馆/赛事占用/暴雨/空气污染预警，联动容量并通知受影响预约 |
| GET | `/venues/{id}/availability` | 居民查某时段真正可用场地、容量与关闭原因 |
| GET | `/tickets`、`POST /tickets/{id}/advance` | 维修队列（queued→in_progress→resolved）与资金结算 |
| POST | `/funds/batches`、`GET /funds/batches/{id}/balance` | 公益金分年度批次与余额构成 |
| POST | `/funds/entries` | expenditure / return / carryforward 台账分录 |
| GET | `/funds/audit`、`/funds/verify` | 资金→证据→资产现状追溯；哈希链完整性校验 |
| POST | `/footfall`、`GET /footfall/stats` | 匿名分时段客流上报与利用率分析 |
| GET | `/coverage` | 按片区统计类型覆盖、千人容量与无障碍缺口 |

状态文件默认写入 `data/`（可用 `--data-dir` 或 `FITNESS_DATA_DIR` 覆盖），已在 `.gitignore` 中忽略。
