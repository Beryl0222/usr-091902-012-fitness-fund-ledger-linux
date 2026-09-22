# 公共健身设施运营

连接体育公园、百姓健身房、山地步道的**场地/器材/开放时段/无障碍能力/资金批次/维保责任**，
让公益金支出可追溯、可用容量真实可信、客流分析不触碰个人轨迹。

## 运行与测试

```bash
python3 service.py --check          # 基础自检
python3 service.py --port 8000      # 启动 HTTP 服务（数据默认写入 ./data）
python3 service.py --port 8000 --data-dir /var/lib/fitness
npm test                            # 运行全部 Python 测试（领域 + HTTP + 契约）
```

## 核心语义

| 运营要求 | 实现方式 |
| --- | --- |
| 闸机/巡检离线补传不重复计数 | 事件以 `来源:事件id` 幂等去重，重启后仍然生效；重复提交返回 `dedup:true` |
| 匿名客流，不还原个人轨迹 | 客流接口字段白名单，拒绝 user_id/手机号/设备指纹等任何身份字段；只输出分时段聚合计数 |
| 闭馆/赛事/预警联动容量并通知 | 临时闭馆容量归零、赛事占用按比例折减、暴雨/空气污染预警按等级折减户外场地（室内不受限）；受影响时段内的有效预约自动生成通知 |
| 居民查到真正可用场地及原因 | `GET /availability` 返回每场地容量、剩余名额与逐条关闭/折减原因（含停用器材清单） |
| 每笔公益金对应用途、验收证据、资产变化 | 支出必须匹配批次批准用途；验收必须提交证据编号、验收人与真实资产变化（场地建成/器材到位/无障碍改造） |
| 跨年度结余与退回不能靠改字段掩盖 | 资金为只追加的 SHA-256 哈希链（`fund.jsonl`），退回/结转只能新增记录；篡改或断链会被 `/audit` 发现并拒绝启动 |
| 服务重启不扰乱预约与维修队列 | 事件溯源：重启重放 `events.jsonl` + 资金哈希链，完整恢复预约占用、工单队列、闭馆/预警、通知与幂等键 |
| 审计从资金追到设施改善 | `GET /fund/batches/{id}/trace` 串联批次 → 支出 → 验收证据 → 场地/器材资产 |
| 偏远片区覆盖不足可度量 | 需求点按网格/社区粒度登记，`GET /coverage?radius_m=1000` 计算到最近在役场地的距离并分类 |

金额以"分"整数存储；时间使用 ISO8601 UTC 字符串（按字典序即可比较时段重叠）。

## 主要接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 服务身份（稳定契约） |
| POST | `/facilities` | 登记场地（类型、容量、位置、无障碍、维保责任） |
| POST | `/facilities/{id}/schedule` `/accessibility` `/maintenance` `/open` `/retire` | 开放时段、无障碍能力、维保责任、开放、退役 |
| POST | `/equipment` | 登记器材 |
| POST | `/fund/batches` | 创建分年度公益金批次（核定用途） |
| POST | `/expenditures` | 登记支出（校验用途与余额） |
| POST | `/expenditures/{id}/accept` | 验收：证据 + 验收人 + 资产变化 |
| POST | `/expenditures/{id}/return` | 退回（必须填原因，支持部分/全额） |
| POST | `/fund/batches/{id}/carry-over` | 跨年度结转（批次锁定，余额留痕） |
| GET | `/fund/batches/{id}/trace` | 资金→资产审计追溯链 |
| POST | `/events/visits` `/events/inspections` | 闸机客流/巡检事件（幂等、隐私白名单） |
| GET | `/visits` | 分时段匿名客流聚合 |
| POST | `/bookings`，`POST /bookings/{id}/cancel` | 使用安排预约（按实时可用容量校验） |
| POST | `/closures`，`POST /closures/{id}/lift` | 临时闭馆/赛事占用 |
| POST | `/forecasts`，`POST /forecasts/{id}/lift` | 暴雨/空气/高温/雷电预警（区级或场地级） |
| GET | `/availability` | 居民查询：start/end/district/kind/accessible_only |
| POST | `/work-orders`，`/{id}/start` `/{id}/complete` | 维修工单队列（场地级/器材级，可关联维修支出） |
| GET | `/work-orders` | 维修队列（未完工优先） |
| POST | `/demand-points` | 登记覆盖需求点（网格粒度） |
| GET | `/coverage` | 服务半径覆盖分析 |
| GET | `/notifications` | 闭馆/预警产生的受影响安排通知 |
| GET | `/audit` | 台账完整性、资金平衡与资源计数 |

错误以 JSON 返回：`400 bad_request` / `404 not_found` / `409 conflict`。

## 数据文件

* `data/events.jsonl`：运营事件日志（只追加），重启时重放恢复全部投影。
* `data/fund.jsonl`：公益金哈希链台账，每条含 `prev_hash`/`hash`，审计可独立校验。

`fixtures/domain.json` 保存领域名词与状态枚举，仅用于统一语义；业务记录一律由接口产生。
