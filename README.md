# okx-quant-lab（模块化/微服务风格的数字货币量化交易项目骨架）

> 重要声明：以下内容仅用于工程与研究用途，不构成任何投资建议。请先在 OKX 模拟盘（Demo）充分验证，再逐步放大规模。

## 1. 你将得到什么

- **微服务/模块化架构（单仓库多服务容器）**：Data / Strategy / Risk / Execution / Portfolio / API / AI(可选)  
- **事件驱动（NATS）**：市场数据、信号、风控审批、订单请求、订单回报、资金/仓位快照均以事件流方式流转  
- **数据库（PostgreSQL）**：订单、成交、行情采样、权益快照、信号等持久化  
- **备份（可选）**：`pg_dump` 定时备份到 volume  
- **动态仪表盘（可选）**：Prometheus + Grafana（默认不启用以节省 16GB 内存）  
- **AI/参数自适应（可选）**：Optuna 做网格参数的轻量自动调参，把结果写入 Redis 并推送事件给策略服务

## 2. 针对你的机器（M4 Mac / 16GB / OrbStack）的最优建议

- **消息总线用 NATS**：比 Kafka 轻很多，延迟/吞吐足够，资源占用低（适合 16GB）。  
- **默认只启动 core**：先跑通闭环；观测栈（Prom+Grafana）和备份/AI 用 docker compose profiles 按需启用。
- **控制订阅品种数量**：先 2-4 个 instId；策略也先跑 1-2 个，确认稳定后再扩。
- **强制防呆**：默认 `OKX_SIMULATED_TRADING=1`，且 `ALLOW_LIVE_TRADING=false`，避免误触实盘。

## 3. 一键启动

### 3.1 方式 A：最简单（本地 DryRun，不需要 APIKey）

1) 复制 `.env.example` 为 `.env`  
```bash
cp .env.example .env
```

2) 启动（核心服务）  
```bash
make up
```

3) 打开 API：  
- http://localhost:8000/docs （FastAPI Swagger）

> DRYRUN 模式会本地模拟下单/成交，便于你先验证架构与链路。

### 3.2 方式 B：连接 OKX 模拟盘（需要 Demo APIKey）

1) 在 `.env` 中设置：
- `EXECUTION_MODE=OKX`
- `OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE`
- `OKX_SIMULATED_TRADING=1`（模拟盘）

2) 启动：
```bash
make up
```

> OKX 模拟盘需要请求头 `x-simulated-trading=1`。该项目已内置。  
> WebSocket 登录签名与 `/users/self/verify` 的拼接规则也已内置。

### 3.3 启动观测仪表盘（可选）

```bash
make up-obs
```

- Prometheus: http://localhost:9090  
- Grafana: http://localhost:3000 （默认 admin/admin，可在 .env 改）

## 4. 服务与职责（建议你从这里理解扩展点）

- **data**：连 OKX WS 公共频道（tickers），发布 `md.ticker.*` 事件；写 Redis 最新价；行情采样落库  
- **strategy**：订阅行情事件，运行策略（grid / pairs / cash_carry），输出订单意图 `signal.order_intent`  
- **risk**：订阅订单意图，做风控（额度、价格偏离、频率、KillSwitch…）通过则发布 `order.request`  
- **execution**：订阅 `order.request`，下单/撤单（OKX 或 DRYRUN），输出 `execution.ack`  
- **portfolio**：连 OKX 私有 WS 订单频道，获取订单状态/成交增量，更新仓位/权益，写 DB/Redis，发布 `okx.order_update`、`portfolio.*`  
- **api**：对外查询 & 运维控制（KillSwitch、最近订单/成交、最新行情、仓位等）

## 5. 扩展路线（你后续可以按这个方向升级）

- 数据层：接入 K线、深度、成交明细；引入分区/TimescaleDB；冷/热分层存储  
- 执行层：引入订单状态机、挂单管理、撤单重试、交易所限速自适应  
- 风控：组合保证金、VaR/ES、净敞口控制、跨策略风险预算、日内回撤熔断  
- 策略：做成插件体系（entrypoints）；加入回测/仿真撮合；引入协整检验/半衰期估计  
- 可观测：OpenTelemetry + Loki/Tempo；全链路 trace；告警（Alertmanager）

---

## 6. 目录结构

```
app/
  core/          # 配置、日志、NATS、Redis、metrics
  db/            # SQLAlchemy models & session
  exchanges/okx/ # OKX REST/WS + 签名
  schemas/       # 事件模型（pydantic）
  strategies/    # grid / pairs / cash_carry
  services/      # data/strategy/risk/execution/portfolio/api/ai
infra/
  prometheus/
  grafana/
scripts/
  init_db.py
  backup_loop.sh
```

