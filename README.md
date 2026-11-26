# Wavegate

Wavegate 是一个 Kubernetes 风格的渐进交付控制器。它从端点 API 的 list/watch 维护就绪视图，以服务为单位安装加权流量规则，通过实际 HTTP 请求观测稳定版与候选版，按固定阶梯放量、晋级或回退。控制面提供可运行的假 API，上游使用可控的 loopback HTTP 端点，演示用两台逻辑节点承载它们。

## 启动

使用 Linux 和 Go 1.23。依赖均来自 Go 标准库，在 Linux Docker 中可离线构建和运行。

```sh
mkdir -p bin
go build -trimpath -o bin/wavegate ./cmd/wavegate
bin/wavegate serve -config config/wavegate.v1.json
```

配置文件给出监听地址、持久目录、默认与逐命名空间配额、watch 历史、决策保留量、调谐周期、上游超时和排空时间。API 与端点地址均限本机 loopback。一个进程持有一个状态目录的独占租约。使用相同状态目录重启时会先核对五份 v1 文档的提交边界，再恢复配额和调谐。

## 可重复演示

```sh
sh scripts/demo.sh ./bin/wavegate var/run-01
bin/wavegate inspect-state -state-directory var/run-01/state
```

指定一个新输出目录。演示读取 [v1 输入](samples/v1/demo-input.json)，为每个端点启动实际 HTTP 上游，写入假 API，再使用假时钟驱动调谐。每组发送 100 个请求，核对规则版本、上游身份、节点、角色、错误数和实际转发路径。

| 场景 | 可观察结果 |
| --- | --- |
| 正常放量 | 稳定版先收 100 个请求，候选版两阶收 10 和 40 个，晋级后新版以稳定角色收 100 个 |
| 人工暂停 | 原权重持续，恢复时从同一阶梯建立新绑定和新观察窗 |
| 自动与手动回退 | 候选版错误超过阈值，或接到人工中止后，将候选权重归零并释放预留 |
| 重启续接 | 提交下一阶流量后丢弃回执，重启从流量绑定确认原操作，绑定版本保持一致 |
| 命名空间隔离 | 同命名空间第二条发布等待预算，独立命名空间继续执行 |
| 节点与就绪性 | 稳定请求遍及两节点，候选请求仅到达就绪节点 |

输出包含实际持久状态、端点 list/watch 和决策序列、三份发布收据和三份可复算窗口。仓库附带一份 [ECS Docker 生成的 v1 输出](samples/v1/persisted/)。使用 `verify-receipt -input FILE` 和 `verify-window -input FILE` 在其他目录复验，或从标准输入读取 JSON。样本中的上游地址是生成当次所用的 loopback 端口。

## 调谐与持久契约

- `discovery` 使用全局单调的端点资源版本。watch 过期或序列断裂会触发重新 list，原子替换视图。端点 readiness、节点、修订版和 UID 都参与切流或派发；已退役 UID 也保留在身份账中。
- `rollout` 推进固定阶梯。规范创建后保持发布身份、修订版、阶梯和阈值，更新请求携带上一次返回的资源版本并可调整 `paused` 或置位 `abort`。一条发布可在初始稳定绑定建立后保持等待；双组可用才应用正权重。暂停、watch 中断或绑定重配会重新建立进阶所依赖的观察窗。
- `traffic` 以操作 ID、绑定资源版本及所有者执行原子比较并交换。候选权重确定角色，角色内轮询当前规则中的就绪端点。已选角色无可用端点时返回 503。晋级后新请求走新稳定版，旧稳定端点保留到最短排空时间和在途请求均结束。
- `metrics` 使用绑定版本隔离样本。每阶比较同一半开区间 `[start,end)` 的两组错误率和均值延迟。窗口到期后，两组达到样本下限才会封存判定。封存写入样本水位和摘要，可重新计算。代理直接记录上游结果，也接收显式 v1 样本。
- `journal` 先写入 traffic intent，再执行原子 apply，最后串接提交决策、发布 status 并更新配额视图。重启依据应用端实际操作 ID 对账。决策哈希和检查点用于一致性校验，收据包含保留决策与检查点上最近的该发布记录。
- `quota` 从日志恢复已授予预留。每个服务同一时间只允许一个活跃预留；命名空间同时限制发布数和候选就绪单位。晋级或回退完成时在同一个决策中清除预留。

| 持久文件 | 权威内容 |
| --- | --- |
| `endpoints.v1.json` | 端点库存、UID 账、watch 游标和有界历史 |
| `releases.v1.json` | ReleaseSpec、资源版本和最近一次已发布的 status |
| `traffic.v1.json` | 每个服务当前 TrafficBinding、操作 ID 和分配水位 |
| `samples.v1.json` | 已摄入样本、样本水位、封存窗口 |
| `decisions.v1.json` | 决策链、检查点、可重放发布和预留 |

每份文件用同目录暂存、fsync 和原子重命名发布。控制器按决策链区分已准备、流量已应用和已完成。状态是串行、单写者的文件基线，适合单个进程的可复现场景。指标桶在该基线中保留样本，以便复算历史判定。

## HTTP 观察口

`ns`、`name`、`svc`、`node`、`pod` 分别替换为命名空间、发布名、服务名、节点和端点名。错误以 JSON 返回。资源版本不匹配返回 409，旧游标返回 410。

| 方法和路径 | 数据 |
| --- | --- |
| `GET /healthz` | watch 同步状态及端点水位 |
| `GET /v1/endpoints` | v1 端点清单 |
| `GET /v1/endpoints?watch=1&resourceVersion=R` | R 之后的 NDJSON 端点事件 |
| `PUT /v1/endpoints` | 写入 v1 端点，首次写入用版本 0 |
| `DELETE /v1/endpoints/ns/svc/node/pod?uid=ID&resourceVersion=R` | 对应 UID 和版本的端点删除 |
| `PUT /v1/releases` | 创建或更新 v1 ReleaseSpec |
| `GET /v1/releases`、`GET /v1/releases/ns/name` | desired 及已发布 status |
| `GET /v1/releases/ns/name/receipt` | 可复算决策收据 |
| `GET /v1/namespaces/ns/quota` | 当前限额、占用和预留 |
| `GET /v1/traffic/ns/svc`、`GET /v1/traffic/ns/svc/activity` | 实际绑定及按 UID 的在途请求 |
| `POST /v1/samples`、`GET /v1/samples/ns/svc/R` | 当前绑定的样本摄入与封存证据 |
| `GET /v1/decisions?after=S&limit=N` | 决策分页及压缩水位 |
| `GET /v1/decisions/watch?after=S` | NDJSON 决策流，掉出保留段时发出 RESET 水位 |
| `/route/ns/svc/path` | 实际代理。响应头包含 binding、role、revision、endpoint |

## 原生验证

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go test -run '^$' -bench 'Benchmark(WatchRelist|RenderRule|AggregateWindow)$' -benchtime=250ms -count=3 -benchmem ./discovery ./traffic ./metrics
```

故障边界由 `clock.Manual`、`fault.Source`、`fault.Plane` 和 `fault.Windows` 提供。原生测试覆盖端点事件暂缓、库存变化后旧 UID 拒绝服务、断开 watch 与过期游标、在途排空、观察窗回放、apply 回执丢失以及窗口封存后决策尚未写入的进程中断。构造器支持为测试替换边界，日常启动由 `wavegate serve` 使用真实时钟和原生实现。
