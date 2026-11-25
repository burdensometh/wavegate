# v1 持久样本

这些文件来自 `wavegate demo` 在 ECS Linux Docker 内的实际输出。重新运行演示会分配上游端口、发送请求、写入持久文件并生成相同转换序列。样本 ID、端口和依赖它们的哈希随运行变化。

- `report.v1.json` 给出每组实际请求命中的节点、端点和角色。
- `endpoint-events.v1.json` 保存假 API 的端点清单及 NDJSON watch 事件。
- `decision-events.v1.json` 保存依序写入的调谐决策。
- `*.release.v1.json` 和 `*.receipt.v1.json` 保存状态及决策收据。
- `*.window.v1.json` 保存参与封存判定的样本、阈值和摘要。
- `state/` 保存用于启动恢复的五份原始文档。可复制到临时目录后调用 `wavegate inspect-state -state-directory 路径`。要发送新探针时，应运行演示以启动样本对应的上游。
