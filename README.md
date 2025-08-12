# Redis Fatigue Test (Go)

一个基于 Go 的 Redis 疲劳测试工具，采用配置文件驱动（不使用命令行参数）。

- 支持单机或集群模式（自动根据 `redis.addrs` 是否配置选择）
- 支持常用操作：GET / SET / HGET / HSET
- 支持写入过期时间、管道/事务管道批处理
- 多线程并发、分轮次统计
- 输出每轮与总体统计：QPS、成功率、延迟均值与 P50/P95/P99

目录结构：
- `go.mod`：Go 模块定义
- `config.yaml`：测试配置
- `main.go`：主程序
- `README.md`：使用说明

## 1. 环境要求
- Go 1.21+
- 外网或可用的 Go 模块代理

在中国大陆环境建议使用 Go 模块代理（阿里/七牛/清华等镜像均可，此处以 goproxy.cn 为例）：

```bash
export GOPROXY=https://goproxy.cn,direct
```

> 若使用 zsh，可将上述配置加入 `~/.zshrc` 以长期生效。

## 2. 配置说明 (`config.yaml`)
```yaml
redis:
  # 单机地址，或使用 addrs 配置集群模式
  addr: "127.0.0.1:6379"
  # addrs: ["127.0.0.1:6379", "127.0.0.1:6380"]
  password: ""
  db: 0
  tls: false

test:
  threads: 4              # 并发线程数
  loops: 3                # 总轮次，每轮都会重新统计并打印结果
  ops_per_thread: 1000    # 每个线程每轮执行的操作次数
  key_space: 10000        # 键空间大小，随机键会落在 [0, key_space)
  value_size: 128         # 字符串写入的value大小(字节)
  expire_seconds: 300     # 所有写入的过期时间(秒)
  pipeline: 1             # 写入时批量大小，>1时启用(建议>=1)。=1 时仍可使用 TxPipeline 以保证 HSET+EXPIRE 原子性
  use_tx: true            # 写入使用 TxPipeline(事务管道)，保证写与过期原子性
  hash_field_count: 5     # HSET时写入的字段个数
  # 操作权重，权重为0则不执行该操作
  # 支持: get, set, hget, hset
  operation_mix:
    get: 40
    set: 40
    hget: 10
    hset: 10
```

- 当 `redis.addrs` 非空时自动使用 Redis Cluster 客户端，否则使用单机客户端
- 键空间分离：字符串键前缀 `str_key_*`；哈希键前缀 `hash_key_*`，避免 `WRONGTYPE`
- HSET 写入后会设置 `EXPIRE`，当 `use_tx: true` 且 `pipeline>1` 时使用 TxPipeline 保证原子性

## 3. 运行
首次运行会自动拉取依赖模块，建议先设置镜像：

```bash
export GOPROXY=https://goproxy.cn,direct
```

在项目根目录执行：

```bash
# 运行
go run .

# 或构建并运行
go build -o redis-fatigue .
./redis-fatigue
```

程序会读取当前目录下的 `config.yaml` 并开始压测。每轮结束打印统计信息，最后打印总体汇总。

## 4. 输出示例
```
================================================================
Round 1 finished in 2.345s
Total Ops: 4000, Success: 3998, Fail: 2, SuccessRate: 99.95%
QPS: 1705.78
Latency avg:1.2ms p50:1.0ms p95:2.5ms p99:4.0ms
...
================================================================
All rounds finished
Total Ops: 12000, Success: 11990, Fail: 10, SuccessRate: 99.92%
Total Time: 7.123s, Overall QPS: 1684.12
Latency avg:1.3ms p50:1.1ms p95:2.7ms p99:4.2ms
```

## 5. 扩展操作类型
当前实现了 GET / SET / HGET / HSET。若需扩展更多操作（如 DEL、MGET、INCR、LPUSH/LPOP 等）：
- 在 `main.go` 中的 `opType` 与 `OperationMix` 增加枚举项与权重
- 在 `worker()` 的批处理发送逻辑中添加相应命令与错误判断
- 注意写入类操作与过期的原子性（可通过 TxPipeline 保证）

## 6. 常见问题
- 若出现连接认证错误，请在 `config.yaml` 设置正确的 `password`
- 集群模式请配置 `redis.addrs`，并删除或忽略 `redis.addr`
- 非管道情况下难以精确统计每条命令的成功与失败，建议将 `pipeline>=1` 并保留 `use_tx: true`

---
如需增加 CSV 导出、分操作类型统计、错误类型统计、RDB/AOF 压力场景等，请告知，我可以在保持现有结构的前提下增量扩展。
# redis_stress_test
