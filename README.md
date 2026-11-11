# OpenCompute AI v0.8.1 — Public CUDA Worker → GPU Task Scheduler

已将通过公共接入验证的 CUDA Worker 接入现有 v0.7 GPU 资源清单与任务调度器：公共 GPU 节点使用独立的 \`public_gpu\` 身份，只能报告 GPU、领取 GPU 白名单任务并提交自己的租约结果，不能使用可信 CPU Worker 的身份接口。公共矩阵任务的结果会由调度器独立计算校验。

- [公共 CUDA Worker 接入、命令与安全边界](docs/v0.8.1-public-cuda-worker.md)
- [公共 GPU Worker 集成测试](tests/test_public_gpu_integration.py)

**当前边界：** 第一阶段只开放单个 CUDA 设备的 \`matmul_checksum\` 白名单任务；GPU 资源数据仍是 Worker 自报遥测，不是硬件远程证明。公共 GPU 上的 Qwen2 推理、多卡上报、跨设备模型分片和张量并行尚未完成。

---

# OpenCompute AI v0.6.0 — 可恢复任务与连接诊断

新增任务详情、取消/重试与筛选，连接诊断页、可主动断开的管理员会话、正确的跨平台 Worker 接入命令，以及可选 HTTPS 入口。沿用现有数据库和模型引擎。

- [v0.6 更新、迁移与部署检查](docs/v0.6-operations.md)
- [只读部署检查脚本](scripts/check-deployment.py) · [可选 HTTPS Compose](compose.public.yaml)
- [网页交互回归](web/tests/dashboard.test.cjs) · [真实浏览器/API/Worker 集成检查](web/tests/browser-smoke.py)

这是单管理员研究控制台，尚非陌生设备、多用户公开算力服务。普通 CPU 白名单任务可操作；Qwen2 仍是本机 CPU 实验，未实现远程设备大模型加速。

---

# OpenCompute AI v0.5.1 — Web Dashboard + v0.5.0 live Qwen2 queue

**浏览器控制台：** 启动 API 后访问 `http://127.0.0.1:8000/`。控制台使用管理员密钥连接现有集群 API，提供算力总览、Worker 设备清单、CPU 任务提交、模型目录、Qwen2 队列状态和设备接入说明。

**Windows 本地一键启动：** [启动指南](docs/v0.6-windows-local-launcher.md) · [启动脚本](scripts/start-local.ps1) · `powershell -ExecutionPolicy Bypass -File .\scripts\start-local.ps1 -SimulateWorkers`（自动生成本地密钥并启动 3 个模拟 CPU Worker）。

- [Web 控制台](web/index.html)
- [控制台打包回归测试](tests/test_web_dashboard_v051.py)
- [Qwen2 实时推理服务](cluster_v05/qwen2_service.py) · [HTTP API](cluster_v05/api.py)
- [v0.5.0 服务配置和部署说明](docs/v0.5.0-live-qwen2-queue.md)

**当前边界：** 这是管理员密钥控制台，不是多用户账号注册系统。通用 Worker 目前在可信私有网络中执行白名单 CPU 示例任务；Qwen2 PP 是本机 CPU 推理实验，尚未分布到远程个人设备。不要将默认 HTTP API 直接暴露到公网。

---


新增 FastAPI 推理入口和实时请求服务：动态请求先进入 v0.4.9 公平队列，再组成最多 4 条请求的 microbatch，交给已验证的 Qwen2 两阶段 Gloo 推理引擎。

- [v0.5.0 服务实现](cluster_v05/qwen2_service.py) · [HTTP API](cluster_v05/api.py)
- [模型中心浏览器控制台](cluster_v04/console.html)：查看队列状态并提交原始 Token ID 请求
- [动态队列到真实 Qwen2 双 Rank 的集成测试](tests/test_qwen2_live_service_v050.py)
- [服务配置、HTTP 用法与限制](docs/v0.5.0-live-qwen2-queue.md)

**实验边界：** 每个 microbatch 暂时启动新的本机双 Rank torchrun 进程组，权重会重复加载；尚未实现常驻推理进程、流式输出、远程设备连接或公网安全。CI 只有在真实 Gloo 集成测试通过后才算验收。

---

# OpenCompute AI v0.4.9 — 有界动态请求队列与公平调度原语

新增模型无关的异步请求队列：全局/客户端队列限额、客户端 Round-Robin 公平调度、客户端内 FIFO、排队截止时间、重复请求 ID 防护和安全关闭。

- [v0.4.9 请求队列实现](compute/request_queue_v049.py)
- [v0.4.9 队列公平性、超时与生命周期测试](tests/test_request_queue_v049.py)
- [v0.4.9 设计、接口示例与集成边界](docs/v0.4.9-fair-request-queue.md)

**版本边界：** v0.4.9 本身交付的是独立调度原语；其与 Qwen2 PP/FastAPI 的第一版整合见上方 v0.5.0。v0.5.0 仍是本机 CPU 实验服务，尚非公网多租户或远程算力池。

---

# OpenCompute AI v0.4.8 — 多请求 Qwen2 PP 流水线调度与 KV 预算

基于 v0.4.7 已通过测试的双阶段 Gloo PP，本版新增最多 **4 个独立请求**的交错计算与 **非阻塞 P2P**：Rank 0 可以提交请求 A 的前半层并继续计算请求 B，而 Rank 1 处理已收到的其他请求。每条请求拥有独立的 KV Cache，支持不同 Prompt 长度与 FP32/BF16 盘上权重；受请求数量、Token 上限及每 Rank 的理论 KV 内存预算限制。

- [v0.4.8 多请求 PP 计算入口](compute/qwen2_pipeline_batch_v048.py)
- [同一模型的顺序/异步执行重复基准](benchmarks/qwen2_multi_request_v048.py)
- [模型文件、安全约束与多请求隔离测试](tests/test_qwen2_pipeline_batch_v048.py)
- [独立 Hugging Face Qwen2 三 Prompt 数值验证](tests/test_qwen2_pipeline_batch_hf_v048.py)
- [v0.4.8 方案、执行示例与安全边界](docs/v0.4.8-multi-request-pipeline.md)
- [GitHub Actions CI 和 JSON 性能报告](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**实验限定：** v0.4.8 引擎本身仍是可信本机 CPU Gloo 双进程；v0.5.0 在其上新增了首版本地 HTTP 请求服务。两者都不是公网算力池或真实跨设备加速。支持多请求交错不等于已证明真实 CPU 计算同时运行；是否提升 Token/s 以重复基准测试结果为准。理论 KV 限额不是完整进程 RSS 的硬上限，未运行官方 DeepSeek 完整预训练权重。

---

# OpenCompute AI v0.4.7 — 首个真实 Qwen2 流水线并行（PP）原型

本版新增与原有 TP 张量并行**完全独立**的双 Rank Qwen2 PP 推理路径：Rank 0 运行前半 Transformer 层并持有 Embedding/LM Head，Rank 1 运行后半 Transformer 层，两端通过真实 Gloo send/recv 传递隐藏状态，KV Cache 按层分别保存在两个进程中。

- [PP 推理引擎与按层拆分、校验代码](compute/qwen2_pipeline_v047.py)
- [TP1 / TP2 / PP2 真实执行对比脚本](benchmarks/qwen2_pp_compare_v047.py)
- [合成模型文件安全与双 Rank 数值测试](tests/test_qwen2_pipeline_v047.py)
- [独立 HF Qwen2 模型与 PP 数值对照](tests/test_qwen2_pipeline_hf_v047.py)
- [v0.4.7 技术架构、使用方法与限制](docs/v0.4.7-qwen2-pipeline.md)
- [GitHub Actions：独立 HF / PP 测试与 JSON 对比报告](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**实验范围：** 可信单机 CPU Gloo 双进程，不是两个真实设备的联网测试。PP 尚未接入现有网站调度器、Worker 身份系统或正式模型目录，也没有独立跑通官方 DeepSeek 完整预训练权重。单条自回归请求需要在两个阶段之间往返传递激活，**不能保证提高 Token/s**。现有 GPT-2/Qwen2 TP 功能保持可用。

---

# OpenCompute AI v0.4.6 — 更可靠的并行性能判断

在原有 Qwen2/DeepSeek-Qwen 多模型实验架构上，本版加入 **单 Rank 无效通信快路径**、可对照旧实现的 A/B 开关、三模式重复 Gloo 计时及张量并行／流水线并行的权重内存和通信成本筛选。

- [v0.4.6 完整技术说明、复现命令与局限](docs/v0.4.6-repeated-benchmark-and-parallel-screen.md)
- [重复性能测量脚本](benchmarks/qwen2_repeat_v046.py) · [TP/PP 理论成本筛选器](compute/parallel_screen_v046.py)
- [Qwen2 推理代码（单 Rank 快路径）](compute/qwen2_generate.py) · [统计/内存筛选单元测试](tests/test_qwen2_repeat_v046.py)
- [GitHub Actions 测试及可下载的 JSON 基准报告](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**边界**：这里的 PP 仅为数学筛选，尚无实际 PP 推理引擎。重复 Gloo 性能数据来自单机随机权重，不代表公网多设备能加速或官方 DeepSeek 预训练模型已获验收。绝不会为此阶段购买云服务器或自动下载大型模型。

---

# OpenCompute AI v0.4.5 — 独立 Qwen2 数值验证与真实 Gloo 通信性能报告

本版继续保持多模型架构，新增 Qwen2 每 Rank 的 Gloo All-Reduce/广播计时、可验证的逐 Token Logits 正确性标准、Linux 单进程峰值 RSS 样本，并运行了 **1/2 Rank × FP32/BF16** 的本机合成权重基准。独立 Hugging Face Qwen2 微型随机模型也已纳入必须运行的 CI 数值对照（不下载模型权重）。

- [v0.4.5 实测数据、通信瓶颈和实验边界](docs/v0.4.5-validation-and-profile.md)
- [通信探针](compute/qwen2_trace.py) · [严格每步输出验证](compute/qwen2_validation.py) · [可运行基准和 JSON 输出](benchmarks/qwen2_matrix_v045.py)
- [离线模型来源/Tokenizer 核验入口](benchmarks/qwen2_offline_validation.py)
- [独立 HF 微型 Qwen2 vs Gloo 数值测试](tests/test_qwen2_hf_tiny_v045.py) · [来源与配置校验测试](tests/test_qwen2_provenance_v045.py)
- [GitHub Actions 运行记录与基准报告 Artifact](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**初步观察：** 在一次 GitHub CI 合成微型模型运行中，FP32 1 Rank 为 370.1 Token/s，FP32 2 Rank 为 220.8 Token/s；此场景下多 Rank 变慢，**不能推断跨设备或真实大模型一定加速**。BF16 主要减少磁盘/分发字节，CPU 运算仍为 FP32。

**重要限制：** 此版本尚未下载或实测官方 DeepSeek-R1-Distill-Qwen-1.5B 完整预训练权重，不支持陌生公网节点。模型中心仍将官方 DeepSeek 模型标记为待适配，直到有真实权重独立验收。

---

# OpenCompute AI v0.4.4 — BF16 权重与可信离线模型验证

支持 **Qwen2/DeepSeek-Qwen 架构的 BF16 Safetensors 分块存储**、可选择的 byte-bounded LRU 权重缓存，并提供 Tokenizer 与 Hugging Face 完整模型的 **离线、显式授权的数值对照**流程。GPU/MoE/公网陌生节点仍未支持。

- [v0.4.4 完整技术文档、命令和验收边界](docs/v0.4.4-bf16-offline-reference.md)
- [Qwen2 BF16 分块存储和 LRU 缓存](compute/qwen2_chunked.py)
- [离线官方模型 Tokenizer / HF 参考验证入口](benchmarks/qwen2_offline_reference.py)
- [本地权重结构、Tokenizer 严格预检查及可选 HF 数值对照](benchmarks/qwen2_offline_validation.py) · [验证边界与命令](docs/v0.4.4-hf-validation.md)
- [BF16、内存缓存与两 Rank 回归测试](tests/test_qwen2_bf16_v044.py)
- [GitHub Actions 检查](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**重要限制**：BF16 仅改变分片的**磁盘存储精度**，参与计算的权重按需转换为 FP32，不能宣称推理内存直接减半。当前尚未下载或实际运行官方完整 DeepSeek-R1-Distill-Qwen-1.5B 检查点；在模型中心仍显示「待适配」，直到真实权重的数值/性能测试通过。

---

# OpenCompute AI v0.4.3 — Qwen2 大词表分块与逐层权重加载

新增 **Qwen2 TP v2** 数据格式：Safetensors 词表行分块、逐 Rank/逐层分片、按需读取和独立 SHA-256 / 权限校验。支持可信本地模型文件按 v2 格式导出，并用现有算力池调度两个 Worker 协作推理。原 GPT-2 与 Qwen2 v1 推理代码保持兼容。

- [v0.4.3 方案、导出命令、测试与边界](docs/v0.4.3-chunked-qwen2.md)
- [核心分块实现](compute/qwen2_chunked.py) · [本地 Safetensors 导入器](compute/qwen2_safetensors.py)
- [逐 Token 数值一致性测试](tests/test_qwen2_chunked_v043.py) · [双 Worker HTTP + Gloo 分块集成测试](benchmarks/qwen2_chunked_session_demo.py)
- [GitHub Actions CI](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**注意**：本版不会下载或运行 DeepSeek-R1-Distill-Qwen-1.5B 官方完整权重。其配置可以进行文件大小规划，但分块加载的内存峰值和运行速度仍待真实模型验证。当前仍是可信本机 CPU 研究原型，不是公网算力市场。

---

# OpenCompute AI v0.4.2 — Safetensors 本地模型检查与容量规划

新增 Qwen2 / DeepSeek-Qwen **本地、安全、只读的 Safetensors 预检查器**，支持单文件和索引式分片权重、按需读取张量、参数形状和数据类型检查、可选 SHA-256 指纹验证。模型目录仍保持多架构扩展设计。

- [v0.4.2 安全导入与 DeepSeek 1.5B 容量评估](docs/v0.4.2-safetensors-ingest.md)
- [Safetensors 导入器与容量估算代码](compute/qwen2_safetensors.py)
- [导入安全与兼容性回归测试](tests/test_qwen2_safetensors_v042.py)
- [GitHub Actions 验证状态](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**限制**：目前仅限于符合 512 MB 单文件上限的小型 Qwen2 检查点。官方 DeepSeek-R1-Distill-Qwen-1.5B 的 Rank 0 FP32 IO 文件预计约 1.87 GB，尚不能直接完成分片部署；没有下载或实际运行该完整官方权重。

---

# OpenCompute AI v0.4.1 — Qwen2/DeepSeek-Qwen 架构适配第一阶段

**真正新增了 Qwen2 CPU Gloo 张量并行计算引擎**：实现 RoPE、RMSNorm、GQA、SwiGLU、Rank 本地 KV Cache、分片权重导出和双 Worker 协同生成。已打通原有注册 Worker、调度器与多模型选择 API。

- [Qwen2 引擎与 DeepSeek 兼容性边界](docs/v0.4.1-qwen2-tp.md)
- [Qwen2 分片导出模块](compute/qwen2_shards.py) · [Gloo 生成模块](compute/qwen2_generate.py) · [数值一致性测试](tests/test_qwen2_v041.py)
- [本机真实 HTTP + 两 Worker Qwen2 集成脚本](benchmarks/qwen2_tp_session_demo.py)
- 模型中心：`http://127.0.0.1:8000/v0.4/console`（本地服务启动后）
- [GitHub Actions](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**注意**：已经支持 Qwen2 **合成测试权重**和符合本地限额的小型可信检查点，但**尚未将 DeepSeek-R1-Distill-Qwen-1.5B 官方完整权重实际运行和验收**；该模型仍在模型中心显示「待适配」。原有 GPT-2 引擎保持可用。

---

# OpenCompute AI v0.4 — 多模型算力平台（模型中心第一阶段）

OpenCompute AI 不绑定某一家模型厂商。已建立统一模型目录、按架构声明的适配器能力和模型选择页面：**DeepSeek、Qwen、Llama、GPT-2 等模型可以统一管理，但目前只有原 GPT-2 实验引擎真正可运行**。未适配模型不会被误标为可执行。

- **[模型选择网页](cluster_v04/console.html)**：启动后访问 `http://127.0.0.1:8000/v0.4/console`
- **[多模型架构、真实支持状态和开发顺序](docs/v0.4-multi-model-architecture.md)**
- **[模型目录及后端能力表](cluster_v04/models.py)**
- **[模型分发接口及单元测试](tests/test_model_registry_v04.py)**
- **[GitHub Actions 测试](https://github.com/tangxiang257-droid/opencompute-ai/actions)**

此阶段不自动下载模型，也没有实现 DeepSeek/Qwen/Llama 分布式权重执行，仍以可信本机研究环境为边界。

---

# OpenCompute AI — v0.3.5 多 Rank 安全预检查

新增**分布式启动前错误同步**：如果 Rank 0 的 Prompt 解析失败、Token 越界，或任一 Rank 的检查点加载异常，所有仍存活的 Rank 都将在开始正式推理前通过 Gloo 预检查一致退出。避免已知异常导致其他 Rank 卡在下一次 Broadcast。

- [v0.3.5 模型文件校验、重试溯源及多 Rank 预检查](docs/v0.3.5-checkpoint-security.md)
- [v0.3.5 单机双 Rank 异常退出测试](tests/test_preflight_v035.py)
- [GitHub Actions 运行状态](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**边界**：仅为可信本机多进程实验；无法阻止进程被系统杀死、机器断线引发的通信超时，也不等同于跨设备故障恢复。

---

# OpenCompute AI — v0.3.5 持久化分片校验与安全重建

本轮为每次模型推理会话在 SQLite 中持久化分片文件的 SHA-256 与大小，并记录失败重试的来源 ID。发生文件篡改或历史校验记录缺失时拒绝重试，避免无声替换权重；同时收紧 Worker 的子进程环境变量及可选 Linux 容器运行权限。

- [v0.3.5 完整安全审查及兼容说明](docs/v0.3.5-checkpoint-security.md)
- [检查点篡改、重试、迁移和 Worker 权限回归测试](tests/test_checkpoint_security_v035.py)
- [GitHub Actions](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**版本边界：** 只适合可信本机研究原型，尚无公网节点准入、模型来源可信证明、真正 KV Cache 断点续算。新提交的 CI 结果必须单独验收；此前 v0.3.4 CI 通过不代表这次代码已经通过。

---

# OpenCompute AI — v0.3.4 Worker 资源保护与本机容器隔离

本次将 TP 推理子进程置于更明确的资源限制下：POSIX 内核 CPU 时间配额、父进程定期检查实际进程树 RSS 并在超额时取消推理。虚拟地址空间硬限制为**可选**，默认不启用以避免误伤 PyTorch 的正常内存映射。

- [v0.3.4 资源限制、隔离能力与剩余风险](docs/v0.3.4-worker-limits.md)
- [v0.3.4 资源保护回归测试](tests/test_resource_limits_v034.py)
- [可选 Linux 本机 Docker TP Worker 配置](docker-compose.tp-local.yml)：两个真实 TP Worker、独立缓存、容器 CPU/内存/PID 配额；**并未运行跨物理设备实验**。
- [自动化 CI 状态](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**注意：** 周期性 RSS 检查不是内核级物理内存配额。真正的内存硬限制需要 Linux cgroups / Docker 等隔离机制。本项目仍仅适用于可信本机开发；最新提交的 CI 状态需独立确认。

---

# OpenCompute AI — v0.3.3 本机 Gloo 端口绑定与会话重建

**本轮修复：** 以协调器持有的 `TCPStore` 替代“分配并释放临时端口”的竞态设计；注册 Worker 连接同一个已经绑定的 Store，原 `torchrun` 实验保持兼容。失败会话现在可由管理员手动创建**新的推理组**，从原始提示词与分片重新执行。

- [v0.3.3 实现细节、重试 API 与剩余风险](docs/v0.3.3-rendezvous-recovery.md)
- [新增的 Mock TCPStore / 重建 / 重启回归测试](tests/test_rendezvous_v033.py)
- [GitHub Actions](https://github.com/tangxiang257-droid/opencompute-ai/actions)

**严禁误解为无缝断线续算：** KV Cache 不会恢复，分布式进程组不能在运行中替换 Rank。本版本仍限制**可信本机、单 API 进程、最多一个 TP 会话**；尚未验收公网或跨物理设备运行。

---

# OpenCompute AI — v0.3.2 安全与可靠性修复

**重要更新：** 加强本机 CPU 张量并行实验环境的管理接口鉴权、跨任务资源预留、会话超时检查、管理员取消、Worker 子进程终止、模型分片缓存清理及结果格式验证。

- **[v0.3.2 修复清单、使用方法与仍未解决的问题](docs/v0.3.2-reliability.md)**
- **[回归测试与 CI 工作流](.github/workflows/cluster-v03.yml)**

**边界未改变：** 这仍是可信本机研究原型，不支持公网陌生节点。Gloo rendezvous 仍使用 `127.0.0.1`，端口占用竞态以及真正的跨设备故障恢复尚待解决。

---

# OpenCompute AI — v0.3.1 已注册 Worker 协同推理（本机实验）

**新进展：** 将 v0.3 节点注册和资源感知调度接入 v0.2 GPT-2 CPU 张量并行。已实现 rank 指派、鉴权下载分片、SHA-256 校验、就绪屏障及两个独立 Worker 进程上的实际 Gloo 推理。可选择合成测试模型或预先导出的可信 GPT-2 TP 检查点。

- **[v0.3.1 部署及使用指南](docs/v0.3.1-managed-tp.md)**
- **[本机 HTTP + 两个 Worker 端到端实验](benchmarks/tp_session_demo.py)**：`python -m benchmarks.tp_session_demo`
- **[GitHub Actions 测试](.github/workflows/cluster-v03.yml)**

当前 Gloo rendezvous 固定为 `127.0.0.1`；不支持真实跨主机连接、公网陌生设备或可投入生产的大模型算力池。Docker Compose 的原有 `worker` 服务仍属于 v0.3 普通任务 Worker，**不是** v0.3.1 的 PyTorch TP Worker。v0.3.1 请按照文档在同一主机启动两份 `cluster_v031.worker`。

---

# OpenCompute AI — v0.3 异构设备算力池（本地原型）

**Current milestone:** An authenticated CPU Worker registry, heartbeat and resource reporting, capacity/performance-aware task claims, expiring leases, SQLite persistence, and a localhost browser console. The existing v0.2 GPT-2 tensor-parallel research prototype is preserved, but **its model shards are not yet dynamically scheduled onto registered v0.3 devices**.

- **[v0.3 本地部署、模拟节点、测试和安全说明](docs/v0.3-heterogeneous-cluster.md)**
- **[v0.3 私有集群控制台](cluster_v03/console.html)** (served at `http://127.0.0.1:8000/v0.3/console` after starting the API)
- **[v0.2 分布式 GPT-2 实验](docs/v0.2-distributed-gpt2.md)**
- **[GitHub CI](https://github.com/tangxiang257-droid/opencompute-ai/actions)**

Start a private local cluster after generating two independent secrets in `.env`:

```bash
docker compose --profile sim up -d --build --scale worker=3
```

The API binds localhost only; workers run over the internal Docker network. **Never expose this early prototype to untrusted internet nodes.**

---

# Historical v0.2 CPU tensor parallel research milestones

**Current milestone:** complete multi-layer GPT-2-style greedy decoding with rank-local weights, KV caches, and inter-process All-Reduce. An optional offline exporter supports trusted Hugging Face GPT-2 checkpoints; cloud servers are not needed for development.

- **[v0.2 quick start, pretrained checkpoint import and limitations](docs/v0.2-distributed-gpt2.md)**
- **[CPU CI runs](https://github.com/tangxiang257-droid/opencompute-ai/actions)**

The v0.1 experiments and earlier limitations below are **historical descriptions** of incremental prototypes, not the current full feature list.

---

# OpenCompute AI v0.1 — CPU tensor parallel prototype

The repository also contains an earlier task-scheduler prototype in `app/`. The default Docker deployment now launches the tensor-parallel benchmark API, not the old task queue.

## Cloud VM deployment

Ubuntu with Docker Engine and Compose plugin; minimum 2 vCPU / 4 GiB RAM, recommended 4 vCPU / 8 GiB RAM and 20 GiB disk.

```bash
docker compose up -d --build
curl http://127.0.0.1:8000/health
curl -X POST http://127.0.0.1:8000/benchmarks -H "X-Admin-Key: $OPENCOMPUTE_ADMIN_KEY" -H 'Content-Type: application/json' -d '{"workers":2,"hidden":512,"batch":16}'
```

Only localhost port 8000 is exposed. Access remotely through an SSH tunnel: `ssh -L 8000:127.0.0.1:8000 ubuntu@SERVER_IP`.

## Limitations

The CPU workers perform real row-sharded matrix multiplication with Gloo All-Reduce and compare with a single-process reference. For verification each rank temporarily regenerates the full reference matrix; therefore **this does not yet demonstrate pooled memory capacity**. The prototype is single-host, with no model token generation, user authentication, public dashboard, or hardened job isolation. Do not expose the API publicly.

Next: rank-local weight files, memory measurements, multi-host Gloo, Transformer TP and authenticated web console.

## Disk-sharded CPU verification (v0.1.1)

This path avoids constructing a full weight matrix in any Worker. The shard generator writes deterministic row blocks to disk, each rank loads only its own block, and rank zero checks the output with a streaming reference. It is a synthetic linear-layer proof, **not** an LLM or virtual unified memory implementation.

```bash
python -m compute.shards --output shards --hidden 512 --workers 2
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.sharded_demo --shards shards
python -m unittest discover -s tests -v
python -m benchmarks.run --hidden 512 --batch 16 --iterations 10
```

The benchmark compares 1, 2 and 4 workers and reports per-step maximum elapsed time, computation/collective upper-bound components, numerical error and relative speedup. These are single-host CPU measurements; network benchmarks and measured memory RSS are not implemented yet. GitHub Actions runs small correctness tests and a smoke benchmark on pushes and pull requests.

For Docker, use `docker compose up -d --build`, then POST to `http://127.0.0.1:8000/benchmarks` from the server itself. Keep port 8000 bound to localhost.

## Transformer-style MLP tensor parallel prototype

This experiment shards the first linear projection by columns and the second by rows, applies GELU locally, and All-Reduces the partial output. Each worker loads only its two local matrices from disk.

```bash
python -c "from compute.mlp import prepare_mlp; prepare_mlp('mlp_shards', 256, 512, 2)"
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.mlp --shards mlp_shards
python -m benchmarks.mlp_latency --workers 2 --hidden 256 --expansion 512
```

The existing matrix benchmark now reports Linux per-process peak RSS in KiB, separately from the exact local weight bytes. The MLP benchmark reports local/full weight bytes and elapsed times.

**Interpretation:** `--latency-ms` inserts an artificial delay before the collective. It is a sensitivity test, not real network emulation; no bandwidth throttling, multi-host measurement, GPU VRAM, attention/KV cache, or full Transformer model is included yet. CPU performance numbers vary with machine load and are not comparable across unrelated hosts.

## v0.2: head-sharded causal attention and synthetic Transformer block

Attention weights Q/K/V are column-sharded by heads and the output projection is row-sharded. Each CPU process loads only its rank's weight files and stores its own K/V heads. Prefill uses a causal mask; incremental decode appends to the rank-local KV cache. The output projection is summed with Gloo All-Reduce.

```bash
python -c "from compute.attention import prepare_attention; prepare_attention('attention_shards',32,4,2)"
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.attention --shards attention_shards --prompt-tokens 4 --decode-steps 3
OMP_NUM_THREADS=1 torchrun --standalone --nproc-per-node=2 -m compute.block --hidden 32 --heads 4 --expansion 64 --tokens 4
python -m unittest discover -s tests -v
```

`compute.block` implements a pre-norm residual attention + GELU MLP block with two All-Reduce operations per block. Both demos compare results against streaming single-process references. The block demo regenerates deterministic shard files separately in each process for convenient single-host testing, so it does not measure storage or network efficiency.

**Not yet included:** learned tokenizer/embeddings, trained checkpoint import, token sampling, full autoregressive Transformer-block KV cache, true multi-host orchestration, internet-safe worker communication, network bandwidth shaping or production GPU acceleration. Attention KV cache is demonstrated separately in `compute.attention`.


## v0.2 follow-up: network and official GPT-2 compatibility

- [Local Gloo timing and simulated WAN sensitivity](docs/network-lab.md).
- [Hugging Face reference compatibility workflow](.github/workflows/gpt2-compat.yml): automatic offline test comparing our distributed output against a random-weight official GPT-2 implementation, and manually triggered real `sshleifer/tiny-gpt2` checkpoint parity.
- [Real checkpoint validation script](benchmarks/validate_pretrained.py): `python -m benchmarks.validate_pretrained --model sshleifer/tiny-gpt2 --workers 2 --new-tokens 2`.

**Scope:** single-host experiments are validated locally; HF integration requires optional dependencies and the pretrained model run requires a download. Added bandwidth/latency waits are an explicit sensitivity model, not actual WAN shaping.


## v0.2.1: reduce synchronization and plan Worker groups

- [Communication-elision, repeatable A/B test and capacity-first TP planner](docs/topology-optimization.md)
- One redundant next-token Broadcast per generated token is removed. Per-block All-Reduce is unchanged.
- `python -m compute.topology_planner --devices 4 --memory-mib 1024 --latency-ms 5 --bandwidth-mbps 100` previews rank-local memory and estimated network overhead.
- `POST /v0.2/plan` is a localhost-only dry-run endpoint, not a live scheduler.
- Planner outputs are **estimates, not measured memory feasibility or throughput guarantees**.

- `python -m benchmarks.autotune --devices 4 --hidden 32 --heads 4 --layers 2 --repeats 3` measures feasible CPU TP configurations and reports the fastest **on the test host**, not an internet-wide optimum.
