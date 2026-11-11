# 公网设备自动接入

在 OpenCompute AI 的「设备接入与 API」页面下载 `opencompute-worker.py`，在下载目录运行网页生成的命令。选择 CPU 时只需 Python 3.9+。选择 NVIDIA GPU 时还需可用的 CUDA 版 PyTorch。

服务器验证一次性限时计算题的结果，并检查 CPU、内存和 GPU 显存门槛。达标设备自动获得本节点专用的短期会话凭证。保持终端开启，Worker 每 20 秒执行一道新校验题并续报。设备约 90 秒未通过续报后会显示离线。

公网设备运行固定的算力校验任务。硬件规格由设备上报，矩阵计算结果可验证。可信 Worker 继续使用独立的注册和任务队列处理模型提示词。

## 部署和检查

生产网站通过现有 HTTPS 入口提供接入，Compose 仍将 API 绑定到本机。服务器在独立的公网节点数据库内生成并保存校验题签名密钥。用户无需输入管理员密钥或共享加入密钥。Compose 默认启用公网接入。可在服务器 `.env` 设置 `OPENCOMPUTE_PUBLIC_ADMISSION=0` 关闭接入。

在现有阿里云部署目录执行更新，保留原 `.env` 和数据卷。

```bash
cd /opt/opencompute-ai
git pull --ff-only
docker compose up -d --build
curl -fsS http://127.0.0.1:8000/health
curl -fsS http://127.0.0.1:8000/v0.8/public/status
```

确认 `/health` 返回版本 `0.8.0`。状态接口应返回 `"enabled":true` 和在线计数。从公网 HTTPS 地址打开网页并启动一个 CPU Worker 后，状态应显示节点在线。

接入额度按服务端收到的客户端地址计算。如果多个来访者都被识别为同一个反向代理，请确认代理正确设置转发头，并在 `.env` 用 `OPENCOMPUTE_TRUSTED_PROXY_IPS` 指定受信的实际代理地址。默认只信任本机回环地址。[Uvicorn 代理配置](https://uvicorn.dev/deployment/#proxies-and-forwarded-headers)

## 接入协议

- `POST /v0.8/public/challenge` 返回有效期 60 秒的一次性校验题。
- `POST /v0.8/public/join` 验证签名、资源规格与计算结果，并颁发节点凭证。
- `POST /v0.8/public/heartbeat` 使用节点专用 Bearer 凭证验证新题。一次续报成功后的原请求可以安全重试。
- `GET /v0.8/public/status` 公开汇总数据。

每个部署最多保留 64 个在线公网节点，同一请求来源最多 12 个。接入端点限制请求体大小、全站处理频率和同一请求来源的处理频率。续报额度仅对已认证的公网节点计数。公网会话凭证存放在 Worker 进程内存，服务端保存哈希。
