# 在 GPU 任务队列运行 Qwen2

在控制台打开「任务队列」，将 GPU 任务类型切换到「Qwen2 文本生成」。选择已上线的模型，输入提示词和生成 Token 数，然后提交。任务完成后展开「查看生成文本」。输入按原始提示词续写，支持最多 4,096 个字符及 512 个输入 Token，每次最多生成 64 个 Token。

## 配置可信 Worker

Worker 使用现有设备接入流程。配置 CUDA 版 PyTorch 和项目依赖后，额外安装本地分词器依赖。模型目录需包含 `config.json`、`tokenizer.json` 和单文件 `model.safetensors`。从可信发布源取得权重 SHA-256，再启动 Worker。

```powershell
python -m pip install -r requirements.txt "tokenizers>=0.22,<0.23"
python -c "import torch; print(torch.cuda.is_available())"
$expectedSha256 = Read-Host '输入可信发布源给出的权重 SHA-256'
python -m cluster_v03.worker --name qwen2-gpu --qwen2-dir ./models/qwen2 --qwen2-sha256 $expectedSha256
```

也可设置 `OPENCOMPUTE_QWEN2_DIR` 和 `OPENCOMPUTE_QWEN2_SHA256`。Worker 在启动时验证权重和分词器。模型标识由权重、配置和分词器共同决定，拥有相同文件的多个 Worker 会接收同一模型的任务。服务端根据 Worker 上报的保守估算预留显存。模型文件和目录路径留在设备上。

## 运行和验证

任务由管理员提交，提示词会保存在平台任务记录中，并发送给执行它的 Worker。请在可信节点上运行。Worker 在独立进程内执行任务，每 5 秒发送心跳并检查租约。取消后结果会丢弃，显存预留在 Worker 停止或租约到期后释放。

```powershell
python -m unittest discover -s tests -p "test_gpu_qwen2*.py" -v
```
