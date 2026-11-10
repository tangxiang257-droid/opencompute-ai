"""Extensible model directory and honest execution-capability declarations.

The catalogue is metadata, NOT a claim that an architecture has an inference
backend. Only explicitly registered runners are dispatchable. Never load
arbitrary Python modules or model URLs supplied by a caller.
"""
from dataclasses import dataclass, asdict


@dataclass(frozen=True)
class ModelSpec:
    id: str
    display_name: str
    family: str
    architecture: str
    engine: str | None
    provider: str
    official_source: str | None
    capability: str
    modes: tuple[str, ...]
    description: str
    requires_local_checkpoint: bool = False

    def public(self) -> dict:
        record = asdict(self)
        record["modes"] = list(self.modes)
        record["launchable"] = self.capability == "experimental"
        return record


# Keep human-friendly brands separate from underlying architecture and engine.
# A DeepSeek distillation on Qwen2 != full DeepSeek MoE != GPT-2.
MODELS = (
    ModelSpec(
        id="gpt2-synthetic", display_name="GPT-2 结构测试模型",
        family="GPT-2", architecture="gpt2", engine="gpt2-tp",
        provider="OpenCompute AI", official_source=None,
        capability="experimental", modes=("tensor_parallel",),
        description="随机合成权重，只用于验证多节点推理流程，不具备聊天能力。",
    ),
    ModelSpec(
        id="gpt2-local-checkpoint", display_name="GPT-2 本地可信检查点",
        family="GPT-2", architecture="gpt2", engine="gpt2-tp",
        provider="Local administrator", official_source=None,
        capability="experimental", modes=("tensor_parallel",),
        description="使用本地预先导出的、经过管理员信任确认的 GPT-2 分片。",
        requires_local_checkpoint=True,
    ),
    ModelSpec(
        id="qwen2-synthetic", display_name="Qwen2 GQA 合成测试模型",
        family="Qwen", architecture="qwen2", engine="qwen2-tp",
        provider="OpenCompute AI", official_source=None,
        capability="experimental", modes=("tensor_parallel",),
        description="已实现 Qwen2 的 RoPE/GQA/RMSNorm/SwiGLU CPU 张量并行；目前使用随机合成权重，不具备真实聊天能力。",
    ),
    ModelSpec(
        id="deepseek-r1-distill-qwen-1.5b",
        display_name="DeepSeek-R1-Distill-Qwen-1.5B",
        family="DeepSeek", architecture="qwen2",
        engine=None, provider="deepseek-ai",
        official_source="https://huggingface.co/deepseek-ai/DeepSeek-R1-Distill-Qwen-1.5B",
        capability="planned", modes=("tensor_parallel", "single_device"),
        description="计划接入 Qwen2 系列的 RoPE、RMSNorm、GQA/SwiGLU 推理适配器。",
    ),
    ModelSpec(
        id="qwen2.5-0.5b-instruct",
        display_name="Qwen2.5-0.5B-Instruct",
        family="Qwen", architecture="qwen2",
        engine=None, provider="Qwen",
        official_source="https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct",
        capability="planned", modes=("tensor_parallel", "single_device"),
        description="与部分 DeepSeek-Qwen 蒸馏模型共用 Qwen2 结构适配器。",
    ),
    ModelSpec(
        id="deepseek-r1-distill-llama-8b",
        display_name="DeepSeek-R1-Distill-Llama-8B",
        family="DeepSeek", architecture="llama",
        engine=None, provider="deepseek-ai",
        official_source="https://huggingface.co/deepseek-ai/DeepSeek-R1-Distill-Llama-8B",
        capability="planned", modes=("tensor_parallel", "single_device"),
        description="需要独立 Llama 架构及模型许可适配，不能直接复用 Qwen 权重加载器。",
    ),
    ModelSpec(
        id="deepseek-r1-moe",
        display_name="DeepSeek-R1 完整 MoE",
        family="DeepSeek", architecture="deepseek_v3_moe",
        engine=None, provider="deepseek-ai",
        official_source="https://huggingface.co/deepseek-ai/DeepSeek-R1",
        capability="research", modes=("expert_parallel", "pipeline_parallel"),
        description="超大规模 MoE 专项研究目标；未实现专家并行或跨设备部署。",
    ),
    ModelSpec(
        id="custom-api-model",
        display_name="自定义兼容 API 模型",
        family="External API", architecture="external_api",
        engine=None, provider="User configured",
        official_source=None, capability="planned", modes=("remote_api",),
        description="未来的第三方模型接口；不使用个人设备算力池，须单独进行密钥管理。",
    ),
)

_BY_ID = {m.id: m for m in MODELS}
if len(_BY_ID) != len(MODELS):
    raise RuntimeError("duplicate model IDs in registry")


class ModelUnavailable(Exception):
    """The catalogued model does not have an implemented compute adapter."""


def model_by_id(model_id: str) -> ModelSpec:
    if not isinstance(model_id, str) or model_id not in _BY_ID:
        raise KeyError("unknown model ID")
    return _BY_ID[model_id]


def list_models(*, family: str | None = None) -> list[dict]:
    return [m.public() for m in MODELS
            if family is None or m.family.lower() == family.lower()]


def require_runtime(model_id: str, mode: str) -> ModelSpec:
    model = model_by_id(model_id)
    if mode not in model.modes:
        raise ValueError(f"mode {mode!r} is unsupported for {model.id}")
    if model.capability != "experimental" or model.engine is None:
        raise ModelUnavailable(
            f"{model.id} is catalogued but has no implemented inference adapter")
    if model.engine not in ("gpt2-tp", "qwen2-tp") or mode != "tensor_parallel":
        raise ModelUnavailable("inference adapter not installed")
    return model
