"""Pure stock-Qwen2 configuration normalization shared by existing loaders.

No framework, weights, filesystem, tokenizer or runtime imports. Supported
semantics are full attention, default RoPE and ordinary nonquantized Qwen2.
Unknown metadata keys are retained only in the caller's input, not interpreted.
"""
import math

FORMAT = "opencompute-qwen2-tp-v1"


def manifest_from_config(config, workers, source="local"):
    if type(config) is not dict:
        raise ValueError("model config must be a plain object")
    required = ('hidden_size','num_attention_heads','num_hidden_layers',
                'intermediate_size','vocab_size','max_position_embeddings')
    if any(k not in config for k in required):
        raise ValueError("missing required Qwen2 dimensions")
    for key in required + ('num_key_value_heads','head_dim'):
        if key in config and (type(config[key]) is not int or config[key] <= 0):
            raise ValueError("Qwen2 dimensions must be positive integers")
    if type(workers) is not int or not 1 <= workers <= 4:
        raise ValueError("invalid TP rank count")
    for key in ('use_sliding_window','tie_word_embeddings'):
        if key in config and type(config[key]) is not bool:
            raise ValueError("Qwen2 switches must be booleans")
    for key in ('rms_norm_eps','rope_theta'):
        if key in config and type(config[key]) not in (int,float):
            raise ValueError("Qwen2 numerical parameter must be a number")
    if 'layer_types' in config:
        types = config['layer_types']
        if (type(types) is not list or len(types) != config['num_hidden_layers']
                or any(type(t) is not str or t != 'full_attention' for t in types)):
            raise ValueError("only full attention at every layer is supported")
    if config.get("model_type", "qwen2") != "qwen2":
        raise ValueError("only the stock Qwen2 architecture is supported")
    if config.get("quantization_config"):
        raise ValueError("quantized model export is not supported")
    if config.get("rope_scaling") not in (None, {"rope_type": "default"}, {"type": "default"}):
        raise ValueError("non-default RoPE scaling is not implemented")
    if config.get("use_sliding_window", False):
        raise ValueError("sliding-window attention requires a separate engine")
    if config.get("hidden_act", "silu") != "silu":
        raise ValueError("Qwen2 exporter supports SwiGLU/silu only")
    hidden = int(config["hidden_size"])
    heads = int(config["num_attention_heads"])
    kv_heads = int(config.get("num_key_value_heads", heads))
    layers = int(config["num_hidden_layers"])
    intermediate = int(config["intermediate_size"])
    vocab = int(config["vocab_size"])
    positions = int(config["max_position_embeddings"])
    head_dim = config.get("head_dim", hidden // heads)
    try:
        epsilon = float(config.get("rms_norm_eps", 1e-6))
        theta = float(config.get("rope_theta", 10000.0))
    except OverflowError:
        raise ValueError("Qwen2 numerical parameter exceeds finite float range") from None
    if (not 1 <= workers <= 4 or not 1 <= hidden <= 4096
            or not 1 <= heads <= 128 or not 1 <= kv_heads <= heads
            or not 1 <= layers <= 48 or not 1 <= intermediate <= 32768
            or not 1 <= vocab <= 262144 or not 1 <= positions <= 131072
            or hidden % heads or heads % kv_heads or heads % workers
            or kv_heads % workers or intermediate % workers
            or head_dim != hidden // heads or head_dim % 2
            or not math.isfinite(epsilon) or not 0 < epsilon <= .1
            or not math.isfinite(theta) or not theta > 1):
        raise ValueError("unsupported Qwen2 dimensions or TP rank count")
    if config.get("tie_word_embeddings", False) not in (False, True):
        raise ValueError("invalid tied embeddings option")
    return {
        "format": FORMAT, "source": str(source)[:512],
        "hidden": hidden, "heads": heads, "kv_heads": kv_heads,
        "head_dim": head_dim, "layers": layers, "inner": intermediate,
        "vocab": vocab, "positions": positions, "workers": workers,
        "rms_eps": epsilon, "rope_theta": theta,
        "tied_embeddings": bool(config.get("tie_word_embeddings", False)),
    }

