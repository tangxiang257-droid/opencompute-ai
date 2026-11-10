"""Analytical TP vs pipeline partition screening, NOT a PP implementation.

Communications are illustrative bytes/latency proxies under simple models.
Nothing here estimates end-to-end inference latency, throughput, measured WAN
speed or the trust of independent devices. Always benchmark real deployments.
"""
import math


def _positive_number(value, name, *, zero_allowed=False):
    if (type(value) not in (int, float) or not math.isfinite(value)
            or value < (0 if zero_allowed else 1e-12)):
        raise ValueError(f"{name} must be finite and {'nonnegative' if zero_allowed else 'positive'}")
    return float(value)


def screening(*, hidden, heads, kv_heads, layers, workers,
              transformer_weight_bytes, io_weight_bytes, memory_per_node_bytes,
              latency_ms, bandwidth_mbps, prompt_tokens=16, new_tokens=8,
              concurrent_requests=1):
    """Return independent feasibility and communication proxies.

    Assumptions: one rank per node, uniform compute/storage, float32
    activations, equal-size layer partitions, one greedy request, and a
    hypothetical *ring* All-Reduce. Real Gloo implementation differs.

    For PP: first stage holds embeddings, last holds LM head; each is
    approximated as half of total IO weight bytes. No PP executor exists.
    """
    for name, v in (("hidden", hidden), ("heads", heads),
                    ("kv_heads", kv_heads), ("layers", layers),
                    ("workers", workers), ("prompt_tokens", prompt_tokens),
                    ("new_tokens", new_tokens),
                    ("concurrent_requests", concurrent_requests)):
        if type(v) is not int or not 1 <= v <= (262144 if name == "hidden" else
                                              4096 if name == "layers" else
                                              65536):
            raise ValueError(f"unsupported integer dimension: {name}")
    if not (hidden % heads == 0 and heads % kv_heads == 0):
        raise ValueError("inconsistent GQA head topology")
    transformer = _positive_number(transformer_weight_bytes, "transformer_weight_bytes")
    io = _positive_number(io_weight_bytes, "io_weight_bytes", zero_allowed=True)
    memory = _positive_number(memory_per_node_bytes, "memory_per_node_bytes")
    latency = _positive_number(latency_ms, "latency_ms", zero_allowed=True)
    bandwidth = _positive_number(bandwidth_mbps, "bandwidth_mbps")
    if workers > 32:
        raise ValueError("analytical screening limited to 32 stages/ranks")
    if prompt_tokens + new_tokens > 131072:
        raise ValueError("context outside screening envelope")
    # This is a *weights-only* lower bound. More memory is needed for
    # activations, KV, Python runtime, filesystem cache and load spikes.
    tp_allowed = heads % workers == 0 and kv_heads % workers == 0
    tp_weight_most_loaded = transformer / workers + io
    # PP layers are indivisible and stage 0 / last own different IO halves.
    pp_allowed = workers <= layers
    pp_layers_per_stage = math.ceil(layers / workers)
    pp_heaviest = transformer * pp_layers_per_stage / layers + (
        io if workers == 1 else io / 2)
    if workers == 1:
        pp_heaviest = transformer + io
    # Assuming ring collective and an unicast activation broadcast, only
    # used to highlight why TP is latency sensitive, not actual wire usage.
    forwards = new_tokens
    token_positions = prompt_tokens + max(0, new_tokens - 1)
    logical_activation_bytes = 4.0 * hidden * token_positions
    tp_collectives = (2 * layers + 1) * forwards if workers > 1 else 0
    tp_ring_bytes_per_rank = (
        2 * layers * 2 * (workers - 1) / workers * logical_activation_bytes
        + logical_activation_bytes if workers > 1 else 0)
    # A ring all-reduce's latency has 2*(N-1) steps; the extra broadcast is
    # modeled as (N-1) hops, with these assumptions clearly surfaced.
    tp_latency_proxy_s = (
        (2 * layers * forwards * 2 * (workers - 1)
         + forwards * (workers - 1)) * latency / 1000
        + tp_ring_bytes_per_rank * 8 / (bandwidth * 1e6)
        if workers > 1 else 0.)
    pp_transfers = (workers - 1) * forwards if workers > 1 else 0
    pp_bytes = logical_activation_bytes * (workers - 1)
    pp_latency_proxy_s = (
        pp_transfers * latency / 1000
        + pp_bytes * 8 / (bandwidth * 1e6))
    # Distinguish feasibility from theoretical promise. No compute profiler
    # is used here; network proxies must not become execution predictions.
    return {
        "schema": "opencompute-parallel-screening-v1",
        "input": {
            "hidden": hidden, "heads": heads, "kv_heads": kv_heads,
            "layers": layers, "workers": workers, "prompt_tokens": prompt_tokens,
            "new_tokens": new_tokens,
            "concurrent_requests": concurrent_requests,
            "latency_ms": latency, "bandwidth_mbps": bandwidth,
            "memory_per_node_bytes": int(memory),
        },
        "tensor_parallel": {
            "implemented": True,
            "topology_supported": tp_allowed,
            "weights_only_most_loaded_bytes": int(math.ceil(tp_weight_most_loaded)),
            "passes_weight_only_memory_screen": (
                tp_allowed and tp_weight_most_loaded <= memory),
            "estimated_collectives_per_rank": tp_collectives if tp_allowed else None,
            "ring_proxy_bytes_per_rank": round(tp_ring_bytes_per_rank) if tp_allowed else None,
            "ring_proxy_communication_seconds": round(tp_latency_proxy_s, 6) if tp_allowed else None,
            "warning": "Most loaded rank keeps full vocabulary IO; ring is not measured Gloo.",
        },
        "pipeline_parallel": {
            "implemented": False,
            "topology_supported": pp_allowed,
            "weights_only_most_loaded_bytes": int(math.ceil(pp_heaviest)),
            "passes_weight_only_memory_screen": (
                pp_allowed and pp_heaviest <= memory),
            "stage_boundary_transfers": pp_transfers if pp_allowed else None,
            "unicast_proxy_bytes": round(pp_bytes) if pp_allowed else None,
            "unicast_proxy_communication_seconds": round(pp_latency_proxy_s, 6) if pp_allowed else None,
            "warning": ("Single autoregressive request traverses all stages serially; "
                        "higher throughput may require several concurrent requests."),
        },
        "conclusion": "screening_only_no_winner_without_measured_compute_time",
        "disclaimer": ("Communication proxy is an illustrative ring/unicast model, "
                       "not network measurements or predicted end-to-end latency. "
                       "Weights-only memory excludes runtime/KV/activation peaks."),
    }
