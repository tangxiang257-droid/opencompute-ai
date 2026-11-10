"""Optional per-rank Gloo communication profiler for trusted CPU inference.

Measures wall time *inside* tensor collectives, including synchronization/wait.
Payload bytes count each local tensor passed to a collective, NOT total bytes
placed on the network (ring/tree implementation and links affect the latter).
"""
import time

import torch.distributed as dist


class CollectiveProfiler:
    def __init__(self):
        self.calls_all_reduce = 0
        self.calls_broadcast = 0
        self.payload_all_reduce_bytes = 0
        self.payload_broadcast_bytes = 0
        self.collective_ns = 0

    @staticmethod
    def _tensor_bytes(value):
        return value.numel() * value.element_size()

    def broadcast(self, tensor, *, src):
        started = time.perf_counter_ns()
        try:
            return dist.broadcast(tensor, src=src)
        finally:
            self.collective_ns += time.perf_counter_ns() - started
            self.calls_broadcast += 1
            self.payload_broadcast_bytes += self._tensor_bytes(tensor)

    def all_reduce(self, tensor, *, op=dist.ReduceOp.SUM):
        started = time.perf_counter_ns()
        try:
            return dist.all_reduce(tensor, op=op)
        finally:
            self.collective_ns += time.perf_counter_ns() - started
            self.calls_all_reduce += 1
            self.payload_all_reduce_bytes += self._tensor_bytes(tensor)

    def snapshot(self):
        return {
            "all_reduce_calls": self.calls_all_reduce,
            "broadcast_calls": self.calls_broadcast,
            "all_reduce_tensor_bytes": self.payload_all_reduce_bytes,
            "broadcast_tensor_bytes": self.payload_broadcast_bytes,
            "collective_wall_us": self.collective_ns // 1000,
        }


def profile_rank_stats(*, preflight_us, decode_us, prefill_us, tokens,
                       profiler, rss_sample_bytes=0, peak_rss_bytes_linux=0):
    """Fixed shape and stable units for Gloo gathering across all ranks."""
    fields = (
        preflight_us, decode_us, prefill_us, tokens,
        profiler.collective_ns // 1000,
        profiler.calls_all_reduce, profiler.payload_all_reduce_bytes,
        profiler.calls_broadcast, profiler.payload_broadcast_bytes,
        rss_sample_bytes, peak_rss_bytes_linux,
    )
    if any(type(x) is not int or x < 0 for x in fields):
        raise ValueError("invalid per-rank counter")
    return list(fields)


PROFILE_FIELDS = (
    "preflight_us", "decode_us", "prefill_us", "generated_steps",
    "collective_wall_us", "all_reduce_calls", "all_reduce_tensor_bytes",
    "broadcast_calls", "broadcast_tensor_bytes", "rss_sample_bytes",
    "peak_rss_bytes_linux",
)


def interpret_rank_stats(values):
    if len(values) != len(PROFILE_FIELDS):
        raise ValueError("unexpected rank profiler record")
    data = dict(zip(PROFILE_FIELDS, (int(x) for x in values)))
    if any(value < 0 for value in data.values()):
        raise ValueError("negative profiler metric")
    data["collective_fraction_of_decode"] = round(
        min(1.0, data["collective_wall_us"] / max(1, data["decode_us"])), 6)
    data["collective_fraction_note"] = (
        "Collective wall time includes synchronization and peer waiting; "
        "tensor payload bytes are not measured physical network traffic.")
    return data
