"""Measure real Gloo collectives; optionally add an explicitly synthetic WAN penalty.

Bandwidth model is a ring-AllReduce approximation, not actual bandwidth shaping.
All ranks apply the same sleep after the real collective to preserve ordering.
"""
import time

import torch.distributed as dist


class CommunicationMeter:
    def __init__(self, world: int, latency_ms: float = 0.0,
                 bandwidth_mbps: float = 0.0):
        if world < 1 or latency_ms < 0 or bandwidth_mbps < 0:
            raise ValueError("workers must be positive and network profile nonnegative")
        self.world = world
        self.latency_ms = float(latency_ms)
        self.bandwidth_mbps = float(bandwidth_mbps)
        self.calls = {"broadcast": 0, "all_reduce": 0}
        self.payload_bytes = {"broadcast": 0, "all_reduce": 0}
        self.wire_bytes_estimate = 0.0
        self.gloo_seconds = 0.0
        self.injected_seconds = 0.0

    def _run(self, kind, tensor, call):
        # A one-rank process group needs no network collective at all.
        if self.world == 1:
            return
        size = tensor.numel() * tensor.element_size()
        before = time.perf_counter()
        call()
        self.gloo_seconds += time.perf_counter() - before
        self.calls[kind] += 1
        self.payload_bytes[kind] += size
        # AllReduce: ring algorithm has 2*(N-1)/N bytes per rank.
        # Broadcast: simplified one-full-payload-per-rank model.
        multiplier = 2 * (self.world - 1) / self.world if kind == "all_reduce" else 1.0
        wire_bytes = size * multiplier
        self.wire_bytes_estimate += wire_bytes
        delay = self.latency_ms / 1000.0
        if self.bandwidth_mbps > 0:
            delay += wire_bytes * 8 / (self.bandwidth_mbps * 1_000_000)
        if delay > 0:
            time.sleep(delay)
            self.injected_seconds += delay

    def all_reduce(self, tensor):
        self._run("all_reduce", tensor, lambda: dist.all_reduce(tensor, op=dist.ReduceOp.SUM))

    def broadcast(self, tensor, src=0):
        self._run("broadcast", tensor, lambda: dist.broadcast(tensor, src=src))

    def report(self):
        return {
            "calls": dict(self.calls),
            "payload_bytes": dict(self.payload_bytes),
            "wire_bytes_estimate_per_rank": round(self.wire_bytes_estimate),
            "gloo_seconds": round(self.gloo_seconds, 6),
            "injected_seconds": round(self.injected_seconds, 6),
            "latency_ms_per_collective": self.latency_ms,
            "bandwidth_mbps_assumed": self.bandwidth_mbps,
            "model_warning": "Synthetic additional delay; not tc netem or real WAN shaping",
        }
