"""Rendezvous for coordinator-owned CPU Gloo process groups.

Default torchrun / env:// initialization remains unchanged for standalone
v0.2 experiments. Managed v0.3.3 sessions use a pre-bound TCPStore owned by the
coordinator, eliminating the bind-and-release ephemeral port race.
"""
import os
from datetime import timedelta

import torch.distributed as dist


def init_gloo(*, timeout_seconds=120):
    """Join a process group; never start a rendezvous server in a managed rank."""
    timeout = timedelta(seconds=timeout_seconds)
    if os.environ.get("OPENCOMPUTE_EXTERNAL_STORE") == "1":
        rank = int(os.environ["RANK"])
        world = int(os.environ["WORLD_SIZE"])
        host = os.environ["MASTER_ADDR"]
        port = int(os.environ["MASTER_PORT"])
        if host not in ("127.0.0.1", "localhost"):
            raise ValueError("managed v0.3 rendezvous is localhost-only")
        if not 1 <= world <= 4 or not 0 <= rank < world or not 1 <= port <= 65535:
            raise ValueError("invalid managed rendezvous parameters")
        store = dist.TCPStore(host_name=host, port=port, world_size=None,
                              is_master=False, timeout=timeout)
        dist.init_process_group("gloo", store=store, rank=rank,
                                world_size=world, timeout=timeout)
        return store  # retain until destroy_process_group
    dist.init_process_group("gloo", timeout=timeout)
    return None
