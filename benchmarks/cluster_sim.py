"""Pure-local heterogeneity simulation exercising the real SQLite scheduler."""
import json
import tempfile

from cluster_v03.store import ClusterStore
from cluster_v03.worker import execute_task


def simulate():
    with tempfile.TemporaryDirectory(prefix="oc-hetero-") as directory:
        store = ClusterStore(f"{directory}/cluster.sqlite")
        profiles = [
            ("slow-laptop", 2, 1024, 20),
            ("fast-desktop", 8, 8192, 120),
            ("small-device", 2, 256, 8),
        ]
        nodes = {}
        for name, cores, ram, score in profiles:
            registered = store.register(name=name, platform="simulated",
                                        cpu_cores=cores, ram_mb=ram,
                                        compute_score=score, slots=1,
                                        capabilities=["sum", "sha256", "matmul_checksum"])
            nodes[name] = registered
            store.heartbeat(registered["id"], registered["node_token"],
                            free_ram_mb=ram, cpu_load_pct=0)
        for i in range(4):
            store.submit("sum", {"numbers": [1, 2, i]}, required_cpu=1,
                         required_memory_mb=64)
        allocations = []
        # Fast desktop wins the first claim; while its single slot is reserved,
        # the laptop receives the second. Small device must be a valid fallback.
        for name in ("fast-desktop", "slow-laptop", "small-device"):
            node = nodes[name]
            claimed = store.claim(node["id"], node["node_token"])["task"]
            if claimed:
                allocations.append({"node": name, "task": claimed["id"]})
        for allocation in allocations:
            name = allocation["node"]
            node = nodes[name]
            # Repeated claim is idempotently unavailable for a saturated slot.
            assert store.claim(node["id"], node["node_token"])["task"] is None
        status = store.status()
        assert len(allocations) == 3 and status["counts"]["leased"] == 3
        for allocation in allocations:
            node = nodes[allocation["node"]]
            job = next(t for t in status["tasks"] if t["id"] == allocation["task"])
            # Stored task payload isn't echoed in admin status; demo input is known.
            # Retrieve current lease through direct storage only for this test.
            with store._db() as conn:
                raw = conn.execute("SELECT lease_token,payload_json FROM tasks WHERE id=?",
                                   (job["id"],)).fetchone()
            result = execute_task("sum", json.loads(raw["payload_json"]))
            store.complete(node["id"], node["node_token"], job["id"],
                           raw["lease_token"], result)
        return {"nodes": [name for name, *_ in profiles],
                "first_claims": allocations,
                "status": store.status()["counts"],
                "remark": "Simulated capabilities; no actual remote compute or model TP"}


if __name__ == "__main__":
    print(json.dumps(simulate(), ensure_ascii=False, indent=2))
