from fastapi import FastAPI
from pydantic import BaseModel, Field
import os
import subprocess
import sys
import json

app = FastAPI(title="OpenCompute AI v0.1")

class Benchmark(BaseModel):
    workers: int = Field(default=2, ge=1, le=4)
    hidden: int = Field(default=512, ge=64, le=2048)
    batch: int = Field(default=16, ge=1, le=128)

@app.get("/health")
def health():
    return {"status": "ok", "version": "0.1.0"}

@app.post("/benchmarks")
def benchmark(payload: Benchmark):
    if payload.hidden % payload.workers:
        return {"error": "hidden must be divisible by workers"}
    # Prototype: synchronous, bounded local subprocess. Keep API bound to localhost.
    cmd = [sys.executable, "-m", "torch.distributed.run",
           "--standalone", f"--nproc-per-node={payload.workers}",
           "/app/compute/tp_demo.py", "--hidden", str(payload.hidden),
           "--batch", str(payload.batch), "--iterations", "3"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90,
                                env={**os.environ, "OMP_NUM_THREADS": "1"})
    except subprocess.TimeoutExpired:
        return {"error": "benchmark timed out"}
    if result.returncode:
        return {"error": "benchmark failed", "details": result.stderr[-1500:]}
    for line in reversed(result.stdout.splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {"error": "no benchmark result"}
