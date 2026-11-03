"""Local-only benchmark API. Do not expose without authentication and isolation."""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from compute.shards import prepare

app = FastAPI(title="OpenCompute AI v0.1")


class Benchmark(BaseModel):
    workers: int = Field(default=2, ge=1, le=4)
    hidden: int = Field(default=512, ge=64, le=2048)
    batch: int = Field(default=16, ge=1, le=128)


@app.get("/health")
def health():
    return {"status": "ok", "version": "0.1.1"}


@app.post("/benchmarks")
def benchmark(payload: Benchmark):
    if payload.hidden % payload.workers:
        raise HTTPException(status_code=422, detail="hidden must be divisible by workers")
    with tempfile.TemporaryDirectory(prefix="opencompute-shards-") as tmp:
        prepare(tmp, payload.hidden, payload.workers)
        command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   f"--nproc-per-node={payload.workers}", "-m", "compute.sharded_demo",
                   "--shards", tmp, "--batch", str(payload.batch), "--iterations", "3"]
        try:
            result = subprocess.run(command, capture_output=True, text=True,
                                    timeout=120, env={**os.environ, "OMP_NUM_THREADS": "1"})
        except subprocess.TimeoutExpired:
            raise HTTPException(status_code=504, detail="benchmark timed out")
        if result.returncode:
            raise HTTPException(status_code=500, detail="benchmark failed: " + result.stderr[-1000:])
        for line in reversed(result.stdout.splitlines()):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
        raise HTTPException(status_code=500, detail="benchmark produced no report")
