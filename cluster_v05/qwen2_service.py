"""Dynamic, fair HTTP admission for the trusted-localhost Qwen2 PP engine.

The first live-service adapter deliberately starts one fresh two-rank Gloo
process group per microbatch. It proves that API requests flow through the
bounded queue into the real v0.4.8 Qwen2 pipeline, without pretending that the
process group is persistent or suitable for untrusted WAN workers.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import time
import uuid
from typing import Any, Awaitable, Callable

from compute.request_queue_v049 import FairRequestQueue, QueueFullError, Request


Runner = Callable[[list[dict[str, Any]], int, float], Awaitable[dict[str, Any]]]
_CLIENT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}\Z")


class Qwen2PipelineService:
    """One fair queue and one bounded consumer for a local Qwen2 PP bundle."""

    def __init__(
        self,
        bundle: str | os.PathLike[str],
        *,
        max_pending: int = 128,
        max_pending_per_client: int = 16,
        max_batch_size: int = 4,
        batch_wait_ms: int = 12,
        max_new_tokens: int = 3,
        max_kv_mib: int = 64,
        request_timeout_seconds: float = 120.0,
        execution_timeout_seconds: float = 120.0,
        shutdown_grace_seconds: float = 10.0,
        runner: Runner | None = None,
    ):
        candidate = Path(bundle).expanduser()
        if candidate.is_symlink():
            raise ValueError("Qwen2 PP bundle must not be a symlink")
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise ValueError("Qwen2 PP bundle does not exist") from exc
        if not resolved.is_dir():
            raise ValueError("Qwen2 PP bundle must be a directory")

        if type(max_batch_size) is not int or not 1 <= max_batch_size <= 4:
            raise ValueError("max_batch_size must be between 1 and 4")
        if type(batch_wait_ms) is not int or not 0 <= batch_wait_ms <= 1000:
            raise ValueError("batch_wait_ms must be between 0 and 1000")
        if type(max_new_tokens) is not int or not 1 <= max_new_tokens <= 8:
            raise ValueError("max_new_tokens must be between 1 and 8")
        if type(max_kv_mib) is not int or not 1 <= max_kv_mib <= 4096:
            raise ValueError("max_kv_mib must be between 1 and 4096")
        for name, value in (
            ("request_timeout_seconds", request_timeout_seconds),
            ("execution_timeout_seconds", execution_timeout_seconds),
            ("shutdown_grace_seconds", shutdown_grace_seconds),
        ):
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or value <= 0):
                raise ValueError(f"{name} must be positive")

        self.bundle = resolved
        self.repo_root = Path(__file__).resolve().parents[1]
        self.max_batch_size = max_batch_size
        self.batch_wait_seconds = batch_wait_ms / 1000.0
        self.batch_wait_ms = batch_wait_ms
        self.max_new_tokens = max_new_tokens
        self.max_kv_mib = max_kv_mib
        self.request_timeout_seconds = float(request_timeout_seconds)
        self.execution_timeout_seconds = float(execution_timeout_seconds)
        self.shutdown_grace_seconds = float(shutdown_grace_seconds)
        self.queue = FairRequestQueue(
            max_pending=max_pending,
            max_pending_per_client=max_pending_per_client,
        )
        self._runner = runner or self._run_local_batch
        self._task: asyncio.Task | None = None
        self._closing = False
        self._active_process: asyncio.subprocess.Process | None = None
        self._active_request_ids: tuple[str, ...] = ()
        self._batches_completed = 0
        self._batches_failed = 0
        self._requests_completed = 0
        self._requests_failed = 0
        self._last_error: str | None = None

    @classmethod
    def from_env(cls) -> "Qwen2PipelineService | None":
        """Build the optional local service from deployment environment.

        An unset bundle keeps the route mounted but disabled. A configured
        invalid bundle fails fast instead of silently falling back to a
        synthetic model or another execution engine.
        """
        bundle = os.getenv("OPENCOMPUTE_QWEN2_PP_BUNDLE", "").strip()
        if not bundle:
            return None

        def integer(name: str, default: int) -> int:
            return int(os.getenv(name, str(default)))

        return cls(
            bundle,
            max_pending=integer("OPENCOMPUTE_QWEN2_PP_MAX_PENDING", 128),
            max_pending_per_client=integer(
                "OPENCOMPUTE_QWEN2_PP_MAX_PENDING_PER_CLIENT", 16
            ),
            max_batch_size=integer("OPENCOMPUTE_QWEN2_PP_MAX_BATCH_SIZE", 4),
            batch_wait_ms=integer("OPENCOMPUTE_QWEN2_PP_BATCH_WAIT_MS", 12),
            max_new_tokens=integer("OPENCOMPUTE_QWEN2_PP_MAX_NEW_TOKENS", 3),
            max_kv_mib=integer("OPENCOMPUTE_QWEN2_PP_MAX_KV_MIB", 64),
            request_timeout_seconds=float(
                os.getenv("OPENCOMPUTE_QWEN2_PP_REQUEST_TIMEOUT_SECONDS", "120")
            ),
            execution_timeout_seconds=float(
                os.getenv("OPENCOMPUTE_QWEN2_PP_EXECUTION_TIMEOUT_SECONDS", "120")
            ),
            shutdown_grace_seconds=float(
                os.getenv("OPENCOMPUTE_QWEN2_PP_SHUTDOWN_GRACE_SECONDS", "10")
            ),
        )

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done() and not self._closing

    @property
    def status(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "running": self.running,
            "engine": "opencompute-qwen2-pipeline-batched-v0.4.8",
            "queue_pending": self.queue.pending_count,
            "queue_capacity": self.queue.max_pending,
            "per_client_queue_capacity": self.queue.max_pending_per_client,
            "active_requests": len(self._active_request_ids),
            "active_request_ids": list(self._active_request_ids),
            "max_batch_size": self.max_batch_size,
            "batch_wait_ms": self.batch_wait_ms,
            "max_new_tokens": self.max_new_tokens,
            "batches_completed": self._batches_completed,
            "batches_failed": self._batches_failed,
            "requests_completed": self._requests_completed,
            "requests_failed": self._requests_failed,
            "last_error": self._last_error,
        }

    async def start(self) -> None:
        if self._closing:
            raise RuntimeError("Qwen2 pipeline service is closed")
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._worker_loop(), name="opencompute-qwen2-live-queue"
        )

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        await self.queue.close()
        if self._task is None:
            return
        try:
            await asyncio.wait_for(
                asyncio.shield(self._task), timeout=self.shutdown_grace_seconds
            )
        except asyncio.TimeoutError:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        except asyncio.CancelledError:
            pass

    async def submit(self, client_id: str, prompt_ids: list[int]) -> dict[str, Any]:
        if not self.running:
            raise RuntimeError("Qwen2 pipeline service is not running")
        if not isinstance(client_id, str) or not _CLIENT_ID.fullmatch(client_id):
            raise ValueError("client_id must be 1..64 safe ASCII characters")
        if (not isinstance(prompt_ids, list) or not 1 <= len(prompt_ids) <= 128
                or any(type(token) is not int or token < 0 or token > 2_147_483_647
                       for token in prompt_ids)):
            raise ValueError("prompt_ids must contain 1..128 bounded non-negative integer IDs")

        request_id = uuid.uuid4().hex
        ticket = await self.queue.put(
            request_id,
            client_id,
            {"prompt_ids": list(prompt_ids)},
            timeout=self.request_timeout_seconds,
        )
        return await ticket.result(timeout=self.request_timeout_seconds + 10.0)

    async def _worker_loop(self) -> None:
        while True:
            try:
                first = await self.queue.get()
            except RuntimeError:
                if self._closing:
                    return
                raise
            batch = [first]
            try:
                while len(batch) < self.max_batch_size:
                    try:
                        request = await asyncio.wait_for(
                            self.queue.get(), timeout=self.batch_wait_seconds
                        )
                    except asyncio.TimeoutError:
                        break
                    except RuntimeError:
                        break
                    batch.append(request)
                if self._closing:
                    await self._fail_batch(
                        batch, RuntimeError("request queue closed before dispatch")
                    )
                    return
                await self._execute_batch(batch)
            except asyncio.CancelledError:
                await self._fail_batch(
                    batch, RuntimeError("Qwen2 pipeline service shut down")
                )
                raise

    async def _fail_batch(self, requests: list[Request], error: BaseException) -> None:
        for request in requests:
            if request.state == "in_flight":
                try:
                    await self.queue.fail(request, error)
                    self._requests_failed += 1
                except RuntimeError:
                    pass

    async def _execute_batch(self, batch: list[Request]) -> None:
        now = time.monotonic()
        live: list[Request] = []
        for request in batch:
            if request.deadline <= now:
                await self.queue.fail(
                    request, TimeoutError("request expired before inference dispatch")
                )
                self._requests_failed += 1
            else:
                live.append(request)
        if not live:
            return

        self._active_request_ids = tuple(item.request_id for item in live)
        remaining = min(item.deadline for item in live) - time.monotonic()
        timeout = min(self.execution_timeout_seconds, remaining)
        if timeout <= 0:
            await self._fail_batch(
                live, TimeoutError("request deadline expired before inference")
            )
            self._active_request_ids = ()
            return

        payload = [
            {"id": item.request_id, "prompt_ids": item.payload["prompt_ids"]}
            for item in live
        ]
        try:
            report = await self._runner(payload, self.max_new_tokens, timeout)
            rows = report.get("results") if isinstance(report, dict) else None
            if not isinstance(rows, list) or len(rows) != len(live):
                raise RuntimeError("Qwen2 executor returned an invalid result batch")
            by_id: dict[str, dict[str, Any]] = {}
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("id"), str):
                    raise RuntimeError("Qwen2 executor returned a malformed result")
                if row["id"] in by_id:
                    raise RuntimeError("Qwen2 executor returned duplicate result IDs")
                tokens = row.get("generated_ids")
                if (not isinstance(tokens, list) or len(tokens) != self.max_new_tokens
                        or any(type(token) is not int or token < 0 for token in tokens)):
                    raise RuntimeError("Qwen2 executor returned invalid generated tokens")
                by_id[row["id"]] = row
            if set(by_id) != {item.request_id for item in live}:
                raise RuntimeError("Qwen2 executor result IDs do not match dispatched requests")

            for request in live:
                if request.deadline <= time.monotonic():
                    await self.queue.fail(
                        request, TimeoutError("request deadline expired during inference")
                    )
                    self._requests_failed += 1
                    continue
                row = by_id[request.request_id]
                await self.queue.complete(request, {
                    "request_id": request.request_id,
                    "generated_ids": row["generated_ids"],
                    "engine": report.get("engine"),
                    "model": report.get("model"),
                    "batch_size": len(live),
                    "batch_elapsed_seconds": report.get("elapsed_seconds_rank0"),
                })
                self._requests_completed += 1
            self._batches_completed += 1
            self._last_error = None
        except asyncio.CancelledError:
            await self._fail_batch(
                live, RuntimeError("Qwen2 inference cancelled during service shutdown")
            )
            raise
        except Exception as exc:
            self._batches_failed += 1
            self._last_error = f"{type(exc).__name__}: {exc}"[:400]
            await self._fail_batch(live, exc)
        finally:
            self._active_request_ids = ()

    async def _run_local_batch(
        self, requests: list[dict[str, Any]], max_new_tokens: int, timeout: float
    ) -> dict[str, Any]:
        document = {"requests": requests}
        with tempfile.TemporaryDirectory(prefix="opencompute-qwen2-v050-") as temp:
            request_path = Path(temp) / "requests.json"
            fd = os.open(
                request_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(document, handle, ensure_ascii=False, separators=(",", ":"))

            env = os.environ.copy()
            env["OPENCOMPUTE_EXTERNAL_STORE"] = "0"
            env["OMP_NUM_THREADS"] = "1"
            env.setdefault("MKL_NUM_THREADS", "1")
            command = [
                sys.executable, "-m", "torch.distributed.run",
                "--standalone", "--nproc-per-node=2",
                "-m", "compute.qwen2_pipeline_batch_v048",
                "--bundle", str(self.bundle),
                "--requests-json", str(request_path),
                "--mode", "overlap",
                "--max-new-tokens", str(max_new_tokens),
                "--max-kv-mib", str(self.max_kv_mib),
            ]
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(self.repo_root),
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=(os.name == "posix"),
            )
            self._active_process = process
            communication = asyncio.create_task(process.communicate())
            try:
                output, _ = await asyncio.wait_for(
                    asyncio.shield(communication), timeout=timeout
                )
            except asyncio.TimeoutError as exc:
                await self._terminate_process(process)
                try:
                    await asyncio.wait_for(communication, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    communication.cancel()
                raise TimeoutError("Qwen2 inference process exceeded request deadline") from exc
            except asyncio.CancelledError:
                await self._terminate_process(process)
                try:
                    await asyncio.wait_for(communication, timeout=5.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    communication.cancel()
                raise
            finally:
                self._active_process = None

            text = output.decode("utf-8", errors="replace")
            if process.returncode != 0:
                raise RuntimeError(
                    "Qwen2 Gloo executor failed: " + text[-3000:]
                )
            for line in reversed(text.splitlines()):
                try:
                    report = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(report, dict) and isinstance(report.get("results"), list):
                    return report
            raise RuntimeError("Qwen2 Gloo executor produced no JSON result report")

    @staticmethod
    async def _terminate_process(process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=3.0)
        except asyncio.TimeoutError:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            await process.wait()
