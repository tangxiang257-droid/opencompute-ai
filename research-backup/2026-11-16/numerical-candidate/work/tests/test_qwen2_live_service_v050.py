"""v0.5.0 live request admission and Qwen2 Gloo service integration tests."""
import asyncio
import os
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v05.api import create_router
from cluster_v05.qwen2_service import Qwen2PipelineService
from compute.request_queue_v049 import QueueFullError


def fake_report(requests, max_new_tokens):
    return {
        "engine": "fake-qwen2-pipeline",
        "model": "offline-fixture",
        "elapsed_seconds_rank0": 0.001,
        "request_count": len(requests),
        "results": [
            {
                "id": request["id"],
                "generated_ids": [
                    (request["prompt_ids"][0] + offset) % 64
                    for offset in range(max_new_tokens)
                ],
            }
            for request in requests
        ],
    }


class FakeRunner:
    def __init__(self):
        self.calls = []

    async def __call__(self, requests, max_new_tokens, timeout):
        self.calls.append([dict(item) for item in requests])
        await asyncio.sleep(0.005)
        return fake_report(requests, max_new_tokens)


class LiveQueueUnitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.runner = FakeRunner()
        self.service = Qwen2PipelineService(
            self.temp.name,
            runner=self.runner,
            batch_wait_ms=40,
            max_batch_size=4,
            max_new_tokens=3,
        )
        await self.service.start()

    async def asyncTearDown(self):
        await self.service.close()
        self.temp.cleanup()

    async def test_dynamic_requests_are_batched_and_fair(self):
        first = asyncio.create_task(self.service.submit("client-a", [1, 2]))
        await asyncio.sleep(0.01)
        second = asyncio.create_task(self.service.submit("client-b", [5, 6]))
        third = asyncio.create_task(self.service.submit("client-a", [3, 4]))
        results = await asyncio.gather(first, second, third)
        self.assertEqual(len(self.runner.calls), 1)
        self.assertEqual(
            [item["prompt_ids"][0] for item in self.runner.calls[0]],
            [1, 5, 3],
        )
        self.assertTrue(all(item["batch_size"] == 3 for item in results))
        self.assertTrue(all(len(item["generated_ids"]) == 3 for item in results))
        self.assertEqual(self.service.status["requests_completed"], 3)
        self.assertEqual(self.service.status["batches_completed"], 1)

    async def test_shutdown_does_not_dispatch_a_batch_waiting_for_more_requests(self):
        request = asyncio.create_task(self.service.submit("client-a", [1]))
        await asyncio.sleep(0.01)
        await self.service.close()
        with self.assertRaisesRegex(RuntimeError, "closed before dispatch"):
            await request
        self.assertEqual(self.runner.calls, [])

    async def test_backpressure_rejects_when_pending_limit_is_reached(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked_runner(requests, max_new_tokens, timeout):
            started.set()
            await release.wait()
            return fake_report(requests, max_new_tokens)

        await self.service.close()
        self.service = Qwen2PipelineService(
            self.temp.name,
            runner=blocked_runner,
            max_pending=1,
            max_pending_per_client=1,
            max_batch_size=1,
            batch_wait_ms=0,
            max_new_tokens=2,
        )
        await self.service.start()
        first = asyncio.create_task(self.service.submit("a", [1]))
        await asyncio.wait_for(started.wait(), timeout=2)
        second = asyncio.create_task(self.service.submit("b", [2]))
        await asyncio.sleep(0)
        with self.assertRaises(QueueFullError):
            await self.service.submit("c", [3])
        release.set()
        first_result, second_result = await asyncio.gather(first, second)
        self.assertEqual(first_result["generated_ids"], [1, 2])
        self.assertEqual(second_result["generated_ids"], [2, 3])

    async def test_runner_failure_is_returned_to_request_and_worker_recovers(self):
        calls = 0

        async def sometimes_fails(requests, max_new_tokens, timeout):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("synthetic executor failure")
            return fake_report(requests, max_new_tokens)

        await self.service.close()
        self.service = Qwen2PipelineService(
            self.temp.name, runner=sometimes_fails,
            batch_wait_ms=0, max_new_tokens=2,
        )
        await self.service.start()
        with self.assertRaisesRegex(RuntimeError, "synthetic executor failure"):
            await self.service.submit("a", [1])
        result = await self.service.submit("b", [2])
        self.assertEqual(result["generated_ids"], [2, 3])
        self.assertEqual(self.service.status["batches_failed"], 1)
        self.assertEqual(self.service.status["requests_completed"], 1)

    def test_http_route_authenticates_and_dispatches_through_queue(self):
        temp = tempfile.TemporaryDirectory()
        service = Qwen2PipelineService(temp.name, runner=FakeRunner(), batch_wait_ms=0)
        app = FastAPI()
        app.include_router(create_router(service, admin_key="test-admin-key-long-enough"))
        app.router.on_startup.append(service.start)
        app.router.on_shutdown.append(service.close)
        try:
            with TestClient(app) as client:
                denied = client.post(
                    "/v0.5/requests",
                    headers={"X-Client-ID": "client-a"},
                    json={"prompt_ids": [1, 2, 3]},
                )
                self.assertEqual(denied.status_code, 401)
                accepted = client.post(
                    "/v0.5/requests",
                    headers={
                        "X-Admin-Key": "test-admin-key-long-enough",
                        "X-Client-ID": "client-a",
                    },
                    json={"prompt_ids": [7, 8]},
                )
                self.assertEqual(accepted.status_code, 200, accepted.text)
                self.assertEqual(accepted.json()["generated_ids"], [7, 8, 9])
                self.assertEqual(accepted.json()["batch_size"], 1)
        finally:
            temp.cleanup()

    def test_http_rejects_boolean_token_ids_and_missing_client_identity(self):
        temp = tempfile.TemporaryDirectory()
        service = Qwen2PipelineService(temp.name, runner=FakeRunner(), batch_wait_ms=0)
        app = FastAPI()
        app.include_router(create_router(service, admin_key="test-admin-key-long-enough"))
        app.router.on_startup.append(service.start)
        app.router.on_shutdown.append(service.close)
        try:
            with TestClient(app) as client:
                headers = {
                    "X-Admin-Key": "test-admin-key-long-enough",
                    "X-Client-ID": "client-a",
                }
                invalid = client.post(
                    "/v0.5/requests", headers=headers, json={"prompt_ids": [True]}
                )
                self.assertEqual(invalid.status_code, 422)
                missing = client.post(
                    "/v0.5/requests",
                    headers={"X-Admin-Key": "test-admin-key-long-enough"},
                    json={"prompt_ids": [1]},
                )
                self.assertEqual(missing.status_code, 422)
        finally:
            temp.cleanup()


@unittest.skipUnless(
    os.environ.get("OPENCOMPUTE_REQUIRE_LIVE_PP_V050") == "1",
    "real two-rank Qwen2 Gloo integration is enabled in cluster CI",
)
class LiveQwen2GlooIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_http_queue_batch_executes_real_qwen2_two_stage_pipeline(self):
        import torch
        from httpx import ASGITransport, AsyncClient
        from compute.qwen2_chunked import export_chunked
        from compute.qwen2_pipeline_v047 import partition_one_rank_v2
        from compute.qwen2_reference import generate_dense
        from compute.qwen2_shards import synthetic_state

        torch.set_num_threads(1)
        prompts = [
            {"client": "client-a", "tokens": [1, 2, 3]},
            {"client": "client-b", "tokens": [4, 5]},
            {"client": "client-a", "tokens": [6, 7, 8, 9]},
        ]
        admin_key = "integration-admin-key-long-enough"
        with tempfile.TemporaryDirectory(prefix="opencompute-v050-live-") as temp:
            root = Path(temp)
            state, config = synthetic_state(
                hidden=24, heads=6, kv_heads=2, layers=2,
                inner=48, vocab=64, positions=64, seed=5050,
            )
            source = root / "tp1"
            bundle = root / "pp"
            export_chunked(
                state, config, source, workers=1, chunk_rows=11,
                source="random-v050-live-service",
            )
            partition_one_rank_v2(source, bundle)
            service = Qwen2PipelineService(
                bundle,
                max_batch_size=4,
                batch_wait_ms=60,
                max_new_tokens=3,
                max_kv_mib=64,
                request_timeout_seconds=90,
                execution_timeout_seconds=90,
            )
            app = FastAPI()
            app.include_router(create_router(service, admin_key=admin_key))
            await service.start()
            try:
                async with AsyncClient(
                    transport=ASGITransport(app=app), base_url="http://opencompute.test"
                ) as client:
                    responses = await asyncio.gather(*[
                        client.post(
                            "/v0.5/requests",
                            headers={
                                "X-Admin-Key": admin_key,
                                "X-Client-ID": item["client"],
                            },
                            json={"prompt_ids": item["tokens"]},
                        )
                        for item in prompts
                    ])
                self.assertTrue(
                    all(response.status_code == 200 for response in responses),
                    [response.text for response in responses],
                )
                results = [response.json() for response in responses]
            finally:
                await service.close()

            self.assertTrue(all(item["batch_size"] == 3 for item in results))
            self.assertEqual(service.status["batches_completed"], 1)
            for prompt, result in zip(prompts, results):
                expected = generate_dense(state, config, prompt["tokens"], 3)
                self.assertEqual(result["generated_ids"], expected["generated_ids"])


if __name__ == "__main__":
    unittest.main()
