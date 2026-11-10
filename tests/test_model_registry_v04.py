"""v0.4 multi-model registry and strict backend adapter dispatch tests.

No pretrained model download and no distributed process is started by these
tests. They validate the integration contract and planned-engine rejection.
"""
import unittest
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from cluster_v04.api import create_model_router
from cluster_v04.models import MODELS, list_models, model_by_id, require_runtime, ModelUnavailable


ADMIN = "local-test-admin-credential-long-123456789"


class ModelCatalogTests(unittest.TestCase):
    def setUp(self):
        self.coordinator = Mock()
        self.coordinator.create_synthetic.return_value = {
            "session_id": "a" * 32, "status": "preparing", "workers": 2}
        self.coordinator.create_checkpoint.return_value = {
            "session_id": "b" * 32, "status": "preparing", "workers": 1}
        app = FastAPI()
        app.include_router(create_model_router(self.coordinator, admin_key=ADMIN))
        self.client = TestClient(app)
        self.auth = {"X-Admin-Key": ADMIN}

    def tearDown(self):
        self.client.close()

    def test_catalog_includes_multiple_families_but_only_gpt2_has_runner(self):
        response = self.client.get("/v0.4/models")
        self.assertEqual(response.status_code, 200, response.text)
        items = response.json()["models"]
        self.assertEqual(len(items), len(MODELS))
        self.assertGreaterEqual(len({i["family"] for i in items}), 3)
        self.assertEqual({m["id"] for m in items if m["launchable"]},
                         {"gpt2-synthetic", "gpt2-local-checkpoint",
                          "qwen2-synthetic"})
        deepseek = next(m for m in items
                        if m["id"] == "deepseek-r1-distill-qwen-1.5b")
        self.assertEqual(deepseek["architecture"], "qwen2")
        self.assertFalse(deepseek["launchable"])
        self.assertEqual(
            self.client.get("/v0.4/models?family=DeepSeek").json()["count"], 3)
        self.assertEqual(len(list_models(family="qwen")), 2)

    def test_no_arbitrary_unknown_model_or_engine(self):
        self.assertEqual(self.client.get("/v0.4/models/random-model").status_code, 404)
        with self.assertRaises(KeyError):
            model_by_id("../../arbitrary")
        with self.assertRaises(ModelUnavailable):
            require_runtime("deepseek-r1-distill-qwen-1.5b", "tensor_parallel")
        with self.assertRaises(ValueError):
            require_runtime("gpt2-synthetic", "remote_api")

    def test_unimplemented_model_never_starts_compute(self):
        for model_id in ("deepseek-r1-distill-qwen-1.5b",
                         "qwen2.5-0.5b-instruct",
                         "deepseek-r1-distill-llama-8b",
                         "deepseek-r1-moe",
                         "custom-api-model"):
            mode = model_by_id(model_id).modes[0]
            response = self.client.post("/v0.4/sessions", headers=self.auth,
                                        json={"model_id": model_id, "mode": mode})
            self.assertEqual(response.status_code, 409, response.text)
            self.assertIn("no implemented inference adapter", response.json()["detail"])
        self.coordinator.create_synthetic.assert_not_called()
        self.coordinator.create_checkpoint.assert_not_called()

    def test_admin_required_and_fail_closed(self):
        payload = {"model_id": "gpt2-synthetic"}
        self.assertEqual(self.client.post("/v0.4/sessions", json=payload).status_code, 401)
        self.assertEqual(self.client.post("/v0.4/sessions", json=payload,
            headers={"X-Admin-Key": "invalid"}).status_code, 401)
        self.coordinator.create_synthetic.assert_not_called()
        no_admin = FastAPI()
        no_admin.include_router(create_model_router(self.coordinator, admin_key=""))
        with TestClient(no_admin) as client:
            self.assertEqual(client.post("/v0.4/sessions",
                headers=self.auth, json=payload).status_code, 503)

    def test_synthetic_launch_routes_to_existing_gpt2_engine(self):
        data = {"model_id": "gpt2-synthetic", "workers": 2,
                "hidden": 16, "heads": 4, "layers": 2,
                "prompt_ids": [1, 2, 3], "max_new_tokens": 2}
        result = self.client.post("/v0.4/sessions", headers=self.auth, json=data)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()["engine"], "gpt2-tp")
        self.assertEqual(result.json()["session"]["session_id"], "a" * 32)
        self.coordinator.create_synthetic.assert_called_once_with(
            workers=2, hidden=16, heads=4, layers=2,
            prompt_ids=[1, 2, 3], new_tokens=2)

    def test_qwen2_synthetic_routes_to_new_engine_not_gpt2(self):
        self.coordinator.create_qwen2_synthetic.return_value = {
            "session_id": "c" * 32, "status": "preparing", "engine": "qwen2-tp"}
        reply = self.client.post("/v0.4/sessions", headers=self.auth, json={
            "model_id": "qwen2-synthetic", "workers": 2,
            "prompt_ids": [1, 2, 3], "max_new_tokens": 2})
        self.assertEqual(reply.status_code, 200, reply.text)
        self.assertEqual(reply.json()["engine"], "qwen2-tp")
        self.coordinator.create_qwen2_synthetic.assert_called_once_with(
            workers=2, hidden=24, heads=6, layers=2,
            prompt_ids=[1, 2, 3], new_tokens=2)
        self.coordinator.create_synthetic.assert_not_called()

    def test_checkpoint_uses_manifest_and_cannot_override_shape(self):
        model = {"model_id": "gpt2-local-checkpoint", "prompt_ids": [1, 2],
                 "max_new_tokens": 3}
        self.assertEqual(self.client.post("/v0.4/sessions",
            headers=self.auth, json=model).status_code, 422)
        self.assertEqual(self.client.post("/v0.4/sessions",
            headers=self.auth,
            json={**model, "checkpoint": "trusted", "workers": 2}).status_code, 422)
        self.coordinator.create_checkpoint.assert_not_called()
        result = self.client.post("/v0.4/sessions", headers=self.auth,
                                  json={**model, "checkpoint": "trusted"})
        self.assertEqual(result.status_code, 200, result.text)
        self.coordinator.create_checkpoint.assert_called_once_with(
            checkpoint="trusted", prompt_ids=[1, 2], new_tokens=3)

    def test_invalid_mode_rejected_before_compute(self):
        response = self.client.post("/v0.4/sessions", headers=self.auth,
            json={"model_id": "gpt2-synthetic", "mode": "remote_api"})
        self.assertEqual(response.status_code, 422)
        self.coordinator.create_synthetic.assert_not_called()


if __name__ == "__main__":
    unittest.main()
