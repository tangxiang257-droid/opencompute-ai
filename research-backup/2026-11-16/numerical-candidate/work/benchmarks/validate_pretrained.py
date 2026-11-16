"""Opt-in end-to-end parity check against a real GPT-2 HF checkpoint.

This command downloads the named checkpoint/tokenizer unless already cached.
Use only trusted model repositories; trust_remote_code is disabled.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import torch

from compute.gpt2_shards import export_state


def validate(model_id: str = "sshleifer/tiny-gpt2", prompt: str = "Hello, world",
             workers: int = 2, new_tokens: int = 2, atol: float = 5e-4):
    from transformers import AutoModelForCausalLM, AutoTokenizer

    torch.set_num_threads(1)
    model = AutoModelForCausalLM.from_pretrained(
        model_id, trust_remote_code=False, attn_implementation="eager"
    ).float().eval()
    if model.config.model_type != "gpt2":
        raise ValueError("Only ordinary GPT-2 checkpoints are supported")
    tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=False)
    prompt_ids = tokenizer.encode(prompt, add_special_tokens=False)
    if not prompt_ids:
        raise ValueError("prompt produced zero tokens")
    if len(prompt_ids) + new_tokens - 1 > model.config.n_positions:
        raise ValueError("prompt + generated tokens exceed context")
    expected_logits = []
    expected_tokens = []
    context = list(prompt_ids)
    with torch.inference_mode():
        for _ in range(new_tokens):
            logits = model(input_ids=torch.tensor([context]), use_cache=False).logits[0, -1]
            expected_logits.append(logits.cpu())
            token = int(logits.argmax().item())
            expected_tokens.append(token)
            context.append(token)
    with tempfile.TemporaryDirectory(prefix="oc-real-gpt2-") as tmp:
        export_state(model.state_dict(), model.config.to_dict(), tmp, workers,
                     source=model_id)
        command = [sys.executable, "-m", "torch.distributed.run", "--standalone",
                   f"--nproc-per-node={workers}", "-m", "compute.gpt2_generate",
                   "--shards", tmp, "--prompt-ids", ",".join(map(str, prompt_ids)),
                   "--max-new-tokens", str(new_tokens), "--report-logits"]
        completed = subprocess.run(command, capture_output=True, text=True,
                                   timeout=180, env={**os.environ, "OMP_NUM_THREADS": "1"})
        if completed.returncode:
            raise RuntimeError(completed.stderr[-4000:])
        reports = [json.loads(line) for line in completed.stdout.splitlines()
                   if line.startswith('{"engine":')]
        if len(reports) != 1:
            raise RuntimeError("missing distributed generation output")
        actual = torch.tensor(reports[0]["step_logits"])
        expected = torch.stack(expected_logits)
        max_error = (actual - expected).abs().max().item()
        logits_match = torch.allclose(actual, expected, atol=atol, rtol=atol)
        tokens_match = reports[0]["generated_ids"] == expected_tokens
        result = {
            "model": model_id, "prompt": prompt, "workers": workers,
            "new_tokens": new_tokens, "max_abs_logit_error": max_error,
            "logits_match": logits_match, "greedy_tokens_match": tokens_match,
            "expected_ids": expected_tokens, "actual_ids": reports[0]["generated_ids"],
            "passed": logits_match and tokens_match,
        }
        if not result["passed"]:
            raise AssertionError(json.dumps(result))
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="sshleifer/tiny-gpt2")
    parser.add_argument("--prompt", default="Hello, world")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--new-tokens", type=int, default=2)
    args = parser.parse_args()
    print(json.dumps(validate(args.model, args.prompt, args.workers, args.new_tokens), indent=2))
