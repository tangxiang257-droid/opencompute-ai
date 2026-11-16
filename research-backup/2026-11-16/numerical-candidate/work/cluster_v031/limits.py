"""Local process guardrails for the trusted CPU tensor-parallel prototype.

CPU time is constrained by POSIX RLIMIT_CPU inside the child before PyTorch
imports. Optional RLIMIT_AS restricts *virtual address space*, not RSS, and
may break PyTorch on otherwise adequately provisioned machines. RSS monitoring
in the parent is best-effort and cannot prevent instantaneous memory spikes.
No security boundary against malicious code or remote peers is claimed.
"""
import argparse
import os
import runpy
import sys

import psutil


def _check_positive_int(name, value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer in {low}..{high}")
    return value


def process_tree_rss_bytes(pid):
    """Return currently visible process-tree RSS, or None if not observable.

    Counting shared memory in multiple processes can overestimate usage,
    which is acceptable for a conservative best-effort safety watchdog.
    """
    try:
        parent = psutil.Process(pid)
        members = [parent] + parent.children(recursive=True)
        total = 0
        for process in members:
            try:
                total += process.memory_info().rss
            except psutil.NoSuchProcess:
                continue
        return total
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return None


def apply_kernel_limits(cpu_seconds=180, max_vmem_mib=None):
    """Apply POSIX per-process limits, without silently claiming RSS isolation."""
    cpu_seconds = _check_positive_int("cpu_seconds", cpu_seconds, 1, 3600)
    if max_vmem_mib is not None:
        max_vmem_mib = _check_positive_int(
            "max_vmem_mib", max_vmem_mib, 512, 1048576)
    if os.name != "posix":
        raise RuntimeError("managed CPU time quota requires POSIX; fail closed")
    import resource

    def tighten(which, soft, hard):
        previous_soft, previous_hard = resource.getrlimit(which)
        if previous_hard != resource.RLIM_INFINITY:
            hard = min(hard, previous_hard)
        if previous_soft != resource.RLIM_INFINITY:
            soft = min(soft, previous_soft)
        soft = min(soft, hard)
        resource.setrlimit(which, (soft, hard))

    tighten(resource.RLIMIT_CPU, cpu_seconds, cpu_seconds + 2)
    if max_vmem_mib is not None:
        bytes_limit = max_vmem_mib * 1024 * 1024
        tighten(resource.RLIMIT_AS, bytes_limit, bytes_limit)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Apply local CPU quotas before loading trusted GPT-2 inference")
    parser.add_argument("--cpu-seconds", type=int, default=180)
    parser.add_argument("--engine", choices=("gpt2-tp", "qwen2-tp"),
                        default="gpt2-tp")
    parser.add_argument("--max-vmem-mib", type=int)
    parser.add_argument("model_arguments", nargs=argparse.REMAINDER)
    opts = parser.parse_args(argv)
    args = list(opts.model_arguments)
    if not args or args.pop(0) != "--":
        parser.error("expected '--' before the fixed inference arguments")
    apply_kernel_limits(opts.cpu_seconds, opts.max_vmem_mib)
    # A fixed, auditable engine table; never accept arbitrary module names.
    engine_module = {"gpt2-tp": "compute.gpt2_generate",
                     "qwen2-tp": "compute.qwen2_generate"}[opts.engine]
    sys.argv = [engine_module, *args]
    runpy.run_module(engine_module, run_name="__main__", alter_sys=False)


if __name__ == "__main__":
    main()
