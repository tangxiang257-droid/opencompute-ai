"""Safe, read-only NVIDIA GPU discovery for Worker nodes.

The detector invokes a fixed nvidia-smi query without a shell. Reported values
are telemetry supplied by the Worker and are not hardware attestation.
"""
import argparse
import csv
import io
import json
import shutil
import subprocess


def _query(binary, fields):
    command = [binary, f"--query-gpu={','.join(fields)}",
               "--format=csv,noheader,nounits"]
    try:
        result = subprocess.run(command, check=True, capture_output=True,
                                text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout


def _integer(value, *, minimum=0):
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if number >= minimum else None


def _float(value):
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if 0 <= number <= 100 else None


def _dtypes(compute_capability):
    values = ["fp32", "fp16"]
    try:
        major = int(str(compute_capability).split(".", 1)[0])
    except (TypeError, ValueError):
        major = 0
    if major >= 8:
        values.extend(("bf16", "tf32"))
    return values


def detect_nvidia_gpus():
    """Return bounded GPU inventory using nvidia-smi; return [] if unavailable."""
    binary = shutil.which("nvidia-smi")
    if not binary:
        return []
    fields = ["index", "uuid", "name", "memory.total", "memory.free",
              "utilization.gpu", "compute_cap"]
    output = _query(binary, fields)
    if output is None:
        fields = fields[:-1]
        output = _query(binary, fields)
    if not output:
        return []

    devices = []
    for row in csv.reader(io.StringIO(output), skipinitialspace=True):
        if len(row) not in (6, 7):
            continue
        index = _integer(row[0])
        total = _integer(row[3], minimum=1)
        free = _integer(row[4])
        utilization = _float(row[5])
        name, identifier = row[2].strip(), row[1].strip()
        if (index is None or total is None or free is None or utilization is None
                or not name or not identifier or free > total):
            continue
        capability = row[6].strip() if len(row) == 7 else None
        if capability in ("", "N/A", "[Not Supported]"):
            capability = None
        devices.append({
            "index": index, "uuid": identifier, "name": name[:128],
            "vendor": "nvidia", "backend": "cuda",
            "total_memory_mb": total, "free_memory_mb": free,
            "utilization_pct": utilization,
            "compute_capability": capability[:16] if capability else None,
            "supported_dtypes": _dtypes(capability),
        })
    return devices[:16]


def main():
    parser = argparse.ArgumentParser(description="Detect NVIDIA GPUs visible to this Worker")
    parser.add_argument("--json", action="store_true", help="print JSON inventory")
    parser.parse_args()
    print(json.dumps({"devices": detect_nvidia_gpus()}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
