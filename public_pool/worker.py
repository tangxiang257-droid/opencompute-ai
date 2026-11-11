"""Standalone OpenCompute public Worker. CPU mode needs Python 3.9 or later."""
import argparse
import json
import os
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


MATRIX_SIZE = 48
API = "/v0.8/public"


def matrix_parameters(seed):
    values = bytes.fromhex(seed)
    return values[0] % 30 + 1, values[1], values[2] % 30 + 1, values[3]


def cpu_checksum(seed):
    a, b, c, d = matrix_parameters(seed)
    n = MATRIX_SIZE
    left = [[((i * n + k) * a + b) % 31 for k in range(n)] for i in range(n)]
    columns = [[((k * n + j) * c + d) % 31 for k in range(n)] for j in range(n)]
    return sum(sum(x * y for x, y in zip(row, col)) for row in left for col in columns)


def cuda_checksum(seed, device):
    import torch

    a, b, c, d = matrix_parameters(seed)
    grid = torch.arange(MATRIX_SIZE**2, device=device, dtype=torch.int32)
    left = ((grid * a + b) % 31).reshape(MATRIX_SIZE, MATRIX_SIZE).float()
    right = ((grid * c + d) % 31).reshape(MATRIX_SIZE, MATRIX_SIZE).float()
    return int((left @ right).sum(dtype=torch.float64).item())


def memory_mb():
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("length", wintypes.DWORD), ("load", wintypes.DWORD),
                ("physical_total", ctypes.c_uint64), ("physical_free", ctypes.c_uint64),
                ("page_total", ctypes.c_uint64), ("page_free", ctypes.c_uint64),
                ("virtual_total", ctypes.c_uint64), ("virtual_free", ctypes.c_uint64),
                ("extended_virtual_free", ctypes.c_uint64),
            ]

        status = MemoryStatus()
        status.length = ctypes.sizeof(status)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise RuntimeError("Windows could not report physical memory")
        return status.physical_total // 1024**2
    if sys.platform == "darwin":
        import subprocess

        return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], timeout=3)) // 1024**2
    return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") // 1024**2


def validate_url(raw):
    url = urlsplit(raw)
    if (not url.hostname or url.username is not None or url.password is not None
            or url.path not in ("", "/") or url.query or url.fragment):
        raise ValueError("Use only the OpenCompute site origin, such as https://example.com")
    if url.scheme != "https" and not (
        url.scheme == "http" and url.hostname in ("localhost", "127.0.0.1", "::1")
    ):
        raise ValueError("Public connections require HTTPS")
    return f"{url.scheme}://{url.netloc}"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def run(origin, resource):
    resources = {"cores": os.cpu_count() or 1, "ram_mb": memory_mb(), "vram_mb": 0}
    checksum = cpu_checksum
    if resource == "cuda":
        import torch

        if not torch.version.cuda or not torch.cuda.is_available():
            raise RuntimeError("CUDA mode needs NVIDIA GPU access and CUDA-enabled PyTorch")
        device = torch.device("cuda:0")
        resources["vram_mb"] = torch.cuda.get_device_properties(device).total_memory // 1024**2
        def checksum(seed):
            return cuda_checksum(seed, device)

    opener = build_opener(NoRedirect())

    def post(path, body, token=None):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = Request(origin + API + path, json.dumps(body).encode(), headers)
        with opener.open(request, timeout=10) as response:
            return json.loads(response.read(4097))

    while True:
        try:
            challenge = post("/challenge", {"resource": resource})
            worker = post("/join", {"challenge": challenge, "resources": resources,
                                    "checksum": checksum(challenge["seed"])})
        except HTTPError as exc:
            if exc.code != 429:
                try:
                    message = json.loads(exc.read(4096)).get("detail", "Request rejected")
                except (ValueError, AttributeError):
                    message = "Request rejected"
                raise RuntimeError(f"Admission failed (HTTP {exc.code}): {message}") from None
            print("Admission is busy. Retrying in 20 seconds.", flush=True)
            time.sleep(20)
            continue
        except OSError:
            print("Unable to reach OpenCompute. Retrying in 20 seconds.", flush=True)
            time.sleep(20)
            continue

        print(f"Admitted to the public {resource.upper()} pool. Keep this window open.", flush=True)
        seed = worker["seed"]
        while True:
            time.sleep(worker["heartbeat_seconds"])
            try:
                reply = post("/heartbeat", {"node_id": worker["node_id"], "seed": seed,
                                            "checksum": checksum(seed)}, worker["token"])
            except HTTPError as exc:
                if exc.code == 429:
                    continue
                if exc.code in (401, 409):
                    print("Public session expired. Reconnecting.", flush=True)
                    break
                raise RuntimeError(f"Public Worker stopped (HTTP {exc.code}).") from None
            except OSError:
                print("Connection interrupted. Retrying.", flush=True)
                continue
            seed = reply["seed"]
            print("Verification accepted.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="OpenCompute website origin")
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cpu")
    args = parser.parse_args()
    try:
        run(validate_url(args.url), args.device)
    except KeyboardInterrupt:
        print("\nWorker stopped.")
    except (ImportError, ValueError, RuntimeError, OSError) as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
