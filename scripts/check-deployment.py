#!/usr/bin/env python3
"""Read-only deployment checks. Does not read .env, change DNS, or restart services."""
import argparse
import json
import re
import subprocess
import urllib.error
import urllib.request
from urllib.parse import urlsplit


def validate_public_url(value):
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
        raise ValueError("public URL must be an HTTPS origin without credentials/query/path")
    return value.rstrip("/")


def command(args):
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=8)
        return result.stdout if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def health(origin):
    try:
        with urllib.request.urlopen(origin.rstrip("/") + "/health", timeout=8) as response:
            data = json.loads(response.read(16384))
        return {"ok": data.get("status") == "ok", "version": data.get("version")}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": "http_error", "status": exc.code}
    except (OSError, ValueError, urllib.error.URLError):
        return {"ok": False, "error": "connection_dns_or_tls_failed"}


def public_dns(host):
    # System/MagicDNS resolution does not establish a public DNS record.
    results = []
    for kind in ("A", "AAAA"):
        output = command(["dig", "+time=3", "+tries=1", "@8.8.8.8", host, kind])
        if output is None:
            results.append("unknown")
        elif re.search(r"status: NXDOMAIN", output):
            results.append("missing")
        elif re.search(r"ANSWER: [1-9]", output):
            results.append("present")
        else:
            results.append("unknown")
    return "present" if "present" in results else "missing" if results == ["missing", "missing"] else "unknown"


def conclusion(local, public, dns):
    if not local["ok"]:
        return "local_service_unhealthy", 2
    if public is None:
        return "local_ok_public_not_checked", 0
    if dns == "missing":
        return "local_ok_public_dns_missing", 3
    if not public["ok"]:
        return "local_ok_public_endpoint_failed", 3
    return "local_and_https_ok_from_this_server", 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="Optional public HTTPS origin; otherwise use current Tailscale self DNS name")
    parser.add_argument("--local-url", default="http://127.0.0.1:8000")
    parser.add_argument("--local-only", action="store_true")
    args = parser.parse_args()
    public_url = validate_public_url(args.url) if args.url else None
    report = {"scope": "checks_from_this_server_only", "local": health(args.local_url)}
    if not args.local_only and not public_url:
        status = command(["tailscale", "status", "--json"])
        try:
            parsed = json.loads(status or "{}")
            report["tailscale"] = parsed.get("BackendState", "unavailable")
            hostname = parsed.get("Self", {}).get("DNSName", "").rstrip(".")
            if hostname and parsed.get("BackendState") == "Running":
                public_url = validate_public_url("https://" + hostname)
                funnel = json.loads(command(["tailscale", "funnel", "status", "--json"]) or "{}")
                report["funnel_for_current_hostname"] = bool(funnel.get("AllowFunnel", {}).get(hostname + ":443"))
        except (ValueError, TypeError, AttributeError):
            report["tailscale"] = "unavailable"
    if public_url and not args.local_only:
        report["public_url"] = public_url
        report["public_dns"] = public_dns(urlsplit(public_url).hostname)
        report["public_health"] = health(public_url)
    diagnosis, code = conclusion(report["local"], report.get("public_health"), report.get("public_dns"))
    report["diagnosis"] = diagnosis
    if not args.local_only and not public_url:
        report["next_step"] = "Supply --url https://your-domain or check Tailscale enrollment; public access was not verified."
        code = code or 3
    if diagnosis == "local_ok_public_dns_missing":
        report["next_step"] = "Check Funnel status and current node hostname. If public DNS remains missing after propagation, contact Tailscale support. Do not disable TLS checks."
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
