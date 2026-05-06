"""
Quick proxy connectivity test. Reads proxy.txt, verifies the URL works
end-to-end, and reports the exit IP plus a sample qolvex reachability check.

Usage:
    python proxy_test.py            # basic test (1 request)
    python proxy_test.py --rotate 5 # show 5 different exit IPs to confirm
                                    # rotation actually rotates
"""
from __future__ import annotations

import argparse
import sys

import requests

import core


def _get_exit_ip(proxies: dict | None, timeout: int = 10) -> tuple[str | None, str]:
    """Return (ip, error_msg)."""
    try:
        r = requests.get(
            "https://api.ipify.org?format=json", proxies=proxies, timeout=timeout
        )
        r.raise_for_status()
        return r.json().get("ip"), ""
    except Exception as e:  # noqa: BLE001
        return None, f"{e.__class__.__name__}: {e}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify proxy.txt configuration.")
    ap.add_argument(
        "--rotate", type=int, default=1,
        help="number of consecutive exit-IP probes (default 1; use >1 to "
             "verify rotation gives different IPs)",
    )
    args = ap.parse_args()

    proxy_url = core.PROXY_URL
    if not proxy_url:
        print(
            "[fail] no proxy configured. Create proxy.txt with one URL line, "
            "e.g.:\n  socks5h://USER:PASS@gw.dataimpulse.com:823"
        )
        return 1

    masked = proxy_url
    if "@" in proxy_url:
        scheme_creds, host = proxy_url.rsplit("@", 1)
        # Hide password but keep scheme + user prefix for readability.
        if ":" in scheme_creds:
            head, _ = scheme_creds.rsplit(":", 1)
            masked = f"{head}:****@{host}"
    print(f"[proxy] using: {masked}")

    proxies = core.get_proxies()

    print(f"\n[step 1] direct (no proxy) exit IP for comparison ...")
    direct_ip, err = _get_exit_ip(None)
    if direct_ip:
        print(f"  direct IP   = {direct_ip}")
    else:
        print(f"  direct lookup failed: {err}")

    print(f"\n[step 2] proxy exit IP probes (n={args.rotate}) ...")
    seen: list[str] = []
    fails = 0
    for i in range(args.rotate):
        ip, err = _get_exit_ip(proxies)
        if ip:
            tag = "NEW" if ip not in seen else "repeat"
            seen.append(ip)
            print(f"  probe {i+1}: {ip}  ({tag})")
        else:
            fails += 1
            print(f"  probe {i+1}: FAILED  {err}")

    if fails == args.rotate:
        print("\n[fail] all probes failed. Check credentials, host, and that "
              "PySocks is installed (pip install -r requirements.txt).")
        return 2

    unique = len(set(seen))
    print(f"\n[summary] proxy probes: ok={len(seen)}/{args.rotate}, "
          f"unique IPs={unique}")
    if args.rotate > 1 and unique == 1:
        print("  WARN: only one unique IP across multiple probes \u2014 either "
              "you're on a sticky session or rotation is slow.")

    print("\n[step 3] qolvex reachability via proxy ...")
    try:
        r = requests.get(
            core.USER_API_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            proxies=proxies,
            timeout=15,
        )
        # 401 is expected without a valid cookie; what matters is that the
        # request reached qolvex and got a response.
        print(f"  qolvex /api/stats/dashboard -> HTTP {r.status_code} "
              f"({len(r.content)} bytes)")
        if r.status_code in (200, 401, 403):
            print("  [ok] qolvex reachable through the proxy.")
        else:
            print(f"  [warn] unexpected status {r.status_code}; check log.")
    except Exception as e:  # noqa: BLE001
        print(f"  [fail] qolvex unreachable through proxy: {e}")
        return 3

    print("\n[done] proxy configuration looks good.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
