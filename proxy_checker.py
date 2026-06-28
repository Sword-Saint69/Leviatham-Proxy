"""
============================================================
  PROXY FETCHER + TESTER  (CI Edition)
  Fetch -> Dedup -> Test -> Save -> Recheck
  GitHub Actions compatible (no color in CI)
============================================================
"""
import io, sys, os

# Detect CI environment (GitHub Actions sets CI=true)
IS_CI = os.environ.get("CI", "").lower() == "true"

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import re
import time
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── CONFIG ────────────────────────────────────────────────
SOURCE_FILE  = "source.txt"
OUTPUT_FILE  = "working_proxies.txt"
TEST_THREADS = 500
TEST_TIMEOUT = 8        # seconds per proxy test
FETCH_TIMEOUT= 15       # seconds to fetch a source URL

# Multiple test URLs — rotate to avoid rate-limiting
TEST_URLS = [
    "http://www.google.com/generate_204",   # Returns 204, ultra-reliable
    "http://example.com",
    "http://httpbin.org/ip",
]

# ─── COLORS (disabled in CI) ───────────────────────────────
if IS_CI:
    CYAN = GREEN = YELLOW = RED = WHITE = DIM = RESET = BOLD = ""
else:
    CYAN   = "\033[96m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    RED    = "\033[91m"
    WHITE  = "\033[97m"
    DIM    = "\033[2m"
    RESET  = "\033[0m"
    BOLD   = "\033[1m"

# ─── BUILT-IN SOURCE LIST ──────────────────────────────────
BUILTIN_SOURCES = [
    "https://proxylist.geonode.com/api/proxy-list?page=1&limit=500&sort_by=responseTime&sort_type=asc",
    "https://raw.githubusercontent.com/elliottophellia/proxylist/refs/heads/master/results/http/global/http_checked.txt",
    "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/all.txt",
    "https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/ALL_PROXY.txt",
    "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt",
    "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt",
    "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/refs/heads/main/https-proxy-list-by-EbraSha.txt",
    "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/refs/heads/main/http-proxy-list-by-EbraSha.txt",
    "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/refs/heads/main/socks5-proxy-list-by-EbraSha.txt",
    "https://raw.githubusercontent.com/ebrasha/abdal-proxy-hub/refs/heads/main/socks4-proxy-list-by-EbraSha.txt",
    "https://raw.githubusercontent.com/MrRabbitson/RabbitProxyZ-proxy-list/refs/heads/main/lite.txt",
    "https://raw.githubusercontent.com/MrRabbitson/RabbitProxyZ-proxy-list/refs/heads/main/sub.txt",
    "https://raw.githubusercontent.com/MrRabbitson/RabbitProxyZ-proxy-list/refs/heads/main/whitelists.txt",
    "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/all/data.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/https.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt",
    "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt",
    "https://raw.githubusercontent.com/stormsia/proxy-list/main/working_proxies.txt",
    "https://raw.githubusercontent.com/CelestialBrain/worldpool/refs/heads/main/proxies/all.txt",
    "https://raw.githubusercontent.com/alphaa1111/proxyscraper/refs/heads/main/proxies/http.txt",
    "https://raw.githubusercontent.com/alphaa1111/proxyscraper/refs/heads/main/proxies/socks.txt",
    "https://raw.githubusercontent.com/MrMarble/proxy-list/refs/heads/main/all.txt",
    "https://raw.githubusercontent.com/vmheaven/VMHeaven.io-Free-Proxy-List/refs/heads/main/all_proxies.txt",
    "https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/checked_proxies/http.txt",
    "https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/checked_proxies/socks4.txt",
    "https://raw.githubusercontent.com/dinoz0rg/proxy-list/main/checked_proxies/socks5.txt",
    "https://raw.githubusercontent.com/Skillter/ProxyGather/refs/heads/master/proxies/working-proxies-all.txt",
    "https://raw.githubusercontent.com/trio666/proxy-checker/refs/heads/main/all.txt",
    "https://raw.githubusercontent.com/Bes-js/public-proxy-list/refs/heads/main/proxies.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
    "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt",
    "https://raw.githubusercontent.com/Vann-Dev/proxy-list/refs/heads/main/proxies/http.txt",
    "https://raw.githubusercontent.com/Vann-Dev/proxy-list/refs/heads/main/proxies/https.txt",
    "https://vakhov.github.io/fresh-proxy-list/proxylist.txt",
    "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/http.txt",
]

# ─── GLOBALS ───────────────────────────────────────────────
stats = {
    "fetched": 0, "total": 0, "tested": 0,
    "working": 0, "failed": 0, "start": time.time(),
}
stats_lock    = threading.Lock()
found_proxies = set()
found_lock    = threading.Lock()
stop_event    = threading.Event()

PROXY_RE = re.compile(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[:\s](\d{2,5})\b")

# ─── FETCH ─────────────────────────────────────────────────

def load_sources():
    sources = []
    if os.path.exists(SOURCE_FILE):
        with open(SOURCE_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line.startswith("http"):
                    sources.append(line)
    if not sources:
        print(f"[!] source.txt empty — using built-in list ({len(BUILTIN_SOURCES)} sources)")
        sources = BUILTIN_SOURCES
    return sources


def normalize_url(url: str) -> str:
    """
    Convert github.com/.../raw/... web URLs to raw.githubusercontent.com URLs.
    e.g. https://github.com/user/repo/raw/refs/heads/main/file.txt
      -> https://raw.githubusercontent.com/user/repo/refs/heads/main/file.txt
    """
    # Pattern: github.com/{user}/{repo}/raw/{rest}
    m = re.match(
        r"https://github\.com/([^/]+/[^/]+)/raw/(.*)", url
    )
    if m:
        return f"https://raw.githubusercontent.com/{m.group(1)}/{m.group(2)}"
    return url


def fetch_geonode(url):
    """Fetch from geonode JSON API with pagination."""
    proxies, page = [], 1
    headers = {"User-Agent": "Mozilla/5.0 ProxyChecker/3.0"}
    while True:
        paged = re.sub(r"page=\d+", f"page={page}", url)
        try:
            r    = requests.get(paged, timeout=FETCH_TIMEOUT, headers=headers,
                                allow_redirects=True)
            data = r.json()
            # Geonode wraps proxies in 'data' list
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                ip   = item.get("ip", "")
                port = item.get("port", "")
                if ip and port:
                    proxies.append(f"{ip}:{port}")
            if len(items) < int(data.get("limit", 500)):
                break
            page += 1
        except Exception:
            break
    return proxies


def parse_text_proxies(text: str) -> list:
    """Extract ip:port from plain-text content."""
    proxies = []
    for line in text.splitlines():
        line = line.strip()
        # Skip comments, HTML tags, and blank lines
        if not line or line.startswith(("#", "<", "!", "//")):
            continue
        # Skip lines that look like HTML
        if "<html" in line.lower() or "<!doctype" in line.lower():
            return []   # whole response is HTML, bail out
        m = PROXY_RE.search(line)
        if m:
            proxies.append(f"{m.group(1)}:{m.group(2)}")
    return proxies


def fetch_source(url):
    """Fetch proxies from a single source. Returns list of 'ip:port' strings."""
    proxies = []
    err_tag = "0"
    try:
        # Fix GitHub web URLs -> raw URLs
        url = normalize_url(url)

        if "geonode.com/api" in url:
            return fetch_geonode(url), None

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/plain,*/*",
        }
        r = requests.get(
            url, timeout=FETCH_TIMEOUT, headers=headers,
            allow_redirects=True,
        )

        if r.status_code != 200:
            return [], f"HTTP{r.status_code}"

        # Detect content type
        ct = r.headers.get("Content-Type", "")

        # Try JSON first
        if "json" in ct or r.text.lstrip().startswith(("[", "{")):
            try:
                data = r.json()
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, str):
                            m = PROXY_RE.search(item)
                            if m:
                                proxies.append(f"{m.group(1)}:{m.group(2)}")
                        elif isinstance(item, dict):
                            ip   = item.get("ip") or item.get("host", "")
                            port = item.get("port", "")
                            if ip and port:
                                proxies.append(f"{ip}:{port}")
                elif isinstance(data, dict):
                    pool = (
                        data.get("data")
                        or data.get("proxies")
                        or data.get("list")
                        or []
                    )
                    for item in pool:
                        ip   = item.get("ip") or item.get("host", "")
                        port = item.get("port", "")
                        if ip and port:
                            proxies.append(f"{ip}:{port}")
                if proxies:
                    return proxies, None
            except Exception:
                pass

        # Plain text fallback
        proxies = parse_text_proxies(r.text)
        return proxies, None

    except requests.exceptions.ConnectionError:
        return [], "CONN_ERR"
    except requests.exceptions.Timeout:
        return [], "TIMEOUT"
    except Exception as e:
        return [], f"ERR({type(e).__name__})"


def fetch_all_sources(sources):
    all_proxies = []
    print(f"\n[*] Fetching from {len(sources)} sources...")
    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(fetch_source, url): url for url in sources}
        for i, future in enumerate(as_completed(futures), 1):
            url = futures[future]
            short = url[url.rfind("/")+1:][:50]
            try:
                result, err = future.result()
                count = len(result)
                all_proxies.extend(result)
                if err:
                    tag = f"[{err}]"
                elif count == 0:
                    tag = "[EMPTY]"
                else:
                    tag = f"+{count}"
            except Exception as e:
                count, tag = 0, f"[EXC:{type(e).__name__}]"
            with stats_lock:
                stats["fetched"] += count
            print(f"  [{i:>2}/{len(sources)}] {tag:<12} {short}")
    unique = list(set(all_proxies))
    with stats_lock:
        stats["total"] = len(unique)
    print(f"\n[+] Collected {len(all_proxies):,} -> {len(unique):,} unique after dedup")
    return unique

# ─── TEST ──────────────────────────────────────────────────

def _try_url(proxy_url, test_url):
    try:
        r = requests.get(
            test_url,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=TEST_TIMEOUT,
            allow_redirects=True,
        )
        return r.status_code in (200, 204)
    except Exception:
        return False


def test_http(ip, port):
    url = f"http://{ip}:{port}"
    for test_url in TEST_URLS:
        if _try_url(url, test_url):
            return True
    return False


def test_socks5(ip, port):
    url = f"socks5://{ip}:{port}"
    for test_url in TEST_URLS:
        if _try_url(url, test_url):
            return True
    return False


def test_socks4(ip, port):
    url = f"socks4://{ip}:{port}"
    for test_url in TEST_URLS:
        if _try_url(url, test_url):
            return True
    return False


def test_proxy(proxy):
    with stats_lock:
        stats["tested"] += 1
    try:
        ip, port_str = proxy.rsplit(":", 1)
        port = int(port_str)
    except Exception:
        with stats_lock:
            stats["failed"] += 1
        return proxy, None

    socks_ports = {1080, 1081, 4145, 9050, 9150}

    if port not in socks_ports:
        if test_http(ip, port):
            return proxy, "HTTP"

    if test_socks5(ip, port):
        return proxy, "SOCKS5"

    if test_socks4(ip, port):
        return proxy, "SOCKS4"

    if port in socks_ports and test_http(ip, port):
        return proxy, "HTTP"

    with stats_lock:
        stats["failed"] += 1
    return proxy, None


def save_working(proxy, proto):
    with found_lock:
        if proxy not in found_proxies:
            found_proxies.add(proxy)
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(proxy + "\n")
            with stats_lock:
                stats["working"] += 1
            return True
    return False

# ─── DASHBOARD ─────────────────────────────────────────────

def live_dashboard(total):
    while not stop_event.is_set():
        time.sleep(2 if IS_CI else 0.5)
        with stats_lock:
            tested  = stats["tested"]
            working = stats["working"]
            failed  = stats["failed"]
            elapsed = time.time() - stats["start"]
        pct   = tested / max(total, 1) * 100
        speed = tested / max(elapsed, 1)
        bar   = "#" * int(30 * tested / max(total,1)) + "-" * (30 - int(30 * tested / max(total,1)))
        line  = (
            f"\r[{bar}] {pct:>5.1f}% "
            f"| Tested:{tested:>6} | Working:{working:>4} "
            f"| Failed:{failed:>6} | {speed:>5.0f}/s | "
            f"{int(elapsed//60):02d}:{int(elapsed%60):02d}"
        )
        sys.stdout.write(line)
        sys.stdout.flush()

# ─── MAIN ──────────────────────────────────────────────────

def main():
    print(f"""
+============================================================+
|   PROXY FETCHER + TESTER  v3.0  (CI Edition)              |
|   Fetch -> Dedup -> 500-thread test -> Save                |
+============================================================+
  Sources : {SOURCE_FILE}
  Output  : {OUTPUT_FILE}
  Threads : {TEST_THREADS}
  Timeout : {TEST_TIMEOUT}s per proxy
  CI mode : {IS_CI}
""")

    sources = load_sources()
    print(f"[*] Sources: {len(sources)}")

    stats["start"] = time.time()
    proxies = fetch_all_sources(sources)

    if not proxies:
        print("[!] No proxies fetched.")
        sys.exit(1)

    # Fresh output file with metadata header
    total = len(proxies)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"# Updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                f" | Source count: {len(sources)} | Total tested: {total}\n")

    print(f"\n[*] Testing {total:,} proxies with {TEST_THREADS} threads...")
    if not IS_CI:
        print("    Press Ctrl+C to stop early.\n")

    dash = threading.Thread(target=live_dashboard, args=(total,), daemon=True)
    dash.start()

    try:
        with ThreadPoolExecutor(max_workers=TEST_THREADS) as ex:
            futures = {ex.submit(test_proxy, p): p for p in proxies}
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
                try:
                    proxy, proto = future.result()
                    if proto:
                        is_new = save_working(proxy, proto)
                        if is_new and not IS_CI:
                            ts = datetime.now().strftime("%H:%M:%S")
                            sys.stdout.write(f"\n[LIVE] {proto:<6} {proxy:<25} @ {ts}\n")
                            sys.stdout.flush()
                except Exception:
                    pass
    except KeyboardInterrupt:
        stop_event.set()
        print("\n\n[!] Interrupted.")

    stop_event.set()
    time.sleep(0.6)

    with stats_lock:
        elapsed = time.time() - stats["start"]
        working = stats["working"]
        tested  = stats["tested"]

    print(f"""

+==============================+
|   DONE                       |
+==============================+
|  Total    : {total:<11,}     |
|  Tested   : {tested:<11,}     |
|  Working  : {working:<11}     |
|  Time     : {int(elapsed//60):02d}m {int(elapsed%60):02d}s            |
|  Output   : {OUTPUT_FILE:<11}     |
+==============================+
""")
    # Exit non-zero if nothing found (fail the Action)
    if working == 0:
        print("[!] WARNING: 0 working proxies found.")
        sys.exit(0)   # Still exit 0 so Action doesn't fail on bad proxies


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Proxy Fetcher + Tester v3.0")
    parser.add_argument(
        "--recheck", action="store_true",
        help="Only dedup + recheck existing working_proxies.txt"
    )
    args = parser.parse_args()

    if args.recheck:
        print("[*] RECHECK MODE — re-testing existing working_proxies.txt")
        if not os.path.exists(OUTPUT_FILE):
            print("[!] File not found.")
            sys.exit(1)
        raw = []
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    raw.append(line)
        unique = list(dict.fromkeys(raw))
        print(f"[*] {len(raw)} entries -> {len(unique)} unique (removed {len(raw)-len(unique)} dupes)")

        alive = []
        lock  = threading.Lock()

        def _rc(proxy):
            _, proto = test_proxy(proxy)
            return proxy, proto

        stats["start"] = time.time()
        dash = threading.Thread(target=live_dashboard, args=(len(unique),), daemon=True)
        dash.start()

        with ThreadPoolExecutor(max_workers=TEST_THREADS) as ex:
            futures = {ex.submit(_rc, p): p for p in unique}
            for future in as_completed(futures):
                proxy, proto = future.result()
                if proto:
                    with lock:
                        alive.append(proxy)

        stop_event.set()
        time.sleep(0.6)

        alive = list(dict.fromkeys(alive))
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(f"# Rechecked: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                    f" | Alive: {len(alive)}\n")
            for p in alive:
                f.write(p + "\n")

        print(f"\n\n[DONE] Alive: {len(alive)} / {len(unique)} -> {OUTPUT_FILE}")
    else:
        main()
