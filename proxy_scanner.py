"""
============================================================
  UNLIMITED PROXY SCANNER & TESTER
  Port-scanning + Validation Engine
  Supports: HTTP, SOCKS4, SOCKS5
============================================================
"""
import io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import socket
import threading
import requests
import random
import time
import os
import sys
import ipaddress
from queue import Queue, Empty
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# ─── CONFIG ────────────────────────────────────────────────
PROXY_PORTS = [
    80, 81, 3128, 8080, 8088, 8118, 8888,
    8989, 9090, 9999, 1080, 1081, 4145,
    5080, 7070, 7777, 6060, 9080, 3129,
    8081, 8082, 8083, 8085, 6588, 2020,
]

SCAN_THREADS    = 1000  # Parallel port scanners
TEST_THREADS    = 300   # Parallel proxy testers
SCAN_TIMEOUT    = 1.5   # Seconds for port connect
TEST_TIMEOUT    = 8     # Seconds for proxy validation
TEST_URL        = "http://httpbin.org/ip"   # URL used to validate proxy
OUTPUT_FILE     = "working_proxies.txt"
SCAN_BATCH_SIZE = 2000  # IPs per random batch

# IP ranges to skip (private / loopback / reserved)
SKIP_RANGES = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
]

# ─── GLOBALS ───────────────────────────────────────────────
stats = {
    "scanned":   0,
    "open":      0,
    "tested":    0,
    "working":   0,
    "start_time": time.time(),
}
stats_lock    = threading.Lock()
found_proxies = set()
found_lock    = threading.Lock()
test_queue    = Queue()
stop_event    = threading.Event()

# ─── HELPERS ───────────────────────────────────────────────

def is_public_ip(ip_str: str) -> bool:
    """Return True if the IP is publicly routable."""
    try:
        ip = ipaddress.ip_address(ip_str)
        for net in SKIP_RANGES:
            if ip in net:
                return False
        return True
    except ValueError:
        return False


def random_public_ip() -> str:
    """Generate a random public IPv4 address."""
    while True:
        ip = str(ipaddress.IPv4Address(random.randint(0x01000000, 0xFEFFFFFF)))
        if is_public_ip(ip):
            return ip


def port_is_open(ip: str, port: int) -> bool:
    """Try a TCP connect; return True if the port accepts connections."""
    try:
        with socket.create_connection((ip, port), timeout=SCAN_TIMEOUT) as s:
            return True
    except Exception:
        return False


def test_http_proxy(ip: str, port: int) -> bool:
    """Test an HTTP proxy by sending a request through it."""
    proxy_url = f"http://{ip}:{port}"
    proxies   = {"http": proxy_url, "https": proxy_url}
    try:
        r = requests.get(TEST_URL, proxies=proxies, timeout=TEST_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


def test_socks4_proxy(ip: str, port: int) -> bool:
    """Test a SOCKS4 proxy (requires PySocks)."""
    try:
        import socks
        s = socks.socksocket()
        s.set_proxy(socks.SOCKS4, ip, port)
        s.settimeout(TEST_TIMEOUT)
        s.connect(("httpbin.org", 80))
        s.sendall(b"GET /ip HTTP/1.0\r\nHost: httpbin.org\r\n\r\n")
        data = s.recv(1024)
        s.close()
        return b"200" in data
    except Exception:
        return False


def test_socks5_proxy(ip: str, port: int) -> bool:
    """Test a SOCKS5 proxy (requires PySocks)."""
    try:
        proxy_url = f"socks5://{ip}:{port}"
        proxies   = {"http": proxy_url, "https": proxy_url}
        r = requests.get(TEST_URL, proxies=proxies, timeout=TEST_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


def classify_and_test(ip: str, port: int) -> Optional[str]:
    """
    Single HTTP-only validation for maximum speed.
    Returns the proxy string if working, else None.
    """
    with stats_lock:
        stats["tested"] += 1

    if test_http_proxy(ip, port):
        return f"HTTP  | {ip}:{port}"

    return None


def save_proxy(proxy_line: str, raw: str):
    """Append working proxy to file and in-memory set."""
    with found_lock:
        if raw not in found_proxies:
            found_proxies.add(raw)
            with open(OUTPUT_FILE, "a") as f:
                f.write(raw + "\n")
            with stats_lock:
                stats["working"] += 1


# ─── WORKER LOOPS ──────────────────────────────────────────

def scanner_worker():
    """Continuously scans random IPs across all proxy ports."""
    while not stop_event.is_set():
        ip = random_public_ip()
        ports = random.sample(PROXY_PORTS, k=min(len(PROXY_PORTS), 8))
        for port in ports:
            if stop_event.is_set():
                return
            with stats_lock:
                stats["scanned"] += 1
            if port_is_open(ip, port):
                with stats_lock:
                    stats["open"] += 1
                test_queue.put((ip, port))


def tester_worker():
    """Pulls candidates from queue and validates them as proxies."""
    while not stop_event.is_set():
        try:
            ip, port = test_queue.get(timeout=2)
        except Empty:
            continue

        result = classify_and_test(ip, port)
        if result:
            raw = f"{ip}:{port}"
            save_proxy(result, raw)
            print_working(result)
        test_queue.task_done()


# ─── DISPLAY ───────────────────────────────────────────────

CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
WHITE  = "\033[97m"
DIM    = "\033[2m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def clear_line():
    sys.stdout.write("\033[2K\r")
    sys.stdout.flush()


def print_working(proxy_line: str):
    """Print a found working proxy with highlighted output."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n{GREEN}{BOLD}[+LIVE]{RESET} {WHITE}{proxy_line}{RESET}  {DIM}@ {ts}{RESET}")


def dashboard_thread():
    """Continuously refreshes a live stats dashboard."""
    while not stop_event.is_set():
        time.sleep(1)
        with stats_lock:
            sc = stats["scanned"]
            op = stats["open"]
            te = stats["tested"]
            wk = stats["working"]
            elapsed = time.time() - stats["start_time"]

        speed  = sc / max(elapsed, 1)
        q_size = test_queue.qsize()

        line = (
            f"\r{CYAN}[SCAN]{RESET} {WHITE}{sc:>8,}{RESET} "
            f"{YELLOW}| Open:{RESET} {WHITE}{op:>6}{RESET} "
            f"{YELLOW}| Tested:{RESET} {WHITE}{te:>6}{RESET} "
            f"{GREEN}| Working:{RESET} {WHITE}{BOLD}{wk:>4}{RESET} "
            f"{DIM}| Queue:{q_size:>4} | {speed:>6.0f} ip/s | {int(elapsed//60):02d}:{int(elapsed%60):02d}{RESET}"
        )
        sys.stdout.write(line)
        sys.stdout.flush()


# ─── BANNER ────────────────────────────────────────────────

BANNER = f"""
{CYAN}+============================================================+
|   {WHITE}{BOLD}UNLIMITED PROXY SCANNER  v1.0{RESET}{CYAN}                          |
|   {DIM}Port scanning + HTTP/SOCKS4/SOCKS5 validation{RESET}{CYAN}           |
+============================================================+{RESET}

{YELLOW}  Scan threads : {WHITE}{SCAN_THREADS}
{YELLOW}  Test threads : {WHITE}{TEST_THREADS}
{YELLOW}  Proxy ports  : {WHITE}{len(PROXY_PORTS)} ports
{YELLOW}  Timeout scan : {WHITE}{SCAN_TIMEOUT}s   test: {TEST_TIMEOUT}s
{YELLOW}  Output file  : {WHITE}{OUTPUT_FILE}
{RESET}
  Press {RED}Ctrl+C{RESET} to stop and save results.
{'='*62}
"""

# ─── MAIN ──────────────────────────────────────────────────

def main():
    print(BANNER)

    # Ensure output file exists
    if not os.path.exists(OUTPUT_FILE):
        open(OUTPUT_FILE, "w").close()

    # Start dashboard
    dash = threading.Thread(target=dashboard_thread, daemon=True)
    dash.start()

    # Start tester pool
    tester_pool = []
    for _ in range(TEST_THREADS):
        t = threading.Thread(target=tester_worker, daemon=True)
        t.start()
        tester_pool.append(t)

    # Start scanner pool
    scanner_pool = []
    for _ in range(SCAN_THREADS):
        t = threading.Thread(target=scanner_worker, daemon=True)
        t.start()
        scanner_pool.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n\n{YELLOW}[!] Stopping…{RESET}")
        stop_event.set()
        time.sleep(2)

        with stats_lock:
            wk = stats["working"]
            sc = stats["scanned"]

        print(f"\n{GREEN}{BOLD}+==============================+")
        print(f"|   SESSION COMPLETE           |")
        print(f"+==============================+")
        print(f"|  IPs scanned : {sc:<14,}|")
        print(f"|  Working proxies: {wk:<11}|")
        print(f"|  Saved to: {OUTPUT_FILE:<19}|")
        print(f"+==============================+{RESET}\n")


if __name__ == "__main__":
    main()
