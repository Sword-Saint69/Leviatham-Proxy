# 🌐 Leviatham-Proxy — Live Proxy Intelligence

[![Proxy Checker](https://github.com/Sword-Saint69/Leviatham-Proxy/actions/workflows/proxy-check.yml/badge.svg)](https://github.com/Sword-Saint69/Leviatham-Proxy/actions/workflows/proxy-check.yml)

Working proxies are fetched from **38 public sources**, tested live via 1000 parallel threads, deduplicated, and committed automatically **every hour** via GitHub Actions.

## 📊 Live Dashboard
View live stats, copy, and download the latest proxies directly from the dashboard:  
👉 **[Live Proxy Dashboard](https://Sword-Saint69.github.io/Leviatham-Proxy/)**

---

## 📥 Get Proxies (Raw URLs)

Use these raw links directly in your scripts:

- **Google-Verified HTTP Proxies** (Checked via `google.com/generate_204`)
  ```text
  https://raw.githubusercontent.com/Sword-Saint69/Leviatham-Proxy/main/http/google.txt
  ```
- **HttpBin-Verified HTTP Proxies** (Checked via `httpbin.org/ip`)
  ```text
  https://raw.githubusercontent.com/Sword-Saint69/Leviatham-Proxy/main/http/httpbin.txt
  ```
- **SOCKS5 Proxies**
  ```text
  https://raw.githubusercontent.com/Sword-Saint69/Leviatham-Proxy/main/socks5/working.txt
  ```

---

## 📂 Files

| File/Folder | Description |
|-------------|-------------|
| `http/` | Contains `google.txt` and `httpbin.txt` (Live HTTP proxies) |
| `socks5/` | Contains `working.txt` (Live SOCKS5 proxies) |
| `index.html` | The static HTML dashboard source code |
| `proxy_checker.py` | 1000-thread fetch + test + save script |
| `proxy_scanner.py` | Standalone port scanner script |
| `source.txt` | Custom proxy source URLs (one per line) |
| `requirements.txt` | Python dependencies |

---

## 🚀 Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Full run: fetch from 38 sources + test (1000 threads) + save to 3 files
python proxy_checker.py
```

---

## ⚙️ Configuration

Edit the top of `proxy_checker.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `TEST_THREADS` | 1000 | Parallel test workers |
| `TEST_TIMEOUT` | 8s | Timeout per proxy test |
| `SOURCE_FILE` | `source.txt` | Your custom source URL list |

Add or remove source URLs in `source.txt` — one URL per line. If empty, the built-in list of 38 sources is used.

---

## 🔧 GitHub Actions Setup

1. Push this repo to GitHub
2. Go to **Settings → Actions → General** → set *Workflow permissions* to **Read and write**
3. Go to **Settings → Pages** → build from `main` branch to host the `index.html` dashboard
4. The workflow triggers automatically **every hour** at :00
5. Trigger manually anytime via **Actions → Proxy Checker → Run workflow**

---

## 📋 Proxy Format

```text
ip:port
```

---

## ⭐ Star the repo if it's useful!
