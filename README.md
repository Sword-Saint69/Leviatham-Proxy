# 🌐 Leviatham-Proxy — Free Proxy List Auto-Updated Every Hour


[![Proxy Checker](https://github.com/Sword-Saint69/Leviatham-Proxy/actions/workflows/proxy-check.yml/badge.svg)](https://github.com/Sword-Saint69/Leviatham-Proxy/actions/workflows/proxy-check.yml)

Working proxies are fetched from **38 public sources**, tested live, deduplicated, and committed automatically **every hour** via GitHub Actions.

---

## 📥 Get Proxies

**Raw URL** (use directly in your scripts):
```
https://raw.githubusercontent.com/Sword-Saint69/Leviatham-Proxy/main/working_proxies.txt
```

---

## 📂 Files

| File | Description |
|------|-------------|
| `working_proxies.txt` | Verified working proxies — auto-updated every hour |
| `proxy_checker.py` | Fetch + test + save script |
| `proxy_scanner.py` | Additional scanner script |
| `source.txt` | Custom proxy source URLs (one per line) |
| `requirements.txt` | Python dependencies |
| `.github/workflows/proxy-check.yml` | GitHub Actions workflow |

---

## 🚀 Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Full run: fetch from 38 sources + test + save
python proxy_checker.py

# Re-check only existing working_proxies.txt
python proxy_checker.py --recheck
```

---

## ⚙️ Configuration

Edit the top of `proxy_checker.py`:

| Setting | Default | Description |
|---------|---------|-------------|
| `TEST_THREADS` | 500 | Parallel test workers |
| `TEST_TIMEOUT` | 8s | Timeout per proxy test |
| `SOURCE_FILE` | `source.txt` | Your custom source URL list |
| `OUTPUT_FILE` | `working_proxies.txt` | Output file |

Add or remove source URLs in `source.txt` — one URL per line. If empty, the built-in list of 38 sources is used.

---

## 🔧 GitHub Actions Setup

1. Push this repo to GitHub
2. Go to **Settings → Actions → General** → set *Workflow permissions* to **Read and write**
3. The workflow triggers automatically **every hour** at :00
4. Trigger manually anytime via **Actions → Proxy Checker → Run workflow**

---

## 📋 Proxy Format

```
ip:port
```

Supports `HTTP`, `HTTPS`, `SOCKS4`, and `SOCKS5` proxies.

---

## ⭐ Star the repo if it's useful!
