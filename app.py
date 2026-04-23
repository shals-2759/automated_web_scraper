import streamlit as st
import subprocess
import json
import sys
import os
import tempfile
import pandas as pd
from pathlib import Path

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AI Web Scraper",
    page_icon="🕷️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
.stApp { background: #0a0a0f; color: #e8e8f0; }
h1,h2,h3 { font-family: 'Space Mono', monospace !important; color: #00ff88 !important; }
.main-header { font-family:'Space Mono',monospace; font-size:2.2rem; font-weight:700; color:#00ff88; letter-spacing:-1px; }
.sub-header  { color:#666688; font-size:0.9rem; margin-bottom:1.5rem; font-family:'Space Mono',monospace; }
.info-box { background:#12121f; border:1px solid #1e1e3a; border-left:3px solid #00ff88;
            border-radius:6px; padding:0.9rem 1.1rem; margin:0.6rem 0; font-size:0.85rem; color:#9999bb; }
.warn-box { background:#1f1a0d; border:1px solid #ffaa00; border-left:3px solid #ffaa00;
            border-radius:6px; padding:0.7rem 1rem; margin:0.6rem 0; font-size:0.83rem; color:#ffcc44; }
.status-run { background:#0d1f17; border:1px solid #00ff88; border-radius:6px;
              padding:0.6rem 1rem; color:#00ff88; font-family:'Space Mono',monospace; font-size:0.82rem; }
.status-err { background:#1f0d0d; border:1px solid #ff4455; border-radius:6px;
              padding:0.6rem 1rem; color:#ff4455; font-family:'Space Mono',monospace; font-size:0.82rem; }
.metric-card { background:#12121f; border:1px solid #1e1e3a; border-radius:8px; padding:1rem; text-align:center; }
.metric-val  { font-family:'Space Mono',monospace; font-size:1.7rem; color:#00ff88; font-weight:700; }
.metric-lbl  { color:#666688; font-size:0.76rem; margin-top:0.2rem; }
div[data-testid="stDataFrame"] { border:1px solid #1e1e3a; border-radius:8px; overflow:hidden; }
.stButton>button { background:#00ff88 !important; color:#0a0a0f !important;
    font-family:'Space Mono',monospace !important; font-weight:700 !important;
    border:none !important; border-radius:6px !important; padding:0.6rem 1.5rem !important; }
.stButton>button:hover { background:#00cc6e !important; box-shadow:0 4px 20px rgba(0,255,136,.3) !important; }
.stTextInput>div>div>input, .stTextArea>div>div>textarea {
    background:#12121f !important; border:1px solid #1e1e3a !important;
    border-radius:6px !important; color:#e8e8f0 !important; }
.stTextInput>div>div>input:focus, .stTextArea>div>div>textarea:focus {
    border-color:#00ff88 !important; box-shadow:0 0 0 2px rgba(0,255,136,.15) !important; }
label { color:#9999bb !important; font-size:0.83rem !important;
        font-family:'Space Mono',monospace !important; }
[data-testid="stSidebar"] { background:#0d0d1a !important; border-right:1px solid #1e1e3a; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).parent.resolve()
ENV_PATH = APP_DIR / ".env"


def env_key_status() -> tuple[bool, str]:
    """Return (found, masked_value) from .env file."""
    if not ENV_PATH.exists():
        return False, ""
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("GOOGLE_API_KEY"):
                _, _, val = line.partition("=")
                val = val.strip().strip('"').strip("'")
                if val:
                    return True, val[:8] + "..." + val[-4:]
    return False, ""


def save_env_key(key: str) -> None:
    lines = []
    replaced = False
    if ENV_PATH.exists():
        with open(ENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("GOOGLE_API_KEY"):
                    lines.append(f'GOOGLE_API_KEY="{key}"\n')
                    replaced = True
                else:
                    lines.append(line)
    if not replaced:
        lines.append(f'GOOGLE_API_KEY="{key}"\n')
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.writelines(lines)


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-header">🕷️ AI_SCRAPER</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">// multi-page · gemini · langchain · rate-limit-safe</div>',
            unsafe_allow_html=True)
st.divider()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔑 API Key")
    found, masked = env_key_status()

    if found:
        st.markdown(f"""
        <div class="info-box">
        ✅ Key loaded from <code>.env</code><br>
        <code>{masked}</code>
        </div>""", unsafe_allow_html=True)
        if st.button("🔄 Update key"):
            st.session_state["show_key_input"] = True
    else:
        st.markdown("""
        <div class="warn-box">
        ⚠️ No key in <code>.env</code><br>
        Enter it below to save permanently.
        </div>""", unsafe_allow_html=True)
        st.session_state["show_key_input"] = True

    if st.session_state.get("show_key_input"):
        new_key = st.text_input("GOOGLE_API_KEY", type="password", placeholder="AIza...")
        if st.button("💾 Save to .env") and new_key.strip():
            save_env_key(new_key.strip())
            st.session_state.pop("show_key_input", None)
            st.success("Saved to .env ✓")
            st.rerun()

    st.markdown("---")
    st.markdown("### ⚙️ Pagination")
    max_pages = st.slider("MAX PAGES TO CRAWL", 1, 20, 5)

    st.markdown("---")
    st.markdown("### 📖 How it works")
    st.markdown("""
<div class="info-box">
1. Key loaded from <code>.env</code><br>
2. Fetch page → clean HTML<br>
3. Gemini extracts items (JSON)<br>
4. Detect next page (heuristic→AI)<br>
5. Repeat until max items / pages<br>
6. Rate-limiter prevents 429s<br>
7. Results shown in table
</div>""", unsafe_allow_html=True)

    st.markdown("### 🧰 Stack")
    st.markdown("""
<div class="info-box">
• <b>UI</b>: Streamlit<br>
• <b>HTTP</b>: httpx + BeautifulSoup<br>
• <b>AI</b>: Gemini 2.0 Flash<br>
• <b>Chain</b>: LangChain<br>
• <b>Pagination</b>: heuristic + AI<br>
• <b>Rate limit</b>: token-bucket + back-off
</div>""", unsafe_allow_html=True)

# ── Main form ─────────────────────────────────────────────────────────────────
c1, c2 = st.columns([3, 1])
with c1:
    url = st.text_input("TARGET URL", placeholder="https://books.toscrape.com")
with c2:
    max_items = st.number_input("MAX ITEMS", min_value=1, max_value=500, value=20)

fields_input = st.text_area(
    "FIELDS TO EXTRACT (one per line)",
    placeholder="title\nprice\nrating\navailability",
    height=110,
)

extra_instructions = st.text_area(
    "EXTRA AI INSTRUCTIONS (optional)",
    placeholder="Only extract books with 4+ star rating...",
    height=70,
)

# ── Buttons ───────────────────────────────────────────────────────────────────
st.markdown("")
b1, b2, _ = st.columns([1, 1, 5])
with b1:
    run_btn = st.button("▶ SCRAPE", use_container_width=True)
with b2:
    if st.button("✕ CLEAR", use_container_width=True):
        st.session_state.pop("results", None)
        st.session_state.pop("raw_log", None)
        st.rerun()

# ── Scrape ────────────────────────────────────────────────────────────────────
if run_btn:
    if not url.strip():
        st.error("⚠️ Please enter a target URL.")
        st.stop()
    if not fields_input.strip():
        st.error("⚠️ Please enter at least one field.")
        st.stop()

    # Check API key available (either .env or env-var)
    env_found, _ = env_key_status()
    env_var_key  = os.environ.get("GOOGLE_API_KEY", "")
    if not env_found and not env_var_key:
        st.error("⚠️ No GOOGLE_API_KEY found. Save it using the sidebar first.")
        st.stop()

    fields = [f.strip() for f in fields_input.strip().splitlines() if f.strip()]

    payload = {
        "url": url.strip(),
        "fields": fields,
        "max_items": int(max_items),
        "max_pages": int(max_pages),
        "extra_instructions": extra_instructions.strip(),
        # Don't embed key in payload — worker reads from .env directly
        "api_key": "",
    }

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        json.dump(payload, f)
        payload_path = f.name
    output_path = payload_path.replace(".json", "_output.json")

    scraper_script = str(APP_DIR / "scraper_worker.py")

    st.markdown('<div class="status-run">⏳ Scraper running — watch log below...</div>',
                unsafe_allow_html=True)
    progress = st.progress(0)
    log_box  = st.empty()
    status   = st.empty()

    try:
        proc = subprocess.Popen(
            [sys.executable, scraper_script, payload_path, output_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(APP_DIR),           # so worker finds .env
        )

        log_lines = []
        for i, line in enumerate(proc.stdout):
            log_lines.append(line.rstrip())
            log_box.code("\n".join(log_lines[-25:]), language="bash")
            # progress ticks: advance faster for first 20 lines, then slow
            progress.progress(min(0.92, 0.02 + i * 0.03))

        proc.wait()
        progress.progress(1.0)
        st.session_state["raw_log"] = "\n".join(log_lines)

        if proc.returncode != 0:
            st.markdown('<div class="status-err">✗ Worker exited with error — see log below.</div>',
                        unsafe_allow_html=True)
        else:
            if os.path.exists(output_path):
                with open(output_path, "r", encoding="utf-8") as f:
                    result_data = json.load(f)
                st.session_state["results"] = result_data
                n = len(result_data.get("items", []))
                p = result_data.get("pages_scraped", "?")
                st.success(f"✓ Extracted {n} items across {p} page(s).")
            else:
                st.error("✗ Output file missing. Check the log for errors.")
    except Exception as e:
        st.error(f"✗ Could not launch worker: {e}")
    finally:
        try:
            os.unlink(payload_path)
        except Exception:
            pass

# ── Results ───────────────────────────────────────────────────────────────────
if "results" in st.session_state:
    results = st.session_state["results"]
    items   = results.get("items", [])

    st.markdown("---")
    st.markdown("### 📊 Extracted Data")

    m1, m2, m3, m4 = st.columns(4)
    def metric(col, val, lbl):
        col.markdown(f'<div class="metric-card"><div class="metric-val">{val}</div>'
                     f'<div class="metric-lbl">{lbl}</div></div>', unsafe_allow_html=True)

    metric(m1, len(items),                          "ITEMS EXTRACTED")
    metric(m2, results.get("pages_scraped", "?"),   "PAGES SCRAPED")
    metric(m3, len(items[0]) if items else 0,        "FIELDS / ITEM")
    metric(m4, results.get("model", "gemini"),       "MODEL")

    st.markdown("")

    if items:
        df = pd.DataFrame(items)
        st.dataframe(df, use_container_width=True, height=420)

        dl1, dl2, _ = st.columns([1, 1, 4])
        with dl1:
            st.download_button("⬇ CSV",  df.to_csv(index=False),
                               "scraped.csv",  "text/csv")
        with dl2:
            st.download_button("⬇ JSON",
                               json.dumps(items, indent=2, ensure_ascii=False),
                               "scraped.json", "application/json")
    else:
        st.warning("No items extracted. Try different fields or URL.")

    if results.get("summary"):
        with st.expander("🤖 AI Summary"):
            st.markdown(results["summary"])

# ── Log ───────────────────────────────────────────────────────────────────────
if "raw_log" in st.session_state:
    with st.expander("🪵 Subprocess Log"):
        st.code(st.session_state["raw_log"], language="bash")