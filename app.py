import streamlit as st
import subprocess
import json
import sys
import os
import tempfile
import pandas as pd
import time
 
# ── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AI Web Scraper",
    page_icon="🕷️",
    layout="wide",
    initial_sidebar_state="expanded",
)
 
# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;600&display=swap');
 
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}
 
.stApp {
    background: #0a0a0f;
    color: #e8e8f0;
}
 
h1, h2, h3 {
    font-family: 'Space Mono', monospace !important;
    color: #00ff88 !important;
}
 
.main-header {
    font-family: 'Space Mono', monospace;
    font-size: 2.4rem;
    font-weight: 700;
    color: #00ff88;
    letter-spacing: -1px;
    margin-bottom: 0.2rem;
}
 
.sub-header {
    color: #666688;
    font-size: 0.95rem;
    margin-bottom: 2rem;
    font-family: 'Space Mono', monospace;
}
 
.info-box {
    background: #12121f;
    border: 1px solid #1e1e3a;
    border-left: 3px solid #00ff88;
    border-radius: 6px;
    padding: 1rem 1.2rem;
    margin: 0.8rem 0;
    font-size: 0.88rem;
    color: #9999bb;
}
 
.status-running {
    background: #0d1f17;
    border: 1px solid #00ff88;
    border-radius: 6px;
    padding: 0.6rem 1rem;
    color: #00ff88;
    font-family: 'Space Mono', monospace;
    font-size: 0.82rem;
}
 
.status-error {
    background: #1f0d0d;
    border: 1px solid #ff4455;
    border-radius: 6px;
    padding: 0.6rem 1rem;
    color: #ff4455;
    font-family: 'Space Mono', monospace;
    font-size: 0.82rem;
}
 
div[data-testid="stDataFrame"] {
    border: 1px solid #1e1e3a;
    border-radius: 8px;
    overflow: hidden;
}
 
.stButton > button {
    background: #00ff88 !important;
    color: #0a0a0f !important;
    font-family: 'Space Mono', monospace !important;
    font-weight: 700 !important;
    border: none !important;
    border-radius: 6px !important;
    padding: 0.6rem 2rem !important;
    font-size: 0.9rem !important;
    letter-spacing: 0.5px !important;
    transition: all 0.2s !important;
}
 
.stButton > button:hover {
    background: #00cc6e !important;
    transform: translateY(-1px);
    box-shadow: 0 4px 20px rgba(0,255,136,0.3) !important;
}
 
.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    background: #12121f !important;
    border: 1px solid #1e1e3a !important;
    border-radius: 6px !important;
    color: #e8e8f0 !important;
    font-family: 'DM Sans', sans-serif !important;
}
 
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus {
    border-color: #00ff88 !important;
    box-shadow: 0 0 0 2px rgba(0,255,136,0.15) !important;
}
 
label {
    color: #9999bb !important;
    font-size: 0.85rem !important;
    font-family: 'Space Mono', monospace !important;
    letter-spacing: 0.3px;
}
 
.metric-card {
    background: #12121f;
    border: 1px solid #1e1e3a;
    border-radius: 8px;
    padding: 1rem;
    text-align: center;
}
 
.metric-value {
    font-family: 'Space Mono', monospace;
    font-size: 1.8rem;
    color: #00ff88;
    font-weight: 700;
}
 
.metric-label {
    color: #666688;
    font-size: 0.78rem;
    margin-top: 0.2rem;
}
 
[data-testid="stSidebar"] {
    background: #0d0d1a !important;
    border-right: 1px solid #1e1e3a;
}
</style>
""", unsafe_allow_html=True)
 
# ── Header ────────────────────────────────────────────────────────────────────
st.markdown('<div class="main-header">🕷️ AI_SCRAPER</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">// crawl4ai + gemini + langchain — subprocess-isolated</div>', unsafe_allow_html=True)
st.divider()
 
# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Configuration")
 
    api_key = st.text_input(
        "GOOGLE_API_KEY",
        type="password",
        placeholder="AIza...",
        help="Your Google Gemini API key",
    )
 
    st.markdown("---")
    st.markdown("### 📖 How it works")
    st.markdown("""
<div class="info-box">
1. Enter target URL<br>
2. Specify fields to extract<br>
3. Scraper runs in subprocess (Windows-safe)<br>
4. Gemini AI parses HTML → JSON<br>
5. LangChain orchestrates the pipeline<br>
6. Results shown in table
</div>
""", unsafe_allow_html=True)
 
    st.markdown("### 🧰 Stack")
    st.markdown("""
<div class="info-box">
• <b>UI</b>: Streamlit<br>
• <b>Scraper</b>: crawl4ai<br>
• <b>AI</b>: Gemini 2.0 Flash<br>
• <b>Chain</b>: LangChain<br>
• <b>Isolation</b>: subprocess
</div>
""", unsafe_allow_html=True)
 
# ── Main Form ─────────────────────────────────────────────────────────────────
col1, col2 = st.columns([3, 2])
 
with col1:
    url = st.text_input(
        "TARGET URL",
        placeholder="https://books.toscrape.com",
        help="The website URL to scrape",
    )
 
with col2:
    max_items = st.number_input("MAX ITEMS", min_value=1, max_value=100, value=10)
 
fields_input = st.text_area(
    "FIELDS TO EXTRACT (one per line)",
    placeholder="title\nprice\nrating\navailability",
    height=120,
    help="Each line = one field name you want extracted from the page",
)
 
extra_instructions = st.text_area(
    "EXTRA AI INSTRUCTIONS (optional)",
    placeholder="Extract only books with rating 4 or above...",
    height=80,
)
 
# ── Run Button ────────────────────────────────────────────────────────────────
st.markdown("")
run_col, clear_col = st.columns([1, 5])
 
with run_col:
    run_btn = st.button("▶ SCRAPE", use_container_width=True)
 
with clear_col:
    if st.button("✕ CLEAR", use_container_width=False):
        st.session_state.pop("results", None)
        st.session_state.pop("raw_log", None)
        st.rerun()
 
# ── Scraping Logic ────────────────────────────────────────────────────────────
if run_btn:
    # Validation
    if not api_key:
        st.error("⚠️  Please enter your Google API key in the sidebar.")
        st.stop()
    if not url.strip():
        st.error("⚠️  Please enter a target URL.")
        st.stop()
    if not fields_input.strip():
        st.error("⚠️  Please enter at least one field to extract.")
        st.stop()
 
    fields = [f.strip() for f in fields_input.strip().splitlines() if f.strip()]
 
    payload = {
        "url": url.strip(),
        "fields": fields,
        "max_items": int(max_items),
        "extra_instructions": extra_instructions.strip(),
        "api_key": api_key.strip(),
    }
 
    # Write payload to a temp file
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        json.dump(payload, f)
        payload_path = f.name
 
    output_path = payload_path.replace(".json", "_output.json")
 
    scraper_script = os.path.join(os.path.dirname(__file__), "scraper_worker.py")
 
    st.markdown(
        '<div class="status-running">⏳ Launching subprocess scraper...</div>',
        unsafe_allow_html=True,
    )
    progress = st.progress(0)
    log_box = st.empty()
 
    try:
        proc = subprocess.Popen(
            [sys.executable, scraper_script, payload_path, output_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
 
        log_lines = []
        for i, line in enumerate(proc.stdout):
            log_lines.append(line.rstrip())
            log_box.code("\n".join(log_lines[-20:]), language="bash")
            progress.progress(min(0.9, i * 0.05))
 
        proc.wait()
        progress.progress(1.0)
 
        st.session_state["raw_log"] = "\n".join(log_lines)
 
        if proc.returncode != 0:
            st.markdown(
                '<div class="status-error">✗ Subprocess exited with error. See log below.</div>',
                unsafe_allow_html=True,
            )
        else:
            if os.path.exists(output_path):
                with open(output_path, "r", encoding="utf-8") as f:
                    result_data = json.load(f)
                st.session_state["results"] = result_data
                st.success(f"✓ Scraped {len(result_data.get('items', []))} items successfully!")
            else:
                st.error("✗ Output file not found. Scraping may have failed.")
    except Exception as e:
        st.error(f"✗ Failed to launch scraper: {e}")
    finally:
        try:
            os.unlink(payload_path)
        except Exception:
            pass
 
# ── Results Display ───────────────────────────────────────────────────────────
if "results" in st.session_state:
    results = st.session_state["results"]
    items = results.get("items", [])
 
    st.markdown("---")
    st.markdown("### 📊 Extracted Data")
 
    # Metrics
    m1, m2, m3 = st.columns(3)
    with m1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{len(items)}</div>
            <div class="metric-label">ITEMS EXTRACTED</div>
        </div>""", unsafe_allow_html=True)
    with m2:
        fields_found = len(items[0].keys()) if items else 0
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{fields_found}</div>
            <div class="metric-label">FIELDS PER ITEM</div>
        </div>""", unsafe_allow_html=True)
    with m3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{results.get('model', 'gemini')}</div>
            <div class="metric-label">AI MODEL</div>
        </div>""", unsafe_allow_html=True)
 
    st.markdown("")
 
    if items:
        df = pd.DataFrame(items)
        st.dataframe(df, use_container_width=True, height=400)
 
        # Download
        csv = df.to_csv(index=False)
        st.download_button(
            "⬇ Download CSV",
            data=csv,
            file_name="scraped_data.csv",
            mime="text/csv",
        )
 
        st.download_button(
            "⬇ Download JSON",
            data=json.dumps(items, indent=2, ensure_ascii=False),
            file_name="scraped_data.json",
            mime="application/json",
        )
    else:
        st.warning("No items extracted. Try adjusting your fields or URL.")
 
    # Raw AI summary
    if results.get("summary"):
        with st.expander("🤖 AI Summary"):
            st.markdown(results["summary"])
 
# ── Log expander ──────────────────────────────────────────────────────────────
if "raw_log" in st.session_state:
    with st.expander("🪵 Subprocess Log"):
        st.code(st.session_state["raw_log"], language="bash")