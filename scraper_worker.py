"""
scraper_worker.py
─────────────────
Runs in a SEPARATE subprocess so that Playwright/crawl4ai event-loop
issues on Windows never crash the Streamlit UI process.
 
Usage (called by app.py automatically):
    python scraper_worker.py <payload_json_path> <output_json_path>
"""
 
import asyncio
import json
import os
import sys
import traceback
 
 
# ── Windows event-loop fix (MUST be before any asyncio usage) ─────────────────
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
 
 
def log(msg: str) -> None:
    print(msg, flush=True)
 
 
# ─────────────────────────────────────────────────────────────────────────────
# LangChain + Gemini extraction chain
# ─────────────────────────────────────────────────────────────────────────────
 
def build_extraction_chain(api_key: str):
    """
    Build a LangChain chain:
      PromptTemplate → ChatGoogleGenerativeAI (Gemini) → JsonOutputParser
    """
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import JsonOutputParser
 
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash",
        google_api_key=api_key,
        temperature=0.1,
    )
 
    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            (
                "You are an expert web scraper assistant. "
                "Given raw HTML content, extract structured data as a JSON array. "
                "Each element of the array must be a JSON object containing ONLY the requested fields. "
                "Return ONLY valid JSON — no markdown, no backticks, no explanation.\n\n"
                "If a field is not found for an item, use null. "
                "Limit results to {max_items} items.\n"
                "{extra}"
            ),
        ),
        (
            "human",
            (
                "Fields to extract: {fields}\n\n"
                "HTML Content (truncated to first 80000 chars):\n{html}"
            ),
        ),
    ])
 
    parser = JsonOutputParser()
 
    chain = prompt | llm | parser
    return chain
 
 
# ─────────────────────────────────────────────────────────────────────────────
# crawl4ai scraper
# ─────────────────────────────────────────────────────────────────────────────
 
async def scrape_url(url: str) -> str:
    """Fetch page HTML using crawl4ai."""
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
 
    browser_cfg = BrowserConfig(headless=True, verbose=False)
    run_cfg = CrawlerRunConfig(
        word_count_threshold=10,
        excluded_tags=["script", "style", "nav", "footer", "header"],
        remove_overlay_elements=True,
        page_timeout=30000,
    )
 
    async with AsyncWebCrawler(config=browser_cfg) as crawler:
        result = await crawler.arun(url=url, config=run_cfg)
        if not result.success:
            raise RuntimeError(f"crawl4ai failed: {result.error_message}")
        # Prefer cleaned HTML; fall back to raw HTML
        html = result.cleaned_html or result.html or ""
        log(f"[crawl4ai] Fetched {len(html):,} chars from {url}")
        return html
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Summarise extracted data (optional, uses LangChain too)
# ─────────────────────────────────────────────────────────────────────────────
 
def generate_summary(items: list, fields: list, api_key: str) -> str:
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_core.prompts import ChatPromptTemplate
 
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=api_key,
            temperature=0.3,
        )
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are a data analyst. Summarise the scraped dataset in 3-4 sentences."),
            ("human", "Fields: {fields}\n\nSample data (first 5 items):\n{sample}"),
        ])
        chain = prompt | llm
        resp = chain.invoke({
            "fields": ", ".join(fields),
            "sample": json.dumps(items[:5], indent=2),
        })
        return resp.content
    except Exception as e:
        return f"(Summary generation failed: {e})"
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Main async pipeline
# ─────────────────────────────────────────────────────────────────────────────
 
async def main(payload_path: str, output_path: str) -> None:
    # 1. Load payload
    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
 
    url: str = payload["url"]
    fields: list = payload["fields"]
    max_items: int = payload.get("max_items", 10)
    extra: str = payload.get("extra_instructions", "")
    api_key: str = payload["api_key"]
 
    os.environ["GOOGLE_API_KEY"] = api_key
 
    log(f"[worker] Target URL   : {url}")
    log(f"[worker] Fields       : {fields}")
    log(f"[worker] Max items    : {max_items}")
 
    # 2. Scrape
    log("[worker] Starting crawl4ai scrape...")
    html = await scrape_url(url)
 
    # 3. Build LangChain extraction chain
    log("[worker] Building LangChain + Gemini extraction chain...")
    chain = build_extraction_chain(api_key)
 
    # 4. Run chain
    log("[worker] Invoking Gemini for extraction (may take ~10-30s)...")
    items = chain.invoke({
        "fields": ", ".join(fields),
        "html": html[:80000],          # stay within context window
        "max_items": max_items,
        "extra": extra or "No additional instructions.",
    })
 
    if not isinstance(items, list):
        # Some models wrap in a dict
        if isinstance(items, dict):
            items = items.get("items") or list(items.values())[0] or []
        else:
            items = []
 
    log(f"[worker] Extracted {len(items)} items.")
 
    # 5. Optional summary
    log("[worker] Generating AI summary...")
    summary = generate_summary(items, fields, api_key)
 
    # 6. Write output
    output = {
        "url": url,
        "fields": fields,
        "model": "gemini-2.0-flash",
        "items": items,
        "summary": summary,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
 
    log(f"[worker] Output written to {output_path}")
    log("[worker] Done ✓")
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
 
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scraper_worker.py <payload.json> <output.json>")
        sys.exit(1)
 
    payload_path = sys.argv[1]
    output_path = sys.argv[2]
 
    try:
        asyncio.run(main(payload_path, output_path))
    except Exception:
        log("[worker] FATAL ERROR:")
        log(traceback.format_exc())
        sys.exit(1)