"""
scraper_worker.py  —  Windows-safe, NO Playwright
───────────────────────────────────────────────────
Uses httpx (pure-Python HTTP) + BeautifulSoup to fetch HTML,
then LangChain + Gemini 2.0 Flash to extract structured data.
Zero asyncio subprocess issues because there is no browser process.
 
Called automatically by app.py via subprocess.Popen.
"""
 
import json
import os
import sys
import traceback
 
 
def log(msg: str) -> None:
    print(msg, flush=True)
 
 
# ─────────────────────────────────────────────────────────────────────────────
# 1. Fetch HTML with httpx (sync, no event-loop at all)
# ─────────────────────────────────────────────────────────────────────────────
 
def fetch_html(url: str):
    import httpx
    from bs4 import BeautifulSoup
 
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
 
    log(f"[fetch] GET {url}")
    with httpx.Client(follow_redirects=True, timeout=30, headers=headers) as client:
        resp = client.get(url)
        resp.raise_for_status()
        log(f"[fetch] HTTP {resp.status_code}  ({len(resp.text):,} chars raw)")
 
    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "iframe",
                     "nav", "footer", "header", "meta", "link"]):
        tag.decompose()
 
    cleaned = soup.get_text(separator="\n", strip=True)
    log(f"[fetch] Cleaned text: {len(cleaned):,} chars")
    return cleaned
 
 
# ─────────────────────────────────────────────────────────────────────────────
# 2. LangChain + Gemini extraction chain
# ─────────────────────────────────────────────────────────────────────────────
 
def build_chain(api_key: str):
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import JsonOutputParser
 
    llm = ChatGoogleGenerativeAI(
        model="gemini-3-flash-preview",
        google_api_key=api_key,
        temperature=0.1,
    )
 
    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            (
                "You are an expert data extraction assistant.\n"
                "Given the text content scraped from a webpage, extract structured data as a JSON ARRAY.\n"
                "Each element must be a JSON object containing ONLY the requested fields.\n"
                "Rules:\n"
                "- Return ONLY a valid JSON array. No markdown, no backticks, no explanation.\n"
                "- If a field is missing for an item, use null.\n"
                "- Limit to {max_items} items maximum.\n"
                "- {extra}\n"
            ),
        ),
        (
            "human",
            (
                "Fields to extract: {fields}\n\n"
                "Page content:\n{content}"
            ),
        ),
    ])
 
    parser = JsonOutputParser()
    return prompt | llm | parser
 
 
# ─────────────────────────────────────────────────────────────────────────────
# 3. Optional AI summary
# ─────────────────────────────────────────────────────────────────────────────
 
def generate_summary(items: list, fields: list, api_key: str) -> str:
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
        from langchain_core.prompts import ChatPromptTemplate
 
        llm = ChatGoogleGenerativeAI(
            model="gemini-3-flash-preview",
            google_api_key=api_key,
            temperature=0.3,
        )
        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are a data analyst. Summarise this scraped dataset in 3-4 concise sentences."),
            ("human", "Fields: {fields}\n\nSample (first 5):\n{sample}"),
        ])
        chain = prompt | llm
        resp = chain.invoke({
            "fields": ", ".join(fields),
            "sample": json.dumps(items[:5], indent=2, ensure_ascii=False),
        })
        return resp.content
    except Exception as exc:
        return f"(Summary skipped: {exc})"
 
 
# ─────────────────────────────────────────────────────────────────────────────
# 4. Main (fully synchronous — zero asyncio)
# ─────────────────────────────────────────────────────────────────────────────
 
def main(payload_path: str, output_path: str) -> None:
    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
 
    url: str       = payload["url"]
    fields: list   = payload["fields"]
    max_items: int = payload.get("max_items", 10)
    extra: str     = payload.get("extra_instructions", "") or "No additional instructions."
    api_key: str   = payload["api_key"]
 
    os.environ["GOOGLE_API_KEY"] = api_key
 
    log(f"[worker] URL     : {url}")
    log(f"[worker] Fields  : {fields}")
    log(f"[worker] Max     : {max_items}")
 
    # Step 1 — fetch
    log("[worker] Fetching page with httpx + BeautifulSoup (no Playwright)...")
    text = fetch_html(url)
    content = text[:60000]
 
    # Step 2 — extract
    log("[worker] Building LangChain chain (Gemini 2.0 Flash)...")
    chain = build_chain(api_key)
 
    log("[worker] Invoking Gemini extraction... (may take 10-30 s)")
    raw = chain.invoke({
        "fields": ", ".join(fields),
        "content": content,
        "max_items": max_items,
        "extra": extra,
    })
 
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = next((v for v in raw.values() if isinstance(v, list)), [raw])
    else:
        items = []
 
    items = items[:max_items]
    log(f"[worker] Extracted {len(items)} items.")
 
    log("[worker] Generating summary...")
    summary = generate_summary(items, fields, api_key)
 
    output = {
        "url": url,
        "fields": fields,
        "model": "gemini-3-flash-preview",
        "items": items,
        "summary": summary,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
 
    log(f"[worker] Saved -> {output_path}")
    log("[worker] Done")
 
 
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scraper_worker.py <payload.json> <output.json>")
        sys.exit(1)
 
    try:
        main(sys.argv[1], sys.argv[2])
    except Exception:
        log("[worker] FATAL ERROR:")
        log(traceback.format_exc())
        sys.exit(1)