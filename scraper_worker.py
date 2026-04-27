"""
scraper_worker.py -- Windows-safe, content-aware multi-page scraper
--------------------------------------------------------------------
Key changes vs previous version:
  1. Navigation: AI returns a RANKED LIST of all relevant URLs to visit upfront
     (not just "next page") -- fixes wrong-link problem on sites like GFG
  2. Output: each page becomes a rich paragraph document (not table rows)
  3. API key: passed directly in payload (user provides it in UI, no .env needed)
"""

import json
import os
import sys
import time
import traceback
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

# ── Force UTF-8 stdout on Windows ─────────────────────────────────────────────
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )


def log(msg: str) -> None:
    safe = msg.encode("utf-8", errors="replace").decode("utf-8")
    print(safe, flush=True)


# ── Blocked sites ─────────────────────────────────────────────────────────────
BLOCKED_DOMAINS = {
    "linkedin.com":  "LinkedIn requires login and actively blocks bots.",
    "facebook.com":  "Facebook requires login.",
    "instagram.com": "Instagram requires login.",
    "twitter.com":   "Twitter/X requires login.",
    "x.com":         "Twitter/X requires login.",
    "google.com":    "Google blocks automated scraping.",
    "amazon.com":    "Amazon has aggressive bot-detection.",
}


def check_blocked(url: str) -> None:
    host = urlparse(url).netloc.lower().lstrip("www.")
    for domain, reason in BLOCKED_DOMAINS.items():
        if host == domain or host.endswith("." + domain):
            raise ValueError(f"BLOCKED SITE: {host} -- {reason}")


# ─────────────────────────────────────────────────────────────────────────────
# 0. Load .env (optional -- API key can also come from payload)
# ─────────────────────────────────────────────────────────────────────────────

def load_env() -> None:
    script_dir = Path(__file__).parent.resolve()
    env_path = script_dir / ".env"
    if not env_path.exists():
        return
    log(f"[env] Loading .env from {env_path}")
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


# ─────────────────────────────────────────────────────────────────────────────
# 1. Rate limiter
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, rpm: int = 12):
        self.min_gap = 60.0 / rpm
        self._last_call = 0.0

    def wait(self) -> None:
        elapsed = time.time() - self._last_call
        wait_for = self.min_gap - elapsed
        if wait_for > 0:
            log(f"[rate] Waiting {wait_for:.1f}s ...")
            time.sleep(wait_for)
        self._last_call = time.time()


_RATE = RateLimiter(rpm=12)


def call_with_backoff(chain, inputs: dict, max_retries: int = 5):
    delay = 15
    for attempt in range(1, max_retries + 1):
        _RATE.wait()
        try:
            return chain.invoke(inputs)
        except Exception as exc:
            msg = str(exc).lower()
            if "429" in msg or "resource_exhausted" in msg or "quota" in msg:
                if attempt == max_retries:
                    raise
                log(f"[rate] Quota hit (attempt {attempt}). Sleeping {delay}s ...")
                time.sleep(delay)
                delay = min(delay * 2, 120)
            else:
                raise
    raise RuntimeError("Exceeded max retries")


# ─────────────────────────────────────────────────────────────────────────────
# 2. HTTP fetch
# ─────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch_page(url: str) -> tuple[str, str]:
    """Returns (cleaned_text, raw_html)."""
    import httpx
    from bs4 import BeautifulSoup

    log(f"[fetch] GET {url}")
    with httpx.Client(follow_redirects=True, timeout=30, headers=HEADERS) as client:
        resp = client.get(url)
        resp.raise_for_status()
        log(f"[fetch] HTTP {resp.status_code}  ({len(resp.text):,} chars)")

    raw_soup = BeautifulSoup(resp.text, "html.parser")   # keep original for link extraction

    clean_soup = BeautifulSoup(resp.text, "html.parser")
    for tag in clean_soup(["script", "style", "noscript", "svg", "iframe",
                            "nav", "footer", "header", "meta", "link"]):
        tag.decompose()

    text = clean_soup.get_text(separator="\n", strip=True)
    log(f"[fetch] Cleaned text {len(text):,} chars")
    return text, str(raw_soup)


# ─────────────────────────────────────────────────────────────────────────────
# 3. URL discovery -- THE KEY FIX
#    Instead of asking "what is the next page?", we ask the AI:
#    "Given the START page and the fields we need, give me ALL the URLs
#    on this site that are likely to contain the requested information,
#    in the order we should visit them."
#    This solves the wrong-link problem completely.
# ─────────────────────────────────────────────────────────────────────────────

def discover_urls_ai(start_url: str, fields: list, raw_html: str,
                     chain_discover, max_pages: int) -> list[str]:
    """
    Ask Gemini to plan the full crawl list from the start page.
    Returns an ordered list of URLs to visit.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(raw_html, "html.parser")
    current_domain = urlparse(start_url).netloc

    # Collect all same-domain links with their anchor text
    seen = set()
    links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(start_url, a["href"].strip())
        if not href.startswith("http"):
            continue
        if urlparse(href).netloc != current_domain:
            continue
        if href in seen or href == start_url:
            continue
        seen.add(href)
        txt = a.get_text(separator=" ", strip=True)[:100]
        links.append(f"{txt}  =>  {href}")

    if not links:
        log("[discover] No same-domain links found on start page.")
        return []

    links_text = "\n".join(links[:200])
    log(f"[discover] Sending {min(len(links),200)} links to AI for URL planning...")

    try:
        raw = call_with_backoff(chain_discover, {
            "start_url": start_url,
            "fields": ", ".join(fields),
            "links": links_text,
            "max_pages": max_pages,
        })
        # Parse the returned JSON array
        if isinstance(raw, list):
            urls = [u for u in raw if isinstance(u, str) and u.startswith("http")]
        else:
            urls = []
        log(f"[discover] AI planned {len(urls)} URLs to visit: {urls}")
        return urls[:max_pages]
    except Exception as exc:
        log(f"[discover] AI URL planning failed: {exc}. Will use heuristic fallback.")
        return []


def heuristic_next(raw_html: str, current_url: str) -> str | None:
    """Fallback: standard pagination heuristic."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(raw_html, "html.parser")
    current_domain = urlparse(current_url).netloc

    # rel=next
    link_next = soup.find("link", rel=lambda r: r and "next" in r)
    if link_next and link_next.get("href"):
        return urljoin(current_url, link_next["href"].strip())

    # anchor text
    NEXT_TEXTS = {"next", "next page", "next article", "next topic",
                  "next post", "next chapter", ">>", "next >>", ">", ">>"}
    for a in soup.find_all("a", href=True):
        txt = a.get_text(separator=" ", strip=True).lower().strip()
        href = a["href"].strip()
        if txt in NEXT_TEXTS and href and not href.startswith("javascript"):
            candidate = urljoin(current_url, href)
            if urlparse(candidate).netloc == current_domain:
                return candidate

    # query string page increment
    parsed = urlparse(current_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    for param in ("page", "p", "pg", "pagenum", "pagenumber"):
        if param in qs:
            try:
                qs[param] = [str(int(qs[param][0]) + 1)]
                return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
            except ValueError:
                pass

    # path /page/N
    m = re.search(r"(.*?/page[/=])(\d+)(.*)", current_url, re.IGNORECASE)
    if m:
        return m.group(1) + str(int(m.group(2)) + 1) + m.group(3)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# 4. LangChain chains
# ─────────────────────────────────────────────────────────────────────────────

def _llm(api_key: str, temp: float = 0.1):
    from langchain_google_genai import ChatGoogleGenerativeAI
    return ChatGoogleGenerativeAI(
        model="gemini-3-flash-preview",
        google_api_key=api_key,
        temperature=temp,
    )


def build_discover_chain(api_key: str):
    """
    Given all links on the start page + the fields the user wants,
    return a JSON array of URLs to visit IN ORDER.
    This is the core fix for wrong navigation.
    """
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import JsonOutputParser

    prompt = ChatPromptTemplate.from_messages([
        ("system", (
            "You are a web crawl planner.\n"
            "The user wants to extract these fields from a website: {fields}\n"
            "You will be given the start URL and a list of all hyperlinks found on that page.\n\n"
            "Your job: identify and return an ordered JSON ARRAY of URLs that are most likely "
            "to contain content matching the requested fields.\n\n"
            "Rules:\n"
            "- Return ONLY a valid JSON array of URL strings, e.g. [\"https://...\", \"https://...\"]\n"
            "- Include the start page's OWN URL first if it contains the data.\n"
            "- Then include any additional pages in logical reading order.\n"
            "- Maximum {max_pages} URLs total.\n"
            "- Stay on the same domain as the start URL.\n"
            "- Do NOT include login pages, contact pages, about pages, or unrelated sections.\n"
            "- If the start page already covers everything, return just: [\"{start_url}\"]\n"
            "- Return NO explanation, NO markdown, ONLY the JSON array.\n"
        )),
        ("human", (
            "Start URL: {start_url}\n\n"
            "All same-domain links on the page:\n{links}"
        )),
    ])
    return prompt | _llm(api_key, temp=0.0) | JsonOutputParser()


def build_extraction_chain(api_key: str):
    """Extract fields from page text as a list of paragraph-friendly dicts."""
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import JsonOutputParser

    prompt = ChatPromptTemplate.from_messages([
        ("system", (
            "You are an expert content extractor.\n"
            "Given webpage text, extract the requested fields and return them as a JSON ARRAY.\n"
            "Each element is a JSON object where:\n"
            "  - keys are exactly the requested field names\n"
            "  - values are FULL, DETAILED text (not truncated, not bullet points)\n"
            "  - values should be complete paragraphs or sentences, not just keywords\n"
            "  - if a field has multiple points (like advantages), write them as a "
            "    flowing paragraph separated by '. ' NOT as a list\n"
            "Rules:\n"
            "  - Return ONLY valid JSON array, no markdown, no backticks.\n"
            "  - Use null only if the field genuinely does not appear on the page.\n"
            "  - {extra}\n"
        )),
        ("human", "Fields to extract: {fields}\n\nPage content:\n{content}"),
    ])
    return prompt | _llm(api_key) | JsonOutputParser()


def build_summary_chain(api_key: str):
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import StrOutputParser

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a research analyst. Write a 3-5 sentence summary of the collected data."),
        ("human", "Fields scraped: {fields}\n\nData collected:\n{content}"),
    ])
    return prompt | _llm(api_key, temp=0.3) | StrOutputParser()


# ─────────────────────────────────────────────────────────────────────────────
# 5. Normalise AI JSON output
# ─────────────────────────────────────────────────────────────────────────────

def normalise(raw) -> list:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if isinstance(raw, dict):
        for v in raw.values():
            if isinstance(v, list):
                return [i for i in v if isinstance(i, dict)]
        return [raw]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# 6. Main
# ─────────────────────────────────────────────────────────────────────────────

def main(payload_path: str, output_path: str) -> None:
    load_env()

    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    start_url: str  = payload["url"]
    fields: list    = payload["fields"]
    max_items: int  = payload.get("max_items", 20)
    max_pages: int  = payload.get("max_pages", 5)
    extra: str      = payload.get("extra_instructions", "") or "No additional instructions."
    api_key: str    = payload.get("api_key") or os.environ.get("GOOGLE_API_KEY", "")

    if not api_key:
        raise ValueError("No API key provided. Enter your Gemini API key in the UI.")

    os.environ["GOOGLE_API_KEY"] = api_key

    log(f"[worker] Start URL : {start_url}")
    log(f"[worker] Fields    : {fields}")
    log(f"[worker] Max items : {max_items}  |  Max pages: {max_pages}")

    check_blocked(start_url)

    log("[worker] Building LangChain chains ...")
    chain_discover = build_discover_chain(api_key)
    chain_extract  = build_extraction_chain(api_key)
    chain_summary  = build_summary_chain(api_key)

    # ── PHASE 1: Fetch start page and plan the full URL list ──────────────────
    log(f"\n[worker] -- Phase 1: Discovering relevant URLs --")
    try:
        start_text, start_html = fetch_page(start_url)
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch start URL: {exc}")

    # Ask AI to plan all URLs to visit based on the links on the start page
    planned_urls = discover_urls_ai(start_url, fields, start_html,
                                    chain_discover, max_pages)

    # If AI couldn't plan, fallback: just use the start URL + heuristic pagination
    if not planned_urls:
        log("[worker] Falling back to start URL only + heuristic pagination.")
        planned_urls = [start_url]

    log(f"[worker] Planned crawl list ({len(planned_urls)} URLs):")
    for i, u in enumerate(planned_urls, 1):
        log(f"  {i}. {u}")

    # ── PHASE 2: Visit each planned URL and extract content ───────────────────
    all_page_results = []
    visited: set = set()
    total_items = 0

    for page_num, url in enumerate(planned_urls, 1):
        if url in visited:
            log(f"[worker] Skipping duplicate URL: {url}")
            continue
        visited.add(url)

        if total_items >= max_items:
            log(f"[worker] Reached max_items ({max_items}). Stopping.")
            break

        log(f"\n[worker] -- Page {page_num}/{len(planned_urls)} --")
        log(f"[worker] URL: {url}")

        # Fetch (reuse start page content if it's the same URL)
        if url == start_url:
            text, raw_html = start_text, start_html
        else:
            try:
                text, raw_html = fetch_page(url)
            except Exception as exc:
                log(f"[fetch] ERROR on {url}: {exc} -- skipping")
                continue

        content = text[:55000]
        remaining = max_items - total_items

        log(f"[worker] Extracting content with Gemini ...")
        try:
            raw = call_with_backoff(chain_extract, {
                "fields": ", ".join(fields),
                "content": content,
                "extra": extra,
            })
            items = normalise(raw)
        except Exception as exc:
            log(f"[extract] ERROR: {exc} -- skipping page")
            continue

        items = items[:remaining]
        log(f"[worker] Extracted {len(items)} item(s) from page {page_num}")

        all_page_results.append({
            "url": url,
            "page_num": page_num,
            "items": items,
        })
        total_items += len(items)
        log(f"[worker] Running total: {total_items} / {max_items}")

    # ── PHASE 3: If planned URLs exhausted but need more, use heuristic ───────
    if total_items < max_items and planned_urls:
        last_url = planned_urls[-1]
        log("\n[worker] -- Phase 3: Heuristic continuation --")
        for extra_page in range(max_pages):
            if total_items >= max_items:
                break
            next_url = heuristic_next(start_html if last_url == start_url else "", last_url)
            if not next_url or next_url in visited:
                break
            visited.add(next_url)
            log(f"[worker] Heuristic next: {next_url}")
            try:
                text, _ = fetch_page(next_url)
            except Exception as exc:
                log(f"[fetch] ERROR: {exc}")
                break
            remaining = max_items - total_items
            try:
                raw = call_with_backoff(chain_extract, {
                    "fields": ", ".join(fields),
                    "content": text[:55000],
                    "extra": extra,
                })
                items = normalise(raw)[:remaining]
            except Exception as exc:
                log(f"[extract] ERROR: {exc}")
                break
            all_page_results.append({"url": next_url, "page_num": len(all_page_results)+1, "items": items})
            total_items += len(items)
            last_url = next_url

    log(f"\n[worker] Total items across all pages: {total_items}")

    # ── PHASE 4: Generate summary ─────────────────────────────────────────────
    log("[worker] Generating summary ...")
    all_text_for_summary = ""
    for pg in all_page_results[:3]:
        for item in pg["items"][:2]:
            for k, v in item.items():
                if v:
                    all_text_for_summary += f"{k}: {v}\n"

    try:
        summary = call_with_backoff(chain_summary, {
            "fields": ", ".join(fields),
            "content": all_text_for_summary[:6000],
        })
    except Exception as exc:
        summary = f"(Summary skipped: {exc})"

    # ── Write output ──────────────────────────────────────────────────────────
    output = {
        "url": start_url,
        "fields": fields,
        "model": "gemini-3-flash-preview",
        "pages_scraped": len(all_page_results),
        "total_items": total_items,
        "pages": all_page_results,         # rich per-page data
        "items": [item for pg in all_page_results for item in pg["items"]],  # flat list
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