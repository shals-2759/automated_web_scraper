"""
scraper_worker.py -- Windows-safe, multi-page, rate-limit-aware
----------------------------------------------------------------
Flow:
  1. Load API key from .env (project root)
  2. Fetch page 1 with httpx + BeautifulSoup
  3. AI (Gemini via LangChain) extracts items AND detects next-page URL
  4. Repeat until max_items collected OR no next page found
  5. Rate-limit protection: token-bucket throttle + exponential back-off on 429
  6. Write output JSON
"""

import json
import os
import sys
import time
import traceback
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

# ── Force UTF-8 stdout on Windows (fixes cp1252 UnicodeEncodeError) ──────────
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )


def log(msg: str) -> None:
    # Strip any non-ASCII box-drawing / decorative chars as a second safety net
    safe = msg.encode("utf-8", errors="replace").decode("utf-8")
    print(safe, flush=True)


# ── Sites known to block plain HTTP scrapers ──────────────────────────────────
BLOCKED_DOMAINS = {
    "linkedin.com":    "LinkedIn requires login and actively blocks bots.",
    "facebook.com":    "Facebook requires login and blocks scrapers.",
    "instagram.com":   "Instagram requires login and blocks scrapers.",
    "twitter.com":     "Twitter/X requires login for most content.",
    "x.com":           "Twitter/X requires login for most content.",
    "google.com":      "Google blocks automated scraping of search results.",
    "amazon.com":      "Amazon has aggressive bot-detection.",
}

GOOD_TEST_URLS = [
    "https://books.toscrape.com          -> title, price, rating, availability",
    "https://quotes.toscrape.com         -> quote, author, tags",
    "https://news.ycombinator.com        -> title, points, comments",
    "https://toscrape.com/               -> various demo scrapers",
]


def check_blocked(url: str) -> None:
    """Raise a clear error if the URL is a known login-walled / bot-blocked site."""
    from urllib.parse import urlparse
    host = urlparse(url).netloc.lower().lstrip("www.")
    for domain, reason in BLOCKED_DOMAINS.items():
        if host == domain or host.endswith("." + domain):
            suggestions = "\n  ".join(GOOD_TEST_URLS)
            raise ValueError(
                f"\n\n  BLOCKED SITE: {host}\n"
                f"  Reason : {reason}\n\n"
                f"  This scraper uses plain HTTP (no login, no browser).\n"
                f"  It cannot access content that requires authentication.\n\n"
                f"  Try one of these public test sites instead:\n  {suggestions}\n"
            )


# ─────────────────────────────────────────────────────────────────────────────
# 0. Load .env from the project folder (same dir as this script)
# ─────────────────────────────────────────────────────────────────────────────

def load_env() -> None:
    """Load .env from the directory that contains this script."""
    script_dir = Path(__file__).parent.resolve()
    env_path = script_dir / ".env"
    if not env_path.exists():
        log(f"[env] No .env found at {env_path}  -  relying on os.environ")
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
            if key and key not in os.environ:   # don't override existing env vars
                os.environ[key] = val
                log(f"[env]   SET {key}=***")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Rate-limit guard
#    Gemini free tier: 15 req/min (RPM) and ~1M tokens/min (TPM)
#    We enforce a minimum gap between calls and back off on 429.
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    """Simple token-bucket: at most `rpm` calls per 60 s."""

    def __init__(self, rpm: int = 12):          # stay under 15 rpm
        self.min_gap = 60.0 / rpm               # seconds between calls
        self._last_call = 0.0

    def wait(self) -> None:
        elapsed = time.time() - self._last_call
        wait_for = self._min_gap - elapsed
        if wait_for > 0:
            log(f"[rate] Waiting {wait_for:.1f}s to respect rate limit...")
            time.sleep(wait_for)
        self._last_call = time.time()

    # Property alias so both self.min_gap and self._min_gap work
    @property
    def _min_gap(self):
        return self.min_gap


_RATE = RateLimiter(rpm=12)


def call_with_backoff(chain, inputs: dict, max_retries: int = 5):
    """Invoke a LangChain chain with exponential back-off on rate-limit errors."""
    delay = 15  # start with 15 s on first 429
    for attempt in range(1, max_retries + 1):
        _RATE.wait()
        try:
            return chain.invoke(inputs)
        except Exception as exc:
            msg = str(exc).lower()
            # Gemini returns 429 or "resource_exhausted"
            if "429" in msg or "resource_exhausted" in msg or "quota" in msg:
                if attempt == max_retries:
                    raise
                log(f"[rate] 429 / quota hit (attempt {attempt}). Sleeping {delay}s...")
                time.sleep(delay)
                delay = min(delay * 2, 120)     # cap at 2 min
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
    """Return (cleaned_text, raw_html_str) for the given URL."""
    import httpx
    from bs4 import BeautifulSoup

    log(f"[fetch] GET {url}")
    with httpx.Client(follow_redirects=True, timeout=30, headers=HEADERS) as client:
        resp = client.get(url)
        resp.raise_for_status()
        log(f"[fetch] HTTP {resp.status_code}  ({len(resp.text):,} chars)")

    soup = BeautifulSoup(resp.text, "html.parser")

    # Keep a copy of the raw (but tag-cleaned) html for next-page detection
    raw_soup = BeautifulSoup(resp.text, "html.parser")

    for tag in soup(["script", "style", "noscript", "svg", "iframe",
                     "nav", "footer", "header", "meta", "link"]):
        tag.decompose()

    text = soup.get_text(separator="\n", strip=True)
    log(f"[fetch] Cleaned text {len(text):,} chars")
    return text, str(raw_soup)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Next-page URL detection (heuristic + AI fallback)
# ─────────────────────────────────────────────────────────────────────────────

def detect_next_page_heuristic(raw_html: str, current_url: str) -> str | None:
    """
    Fast heuristic scan for common pagination patterns.
    Uses only top-level imports — no local re-imports (avoids UnboundLocalError).
    Handles both:
      - Paginated listing sites  (?page=2, /page/2, rel=next)
      - Content/tutorial sites   (Next Article / Next Topic buttons)
    """
    from bs4 import BeautifulSoup
    # All urllib.parse names come from the top-level import — no local re-import
    from urllib.parse import parse_qs, urlencode, urlunparse

    soup = BeautifulSoup(raw_html, "html.parser")
    parsed_current = urlparse(current_url)

    # ── 1. <link rel="next"> (most reliable signal) ───────────────────────────
    link_next = soup.find("link", rel=lambda r: r and "next" in r)
    if link_next and link_next.get("href"):
        return urljoin(current_url, link_next["href"].strip())

    # ── 2. <a rel="next"> ─────────────────────────────────────────────────────
    a_rel_next = soup.find("a", rel=lambda r: r and "next" in r)
    if a_rel_next and a_rel_next.get("href"):
        href = a_rel_next["href"].strip()
        if href and not href.startswith("javascript"):
            return urljoin(current_url, href)

    # ── 3. <a> whose visible text clearly means "next page / next article" ────
    NEXT_TEXTS = {
        "next", "next page", "next article", "next topic",
        "next post", "next chapter", "next >>", "next >",
        "next ->" , "»", "›", ">",
    }
    for a in soup.find_all("a", href=True):
        txt = a.get_text(separator=" ", strip=True).lower().strip()
        href = a["href"].strip()
        if txt in NEXT_TEXTS and href and not href.startswith("javascript"):
            candidate = urljoin(current_url, href)
            # Make sure we stay on the same domain
            if urlparse(candidate).netloc == parsed_current.netloc:
                return candidate

    # ── 4. Paginator buttons: aria-label="Next", class contains "next" ────────
    for a in soup.find_all("a", href=True):
        aria = (a.get("aria-label") or "").lower()
        cls  = " ".join(a.get("class") or []).lower()
        href = a["href"].strip()
        if ("next" in aria or cls in ("next", "next-page", "pagination-next",
                                      "page-next", "arr-next")) \
                and href and not href.startswith("javascript"):
            candidate = urljoin(current_url, href)
            if urlparse(candidate).netloc == parsed_current.netloc:
                return candidate

    # ── 5. Query-string pagination: ?page=N, ?p=N, etc. ──────────────────────
    qs = parse_qs(parsed_current.query, keep_blank_values=True)
    for param in ("page", "p", "pg", "pagenum", "pagenumber", "start"):
        if param in qs:
            try:
                next_num = int(qs[param][0]) + 1
                qs[param] = [str(next_num)]
                new_query = urlencode(qs, doseq=True)
                return urlunparse(parsed_current._replace(query=new_query))
            except ValueError:
                pass

    # ── 6. Path-based pagination: /page/2, /page/3 ───────────────────────────
    m = re.search(r"(.*?/page[/=])(\d+)(.*)", current_url, re.IGNORECASE)
    if m:
        return m.group(1) + str(int(m.group(2)) + 1) + m.group(3)

    return None


def detect_next_page_ai(raw_html: str, current_url: str,
                        chain_next, fields: list) -> str | None:
    """
    Ask Gemini to find the next-page URL when heuristics fail.
    Passes the user's fields so the AI understands WHAT we are collecting
    and can follow content-navigation links (Next Article, Next Topic, etc.)
    as well as standard pagination.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(raw_html, "html.parser")
    current_domain = urlparse(current_url).netloc

    # Build a compact link list — same-domain only, deduplicated
    seen = set()
    links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(current_url, a["href"].strip())
        if not href.startswith("http"):
            continue
        if urlparse(href).netloc != current_domain:
            continue
        if href in seen:
            continue
        seen.add(href)
        txt = a.get_text(separator=" ", strip=True)[:80]
        links.append(f"{txt} -> {href}")

    if not links:
        return None

    links_text = "\n".join(links[:150])

    try:
        result = call_with_backoff(chain_next, {
            "current_url": current_url,
            "links": links_text,
            "fields": ", ".join(fields),
        })
        url_out = str(result).strip().strip('"').strip("'")
        if url_out.startswith("http") and urlparse(url_out).netloc == current_domain:
            return url_out
        if url_out.lower() in ("none", "null", "no", ""):
            return None
    except Exception as exc:
        log(f"[next-page-ai] Error: {exc}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 4. LangChain chains
# ─────────────────────────────────────────────────────────────────────────────

def build_extraction_chain(api_key: str):
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
                "Given webpage text, extract structured data as a JSON ARRAY.\n"
                "Each element is a JSON object with ONLY the requested fields.\n"
                "Rules:\n"
                "- Return ONLY a valid JSON array. No markdown, no backticks, no explanation.\n"
                "- If a field is missing for an item, use null.\n"
                "- Extract ALL items present on this page (up to {batch_size}).\n"
                "- {extra}\n"
            ),
        ),
        ("human", "Fields to extract: {fields}\n\nPage content:\n{content}"),
    ])
    return prompt | llm | JsonOutputParser()


def build_next_page_chain(api_key: str):
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import StrOutputParser

    llm = ChatGoogleGenerativeAI(
        model="gemini-3-flash-preview",
        google_api_key=api_key,
        temperature=0.0,
    )
    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            (
                "You are a web navigation expert.\n"
                "The user is scraping data for these fields: {fields}\n"
                "Given the current page URL and all its hyperlinks, find the URL of the "
                "NEXT page that would contain MORE of the same kind of data.\n"
                "This could be:\n"
                "  - A standard pagination link (?page=2, /page/2, 'Next' button)\n"
                "  - A 'Next Article' or 'Next Topic' link on tutorial/blog sites\n"
                "  - Any link that continues the series of content matching the fields\n"
                "Rules:\n"
                "  - Return ONLY the full URL, nothing else.\n"
                "  - If there is no logical next page, return exactly: None\n"
                "  - Do NOT return the current URL.\n"
                "  - Stay on the same domain as the current URL.\n"
            ),
        ),
        (
            "human",
            "Current URL: {current_url}\n\nAll same-domain links on the page:\n{links}",
        ),
    ])
    return prompt | llm | StrOutputParser()


def build_summary_chain(api_key: str):
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import StrOutputParser

    llm = ChatGoogleGenerativeAI(
        model="gemini-3-flash-preview",
        google_api_key=api_key,
        temperature=0.3,
    )
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a data analyst. Summarise this scraped dataset in 3-4 concise sentences."),
        ("human", "Fields: {fields}\n\nSample (first 5 items):\n{sample}"),
    ])
    return prompt | llm | StrOutputParser()


# ─────────────────────────────────────────────────────────────────────────────
# 5. Normalise raw AI output → list of dicts
# ─────────────────────────────────────────────────────────────────────────────

def normalise(raw) -> list:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for v in raw.values():
            if isinstance(v, list):
                return v
        return [raw]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# 6. Main
# ─────────────────────────────────────────────────────────────────────────────

def main(payload_path: str, output_path: str) -> None:
    # ── 6a. Load .env first ──────────────────────────────────────────────────
    load_env()

    # ── 6b. Read payload ─────────────────────────────────────────────────────
    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    start_url: str  = payload["url"]
    fields: list    = payload["fields"]
    max_items: int  = payload.get("max_items", 20)
    extra: str      = payload.get("extra_instructions", "") or "No additional instructions."
    max_pages: int  = payload.get("max_pages", 10)

    # API key: payload overrides .env (for backwards compat with UI)
    api_key: str = payload.get("api_key") or os.environ.get("GOOGLE_API_KEY", "")
    if not api_key:
        raise ValueError(
            "No GOOGLE_API_KEY found. Add it to .env or provide via the UI."
        )
    os.environ["GOOGLE_API_KEY"] = api_key

    log(f"[worker] Start URL  : {start_url}")
    log(f"[worker] Fields     : {fields}")
    log(f"[worker] Max items  : {max_items}")
    log(f"[worker] Max pages  : {max_pages}")

    # ── Reject known login-walled / bot-blocked sites immediately ────────────
    check_blocked(start_url)

    # ── 6c. Build chains (once) ───────────────────────────────────────────────
    log("[worker] Building LangChain chains...")
    chain_extract  = build_extraction_chain(api_key)
    chain_next     = build_next_page_chain(api_key)
    chain_summary  = build_summary_chain(api_key)

    # ── 6d. Multi-page loop ──────────────────────────────────────────────────
    all_items: list = []
    visited: set    = set()
    current_url     = start_url
    page_num        = 0

    while current_url and len(all_items) < max_items and page_num < max_pages:
        if current_url in visited:
            log(f"[worker] Already visited {current_url}  -  stopping.")
            break
        visited.add(current_url)
        page_num += 1
        remaining = max_items - len(all_items)

        log(f"\n[worker] -- Page {page_num} ----------------------------------")
        log(f"[worker] URL: {current_url}")
        log(f"[worker] Need {remaining} more items")

        # Fetch
        try:
            text, raw_html = fetch_page(current_url)
        except Exception as exc:
            log(f"[fetch] ERROR: {exc}")
            break

        content = text[:55000]          # stay well inside Gemini's context

        # Extract items from this page
        log("[worker] Extracting items with Gemini...")
        try:
            raw = call_with_backoff(chain_extract, {
                "fields": ", ".join(fields),
                "content": content,
                "batch_size": min(remaining, 50),
                "extra": extra,
            })
            page_items = normalise(raw)
        except Exception as exc:
            log(f"[extract] ERROR: {exc}")
            break

        log(f"[worker] Got {len(page_items)} items from page {page_num}")
        all_items.extend(page_items)
        log(f"[worker] Running total: {len(all_items)} / {max_items}")

        if len(all_items) >= max_items:
            log("[worker] Reached max_items  -  stopping pagination.")
            break

        # Detect next page
        log("[worker] Detecting next page...")
        next_url = detect_next_page_heuristic(raw_html, current_url)
        if next_url:
            log(f"[worker] Next page (heuristic): {next_url}")
        else:
            log("[worker] Heuristic found nothing - asking Gemini for next page...")
            next_url = detect_next_page_ai(raw_html, current_url, chain_next, fields)
            if next_url:
                log(f"[worker] Next page (AI): {next_url}")
            else:
                log("[worker] No next page found  -  done paginating.")

        current_url = next_url

    # Trim to exact max
    all_items = all_items[:max_items]
    log(f"\n[worker] Total items extracted: {len(all_items)}")

    # ── 6e. Summary (one final AI call) ──────────────────────────────────────
    log("[worker] Generating summary...")
    try:
        summary = call_with_backoff(chain_summary, {
            "fields": ", ".join(fields),
            "sample": json.dumps(all_items[:5], indent=2, ensure_ascii=False),
        })
    except Exception as exc:
        summary = f"(Summary skipped: {exc})"

    # ── 6f. Write output ──────────────────────────────────────────────────────
    output = {
        "url": start_url,
        "fields": fields,
        "model": "gemini-3-flash-preview",
        "pages_scraped": page_num,
        "items": all_items,
        "summary": summary,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log(f"[worker] Saved -> {output_path}")
    log("[worker] ✓ Done")


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