import os
import asyncio
import json
import requests  # We use this to bypass the buggy SDK
from dotenv import load_dotenv
from crawl4ai import AsyncWebCrawler

load_dotenv()
async def extract_structured_data(url, fields):
    # 1. Scrape (The Eyes)
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=url)
        markdown_content = result.markdown[:8000]

    # 2. Extract (The Brain)
    api_key = os.getenv("GOOGLE_API_KEY")

    # We use 'gemini-1.5-flash' as it is the most stable for free keys
    api_url = f"https://googleapis.com{api_key}"

    payload = {
        "contents": [{
            "parts": [{"text": f"Extract a JSON list with fields {fields} from: {markdown_content}. Return ONLY JSON."}]
        }]
    }

    try:
        response = requests.post(api_url, json=payload)
        res_data = response.json()

        # Check if the API returned an error
        if "error" in res_data:
            print(f"❌ Google API Error: {res_data['error']['message']}")
            return None

        # Extract text from the nested Google response
        ai_text = res_data['candidates'][0]['content']['parts'][0]['text']
        clean_json = ai_text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"❌ Extraction Error: {e}")
        return None


if __name__ == "__main__":
    print("🤖 Testing Direct Connection...")
    data = asyncio.run(extract_structured_data("https://ycombinator.com", ["Title"]))
    if data:
        print("✅ SUCCESS! Data Captured:")
        print(data[:3])
