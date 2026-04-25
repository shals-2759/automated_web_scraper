"""
app.py  --  Flask web server (replaces Streamlit)
Run: python app.py
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import uuid
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_file, session

app = Flask(__name__)
app.secret_key = os.urandom(24)

APP_DIR = Path(__file__).parent.resolve()

# ── In-memory job store (per-process, fine for single-user dev tool) ──────────
jobs: dict[str, dict] = {}   # job_id -> {status, log_lines, output_path, result}


# ─────────────────────────────────────────────────────────────────────────────
# Background thread: runs scraper_worker.py, streams stdout into jobs store
# ─────────────────────────────────────────────────────────────────────────────

def run_scraper(job_id: str, payload: dict, output_path: str) -> None:
    jobs[job_id]["status"] = "running"

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as f:
        json.dump(payload, f)
        payload_path = f.name

    scraper = str(APP_DIR / "scraper_worker.py")
    try:
        proc = subprocess.Popen(
            [sys.executable, scraper, payload_path, output_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(APP_DIR),
        )
        for line in proc.stdout:
            jobs[job_id]["log_lines"].append(line.rstrip())
        proc.wait()

        if proc.returncode == 0 and Path(output_path).exists():
            with open(output_path, encoding="utf-8") as f:
                jobs[job_id]["result"] = json.load(f)
            jobs[job_id]["status"] = "done"
        else:
            jobs[job_id]["status"] = "error"
    except Exception as exc:
        jobs[job_id]["log_lines"].append(f"[flask] FATAL: {exc}")
        jobs[job_id]["status"] = "error"
    finally:
        try:
            os.unlink(payload_path)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    data = request.get_json()
    api_key   = (data.get("api_key") or "").strip()
    url       = (data.get("url") or "").strip()
    fields    = [f.strip() for f in (data.get("fields") or "").splitlines() if f.strip()]
    max_items = int(data.get("max_items") or 20)
    max_pages = int(data.get("max_pages") or 5)
    extra     = (data.get("extra") or "").strip()

    if not api_key:
        return jsonify({"error": "API key is required"}), 400
    if not url:
        return jsonify({"error": "URL is required"}), 400
    if not fields:
        return jsonify({"error": "At least one field is required"}), 400

    job_id = str(uuid.uuid4())
    output_path = str(APP_DIR / "outputs" / f"{job_id}_output.json")
    Path(output_path).parent.mkdir(exist_ok=True)

    payload = {
        "url": url,
        "fields": fields,
        "max_items": max_items,
        "max_pages": max_pages,
        "extra_instructions": extra,
        "api_key": api_key,          # passed directly, NOT stored in .env
    }

    jobs[job_id] = {"status": "queued", "log_lines": [], "result": None,
                    "output_path": output_path}

    t = threading.Thread(target=run_scraper, args=(job_id, payload, output_path), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def job_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "Unknown job"}), 404
    return jsonify({
        "status": job["status"],
        "log": "\n".join(job["log_lines"]),
        "result": job["result"],
    })


@app.route("/api/download/<job_id>")
def download_pdf(job_id):
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "Not ready"}), 404
    result = job["result"]
    pdf_path = str(APP_DIR / "outputs" / f"{job_id}.pdf")
    _generate_pdf(result, pdf_path)
    return send_file(pdf_path, as_attachment=True,
                     download_name="scraped_report.pdf",
                     mimetype="application/pdf")


@app.route("/api/download_json/<job_id>")
def download_json(job_id):
    job = jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "Not ready"}), 404
    result = job["result"]
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json",
                                     mode="w", encoding="utf-8")
    json.dump(result["pages"], tmp, ensure_ascii=False, indent=2)
    tmp.close()
    return send_file(tmp.name, as_attachment=True,
                     download_name="scraped_data.json",
                     mimetype="application/json")


# ─────────────────────────────────────────────────────────────────────────────
# PDF generation with reportlab (no browser needed)
# ─────────────────────────────────────────────────────────────────────────────

def _generate_pdf(result: dict, pdf_path: str) -> None:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.lib import colors
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    HRFlowable, PageBreak)
    from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY

    doc = SimpleDocTemplate(
        pdf_path, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2.5*cm, bottomMargin=2*cm,
    )

    NAVY   = colors.HexColor("#1B2A4A")
    TEAL   = colors.HexColor("#0D7377")
    DARK   = colors.HexColor("#2C3E50")
    GREY   = colors.HexColor("#7F8C8D")
    GREEN  = colors.HexColor("#14A76C")

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("Title", parent=styles["Normal"],
        fontName="Helvetica-Bold", fontSize=22, textColor=NAVY,
        spaceAfter=6, alignment=TA_CENTER)
    subtitle_style = ParagraphStyle("Subtitle", parent=styles["Normal"],
        fontName="Helvetica", fontSize=11, textColor=TEAL,
        spaceAfter=4, alignment=TA_CENTER)
    meta_style = ParagraphStyle("Meta", parent=styles["Normal"],
        fontName="Helvetica", fontSize=9, textColor=GREY,
        spaceAfter=14, alignment=TA_CENTER)
    section_style = ParagraphStyle("Section", parent=styles["Normal"],
        fontName="Helvetica-Bold", fontSize=14, textColor=TEAL,
        spaceBefore=14, spaceAfter=4)
    field_label_style = ParagraphStyle("FieldLabel", parent=styles["Normal"],
        fontName="Helvetica-Bold", fontSize=10, textColor=NAVY, spaceAfter=2)
    field_body_style = ParagraphStyle("FieldBody", parent=styles["Normal"],
        fontName="Helvetica", fontSize=10, textColor=DARK,
        spaceAfter=10, leading=15, alignment=TA_JUSTIFY)
    summary_style = ParagraphStyle("Summary", parent=styles["Normal"],
        fontName="Helvetica-Oblique", fontSize=10, textColor=DARK,
        spaceAfter=8, leading=15, leftIndent=20, rightIndent=20)
    url_style = ParagraphStyle("URL", parent=styles["Normal"],
        fontName="Helvetica", fontSize=8, textColor=GREY, spaceAfter=4)

    story = []

    # Cover
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph("AI Web Scraper", title_style))
    story.append(Paragraph("Extracted Research Report", subtitle_style))
    story.append(Paragraph(
        f"Source: {result.get('url', '')}  |  "
        f"Pages scraped: {result.get('pages_scraped', '?')}  |  "
        f"Model: {result.get('model', 'Gemini')}",
        meta_style))
    story.append(HRFlowable(width="100%", thickness=2, color=TEAL, spaceAfter=16))

    # Summary
    if result.get("summary"):
        story.append(Paragraph("Overview", section_style))
        story.append(HRFlowable(width="100%", thickness=0.5, color=TEAL, spaceAfter=6))
        story.append(Paragraph(result["summary"], summary_style))
        story.append(Spacer(1, 0.4*cm))

    # Per-page content
    pages_data = result.get("pages", [])
    for pg_idx, page in enumerate(pages_data, 1):
        story.append(PageBreak() if pg_idx > 1 else Spacer(1, 0.2*cm))
        story.append(Paragraph(f"Page {pg_idx}: {page.get('url', '')}", section_style))
        story.append(HRFlowable(width="100%", thickness=0.5, color=TEAL, spaceAfter=8))

        for item_idx, item in enumerate(page.get("items", []), 1):
            if len(page.get("items", [])) > 1:
                story.append(Paragraph(f"Entry {item_idx}", ParagraphStyle(
                    "EntryNum", parent=styles["Normal"],
                    fontName="Helvetica-Bold", fontSize=11, textColor=GREEN,
                    spaceBefore=10, spaceAfter=4)))

            for field, value in item.items():
                if value is None:
                    continue
                story.append(Paragraph(str(field).upper(), field_label_style))
                text = str(value).replace("<", "&lt;").replace(">", "&gt;")
                story.append(Paragraph(text, field_body_style))

            story.append(HRFlowable(width="60%", thickness=0.3,
                                    color=colors.HexColor("#BDC3C7"), spaceAfter=6))

    doc.build(story)


if __name__ == "__main__":
    app.run(debug=True, port=5000)