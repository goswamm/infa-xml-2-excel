from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from io import BytesIO
import zipfile
import os
import re

from .parser import parse_xml_bytes, write_excel_bytes, build_target_sql, build_pdf_bytes

app = FastAPI(title="Informatica XML → Excel/DDL/PDF")
templates = Jinja2Templates(directory="app/templates")


def _sanitize_filename(name: str, fallback: str = "target") -> str:
    """Keep it filesystem-safe."""
    if not name:
        return fallback
    # Allow letters, numbers, underscore, dash, dot
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._-")
    return safe or fallback


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/process", response_class=StreamingResponse)
async def process(
    request: Request,
    xml_file: UploadFile = File(...),
    brand_name: str = Form("VAAMG Consulting"),
    brand_tagline: str = Form("Agile in Mind. Enterprise in Delivery."),
    brand_hex: str = Form("#8a1e02"),
):
    # Base name from uploaded file (for zip/xlsx/pdf)
    xml_bytes = await xml_file.read()
    input_basename = os.path.splitext(xml_file.filename or "mapping")[0]
    input_basename = _sanitize_filename(input_basename, "mapping")

    # Parse XML
    tabs, meta = parse_xml_bytes(xml_bytes)

    # Outputs
    xlsx_bytes = write_excel_bytes(tabs)
    ddl_text = build_target_sql(meta, tabs.get("Target Fields"))
    pdf_bytes = build_pdf_bytes(
        meta, tabs, brand_name=brand_name, brand_tagline=brand_tagline, brand_hex=brand_hex
    )

    # ✅ SQL filename from target table name (fallback to 'target' or input basename)
    target_name = _sanitize_filename(meta.get("target_name") or "", fallback="target")
    if target_name.lower() == "target_table":  # parser default; use input as nicer fallback
        target_name = input_basename

    # Bundle ZIP
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{input_basename}.xlsx", xlsx_bytes)
        zf.writestr(f"{target_name}.sql", ddl_text or "-- No target found in XML")
        zf.writestr(f"{input_basename}.pdf", pdf_bytes)

    zip_buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="{input_basename}.zip"'}
    return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)
