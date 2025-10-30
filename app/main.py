from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from io import BytesIO
import zipfile
import os

from .parser import parse_xml_bytes, write_excel_bytes, build_target_sql, build_pdf_bytes

app = FastAPI(title="Informatica XML → Excel/DDL/PDF")
templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Display upload form"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/process", response_class=StreamingResponse)
async def process(
    request: Request,
    xml_file: UploadFile = File(...),
    brand_name: str = Form("VAAMG Consulting"),
    brand_tagline: str = Form("Agile in Mind. Enterprise in Delivery."),
    brand_hex: str = Form("#8a1e02"),
):
    """Handle Informatica XML upload and return ZIP with Excel, DDL, and PDF"""

    # Read file and get clean base name (without extension)
    xml_bytes = await xml_file.read()
    input_filename = os.path.splitext(xml_file.filename or "mapping")[0]

    # Parse Informatica XML
    tabs, meta = parse_xml_bytes(xml_bytes)

    # Generate output artifacts
    xlsx_bytes = write_excel_bytes(tabs)
    ddl_text = build_target_sql(meta, tabs.get("Target Fields"))
    pdf_bytes = build_pdf_bytes(
        meta,
        tabs,
        brand_name=brand_name,
        brand_tagline=brand_tagline,
        brand_hex=brand_hex,
    )

    # Bundle everything into a ZIP
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{input_filename}.xlsx", xlsx_bytes)
        zf.writestr(f"{input_filename}.sql", ddl_text)
        zf.writestr(f"{input_filename}.pdf", pdf_bytes)

    zip_buf.seek(0)
    headers = {
        "Content-Disposition": f'attachment; filename="{input_filename}.zip"'
    }
    return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)
