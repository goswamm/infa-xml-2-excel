from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from io import BytesIO
import zipfile

from .parser import parse_xml_bytes, write_excel_bytes, build_target_sql, build_pdf_bytes

app = FastAPI(title="Informatica XML → Excel/DDL/PDF")
templates = Jinja2Templates(directory="app/templates")

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
    xml_bytes = await xml_file.read()
    tabs, meta = parse_xml_bytes(xml_bytes)

    xlsx_bytes = write_excel_bytes(tabs)
    ddl_text = build_target_sql(meta, tabs.get("Target Fields"))
    pdf_bytes = build_pdf_bytes(
        meta, tabs, brand_name=brand_name, brand_tagline=brand_tagline, brand_hex=brand_hex
    )

    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Business_Summary.xlsx", xlsx_bytes)
        zf.writestr(f"{meta.get('target_name') or 'target'}.sql", ddl_text)
        zf.writestr("Mapping_Summary_VAAMG.pdf", pdf_bytes)

    zip_buf.seek(0)
    headers = { "Content-Disposition": 'attachment; filename="infa_mapping_outputs.zip"' }
    return StreamingResponse(zip_buf, media_type="application/zip", headers=headers)
