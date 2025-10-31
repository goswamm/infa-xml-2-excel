import xml.etree.ElementTree as ET
import pandas as pd
from io import BytesIO
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from textwrap import wrap
import re

# =========================
# Utilities
# =========================

def findall(elem, tag):
    return elem.findall(f".//{tag}") if elem is not None else []

def findfirst(elem, tag):
    return elem.find(f".//{tag}") if elem is not None else None

def _empty_tabs():
    return {
        "Overview": pd.DataFrame(columns=["Item", "Value"]),
        "Source Fields": pd.DataFrame(),
        "Target Fields": pd.DataFrame(),
        "Field Lineage": pd.DataFrame(),
        "Transformations": pd.DataFrame(),
        "Connectors": pd.DataFrame(),
        "Reader Settings": pd.DataFrame(),
        "Writer Settings": pd.DataFrame(),
        "Session Attributes": pd.DataFrame(),
    }


# =========================
# XML → DataFrames
# =========================

def parse_xml_bytes(xml_bytes: bytes):
    """
    Parse Informatica PowerCenter XML into dataframes.
    Always returns (tabs: dict[str, DataFrame], meta: dict), even on errors.
    """
    tabs = _empty_tabs()
    meta = {
        "target_name": "TARGET_TABLE",
        "mapping_name": "",
        "workflow_name": "",
        "source_headers": [],
    }

    if not xml_bytes:
        return tabs, meta

    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return tabs, meta

    repo = findfirst(root, "REPOSITORY")
    folder = findfirst(root, "FOLDER")
    mapping = findfirst(root, "MAPPING")
    workflow = findfirst(root, "WORKFLOW")
    session = findfirst(workflow, "SESSION") if workflow is not None else None

    overview = {
        "Repository": repo.get("NAME") if repo is not None else "",
        "Folder": folder.get("NAME") if folder is not None else "",
        "Mapping Name": mapping.get("NAME") if mapping is not None else "",
        "Workflow Name": workflow.get("NAME") if workflow is not None else "",
        "Session Name": session.get("NAME") if session is not None else "",
    }
    tabs["Overview"] = pd.DataFrame(list(overview.items()), columns=["Item", "Value"])

    # Sources
    source_rows = []
    for s in findall(folder, "SOURCE"):
        s_name = s.get("NAME")
        s_type = s.get("DATABASETYPE")
        for sf in findall(s, "SOURCEFIELD"):
            source_rows.append({
                "Source Name": s_name,
                "Source Type": s_type,
                "Field Name": sf.get("NAME"),
                "Datatype": sf.get("DATATYPE"),
                "Length/Precision": sf.get("PRECISION"),
                "Scale": sf.get("SCALE"),
                "Nullable": sf.get("NULLABLE"),
            })
    src_df = pd.DataFrame(source_rows)
    tabs["Source Fields"] = src_df

    # Targets
    target_rows = []
    targets = findall(folder, "TARGET")
    target_name = targets[0].get("NAME") if targets else "TARGET_TABLE"
    for t in targets:
        t_name = t.get("NAME")
        t_type = t.get("DATABASETYPE")
        for tf in findall(t, "TARGETFIELD"):
            target_rows.append({
                "Target Name": t_name,
                "Database": t_type,
                "Column": tf.get("NAME"),
                "Datatype": tf.get("DATATYPE"),
                "Precision": tf.get("PRECISION"),
                "Scale": tf.get("SCALE"),
                "Key Type": tf.get("KEYTYPE"),
                "Nullable": tf.get("NULLABLE"),
            })
    tgt_df = pd.DataFrame(target_rows)
    tabs["Target Fields"] = tgt_df

    # Transformations (ports + notable table attributes)
    trans_rows = []
    if mapping is not None:
        for tr in findall(mapping, "TRANSFORMATION"):
            tr_name = tr.get("NAME")
            tr_type = tr.get("TYPE")
            for tf in findall(tr, "TRANSFORMFIELD"):
                trans_rows.append({
                    "Transformation": tr_name,
                    "Type": tr_type,
                    "Port Name": tf.get("NAME"),
                    "Port Type": tf.get("PORTTYPE"),
                    "Datatype": tf.get("DATATYPE"),
                    "Precision": tf.get("PRECISION"),
                    "Scale": tf.get("SCALE"),
                    "Default": tf.get("DEFAULTVALUE"),
                    "Expression": tf.get("EXPRESSION") if tf.get("EXPRESSION") else "",
                })
            for ta in findall(tr, "TABLEATTRIBUTE"):
                if ta.get("NAME") in ("Lookup Sql Override", "Lookup condition", "Lookup table name"):
                    trans_rows.append({
                        "Transformation": tr_name,
                        "Type": tr_type,
                        "Port Name": ta.get("NAME"),
                        "Port Type": "Attribute",
                        "Datatype": "",
                        "Precision": "",
                        "Scale": "",
                        "Default": "",
                        "Expression": ta.get("VALUE"),
                    })
    trans_df = pd.DataFrame(trans_rows)
    tabs["Transformations"] = trans_df

    # Connectors (order Source → transforms → Target)
    conn_rows = []
    if mapping is not None:
        for c in findall(mapping, "CONNECTOR"):
            conn_rows.append({
                "From Instance": c.get("FROMINSTANCE"),
                "From Type": c.get("FROMINSTANCETYPE"),
                "From Field": c.get("FROMFIELD"),
                "To Instance": c.get("TOINSTANCE"),
                "To Type": c.get("TOINSTANCETYPE"),
                "To Field": c.get("TOFIELD"),
            })
    conn_df = pd.DataFrame(conn_rows)

    if not conn_df.empty:
        type_order = {
            "Source Definition": 0,
            # common transforms in a reasonable flow order
            "Expression": 10,
            "Aggregator": 11,
            "Joiner": 12,
            "Lookup Procedure": 13,
            "Filter": 14,
            "Router": 15,
            "Update Strategy": 16,
            "Sorter": 17,
            "Sequence": 18,
            "Target Definition": 99,
        }

        def _rank(row):
            fr = type_order.get(str(row.get("From Type") or ""), 50)
            to = type_order.get(str(row.get("To Type") or ""), 50)
            return fr * 100 + to

        conn_df["__rank"] = conn_df.apply(_rank, axis=1)
        conn_df = (
            conn_df.sort_values(
                by=["__rank", "From Type", "From Instance", "To Type", "To Instance", "From Field", "To Field"],
                kind="stable"
            )
            .drop(columns="__rank")
            .reset_index(drop=True)
        )

    tabs["Connectors"] = conn_df

    # Field lineage (only links that end at target)
    lineage_rows = []
    for _, row in conn_df.iterrows():
        if str(row.get("To Type")) == "Target Definition":
            lineage_rows.append({
                "Target Table": target_name,
                "Target Column": row.get("To Field"),
                "Comes From Instance": row.get("From Instance"),
                "Comes From Field": row.get("From Field"),
            })
    tabs["Field Lineage"] = pd.DataFrame(lineage_rows)

    # Session attributes
    session_attrs = {}
    if session is not None:
        for attr in findall(session, "ATTRIBUTE"):
            session_attrs[attr.get("NAME")] = attr.get("VALUE")
    tabs["Session Attributes"] = pd.DataFrame([session_attrs]) if session_attrs else pd.DataFrame()

    # Meta
    meta = {
        "target_name": target_name or "TARGET_TABLE",
        "mapping_name": overview.get("Mapping Name", ""),
        "workflow_name": overview.get("Workflow Name", ""),
        "source_headers": list(src_df["Field Name"].unique()) if not src_df.empty else [],
    }
    return tabs, meta


# =========================
# Excel writer
# =========================

def write_excel_bytes(tabs: dict) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as xlw:
        for name, df in tabs.items():
            if df is None or df.empty:
                continue
            df.to_excel(xlw, index=False, sheet_name=name[:31])
    output.seek(0)
    return output.read()


# =========================
# Dialect-aware DDL
# =========================

def map_type_for_db(datatype, precision, scale, db: str):
    """
    Map Informatica field type to a target database type.
    Covers: oracle, sqlserver, postgres, mysql, snowflake.
    """
    dt = (str(datatype) or "").upper()
    p = str(precision) if precision is not None else ""
    s = str(scale) if scale is not None else ""

    def _num():
        if p.isdigit():
            return (p, s) if s.isdigit() else (p, None)
        return (None, None)

    db = (db or "oracle").lower()

    if db == "oracle":
        if dt in ("VARCHAR", "VARCHAR2"):
            return f"VARCHAR2({p})" if p.isdigit() else "VARCHAR2(255)"
        if dt == "CHAR":
            return f"CHAR({p})" if p.isdigit() else "CHAR(1)"
        if dt in ("NUMBER","DECIMAL","NUMERIC","INTEGER","INT","SMALLINT"):
            pp, ss = _num()
            if pp and ss is not None:
                return f"NUMBER({pp},{ss})"
            if pp:
                return f"NUMBER({pp})"
            return "NUMBER"
        if dt == "DATE":
            return "DATE"
        if dt.startswith("TIMESTAMP"):
            return "TIMESTAMP"
        return "VARCHAR2(255)"

    if db == "sqlserver":
        if dt in ("VARCHAR", "VARCHAR2", "NVARCHAR"):
            return f"VARCHAR({p})" if p.isdigit() else "VARCHAR(255)"
        if dt == "CHAR":
            return f"CHAR({p})" if p.isdigit() else "CHAR(1)"
        if dt in ("NUMBER","DECIMAL","NUMERIC"):
            pp, ss = _num()
            if pp and ss is not None:
                return f"DECIMAL({pp},{ss})"
            if pp:
                return f"DECIMAL({pp})"
            return "DECIMAL(38,10)"
        if dt in ("INTEGER","INT","SMALLINT","BIGINT"):
            return "INT"
        if dt == "DATE":
            return "DATE"
        if dt.startswith("TIMESTAMP") or dt in ("DATETIME","SMALLDATETIME"):
            return "DATETIME"
        return "VARCHAR(255)"

    if db == "postgres":
        if dt in ("VARCHAR", "VARCHAR2"):
            return f"VARCHAR({p})" if p.isdigit() else "VARCHAR(255)"
        if dt == "CHAR":
            return f"CHAR({p})" if p.isdigit() else "CHAR(1)"
        if dt in ("NUMBER","DECIMAL","NUMERIC"):
            pp, ss = _num()
            if pp and ss is not None:
                return f"NUMERIC({pp},{ss})"
            if pp:
                return f"NUMERIC({pp})"
            return "NUMERIC"
        if dt in ("INTEGER","INT","SMALLINT","BIGINT"):
            return "INTEGER"
        if dt == "DATE":
            return "DATE"
        if dt.startswith("TIMESTAMP"):
            return "TIMESTAMP"
        return "VARCHAR(255)"

    if db == "mysql":
        if dt in ("VARCHAR", "VARCHAR2"):
            return f"VARCHAR({p})" if p.isdigit() else "VARCHAR(255)"
        if dt == "CHAR":
            return f"CHAR({p})" if p.isdigit() else "CHAR(1)"
        if dt in ("NUMBER","DECIMAL","NUMERIC"):
            pp, ss = _num()
            if pp and ss is not None:
                return f"DECIMAL({pp},{ss})"
            if pp:
                return f"DECIMAL({pp})"
            return "DECIMAL(38,10)"
        if dt in ("INTEGER","INT","SMALLINT","BIGINT"):
            return "INT"
        if dt == "DATE":
            return "DATE"
        if dt.startswith("TIMESTAMP") or dt == "DATETIME":
            return "DATETIME"
        return "VARCHAR(255)"

    if db == "snowflake":
        if dt in ("VARCHAR","VARCHAR2","STRING","TEXT"):
            return f"VARCHAR({p})" if p.isdigit() else "VARCHAR"
        if dt == "CHAR":
            return f"CHAR({p})" if p.isdigit() else "CHAR(1)"
        if dt in ("NUMBER","DECIMAL","NUMERIC"):
            pp, ss = _num()
            if pp and ss is not None:
                return f"NUMBER({pp},{ss})"
            if pp:
                return f"NUMBER({pp})"
            return "NUMBER"
        if dt in ("INTEGER","INT","SMALLINT","BIGINT"):
            return "NUMBER(38,0)"
        if dt == "DATE":
            return "DATE"
        if dt.startswith("TIMESTAMP"):
            return "TIMESTAMP_NTZ"
        return "VARCHAR"

    return "VARCHAR(255)"  # fallback


def build_target_sql(meta: dict, target_df: pd.DataFrame, target_db: str = "oracle") -> str:
    """
    Create dialect-specific DDL with a separate PK constraint.
    """
    tname = meta.get("target_name") or "TARGET_TABLE"
    if target_df is None or target_df.empty:
        return f"-- No target fields found in XML for table {tname}"

    cols, pk_cols = [], []
    for _, r in target_df.iterrows():
        colname = r.get("Column")
        dtype = map_type_for_db(r.get("Datatype"), r.get("Precision"), r.get("Scale"), target_db)
        not_null = " NOT NULL" if (str(r.get("Nullable")).upper() == "NOTNULL") else ""
        cols.append(f"  {colname} {dtype}{not_null}")
        if str(r.get("Key Type")).upper() == "PRIMARY KEY":
            pk_cols.append(colname)

    db = (target_db or "oracle").lower()
    create = [f"CREATE TABLE {tname} (", ",\n".join(cols), ");"]
    if pk_cols:
        if db in ("postgres", "mysql", "sqlserver", "oracle", "snowflake"):
            create.append(f"ALTER TABLE {tname} ADD CONSTRAINT PK_{tname} PRIMARY KEY ({', '.join(pk_cols)});")
        else:
            create.append(f"-- PRIMARY KEY ({', '.join(pk_cols)})")

    return "\n".join(create)


# =========================
# Transformation logic detection
# =========================

def detect_transformation_logic(tabs: dict, max_lines: int = 40):
    """
    Extract readable bullet lines from transformation expressions
    using common function/pattern detection.
    """
    lines = []
    trans_df = tabs.get("Transformations")
    if trans_df is None or trans_df.empty:
        return lines

    patterns = [
        # String
        ("Trim",        r"\bTRIM\s*\("),
        ("Left Trim",   r"\bLTRIM\s*\("),
        ("Right Trim",  r"\bRTRIM\s*\("),
        ("Uppercase",   r"\bUPPER\s*\("),
        ("Lowercase",   r"\bLOWER\s*\("),
        ("Concat",      r"\|\||\bCONCAT\s*\("),
        ("Substring",   r"\bSUBSTR(?:ING)?\s*\("),
        ("Replace",     r"\bREPLACE\s*\("),
        ("Length",      r"\bLENGTH\s*\(|\bLEN\s*\("),
        ("Position",    r"\bCHARINDEX\s*\(|\bPOSITION\s*\(|\bINSTR\s*\("),
        ("Left Pad",    r"\bLPAD\s*\("),
        ("Right Pad",   r"\bRPAD\s*\("),

        # Numeric
        ("Round",       r"\bROUND\s*\("),
        ("Ceiling",     r"\bCEIL(?:ING)?\s*\("),
        ("Floor",       r"\bFLOOR\s*\("),
        ("Absolute",    r"\bABS\s*\("),
        ("Modulo",      r"\bMOD\s*\("),
        ("Power",       r"\bPOWER\s*\("),
        ("Square Root", r"\bSQRT\s*\("),

        # Date/Time
        ("Now",         r"\bGETDATE\s*\(|\bNOW\s*\(|\bCURRENT_TIMESTAMP\b"),
        ("Date Add",    r"\bDATEADD\s*\(|\bDATE_ADD\s*\("),
        ("Date Diff",   r"\bDATEDIFF\s*\(|\bDATE_DIFF\s*\("),
        ("Extract/Datepart", r"\bEXTRACT\s*\(|\bDATEPART\s*\("),
        ("Format/To Char",   r"\bFORMAT\s*\(|\bTO_CHAR\s*\("),

        # Conditional
        ("CASE",        r"\bCASE\b"),
        ("Coalesce",    r"\bCOALESCE\s*\("),
        ("NullIf",      r"\bNULLIF\s*\("),
        ("IsNull/NVL",  r"\bISNULL\s*\(|\bNVL\s*\("),

        # Type conversion
        ("Cast",        r"\bCAST\s*\("),
        ("Convert",     r"\bCONVERT\s*\("),

        # Aggregate & window
        ("Aggregate SUM",   r"\bSUM\s*\("),
        ("Aggregate AVG",   r"\bAVG\s*\("),
        ("Aggregate COUNT", r"\bCOUNT\s*\("),
        ("Aggregate MIN",   r"\bMIN\s*\("),
        ("Aggregate MAX",   r"\bMAX\s*\("),
        ("Row Number",      r"\bROW_NUMBER\s*\("),
        ("Rank",            r"\bRANK\s*\("),
        ("Dense Rank",      r"\bDENSE_RANK\s*\("),
        ("Lag",             r"\bLAG\s*\("),
        ("Lead",            r"\bLEAD\s*\("),

        # Regex (carry-over)
        ("Regex",       r"\bREGEXP_[A-Z_]+\s*\("),
    ]

    for _, row in trans_df.iterrows():
        expr = (row.get("Expression") or "").strip()
        if not expr:
            continue
        tname = row.get("Transformation") or "Transformation"
        pport = row.get("Port Name") or "Port"

        for label, rgx in patterns:
            if re.search(rgx, expr, flags=re.IGNORECASE):
                shown = expr if len(expr) <= 160 else (expr[:157] + "...")
                lines.append(f"• {label}: {tname}.{pport} → {shown}")
                break  # one bullet per expression
        if len(lines) >= max_lines:
            break
    return lines


# =========================
# PDF builder
# =========================

def hex_to_rgb_tuple(hex_color: str):
    hex_color = hex_color.strip().lstrip("#")
    if len(hex_color) == 3:
        hex_color = "".join([c*2 for c in hex_color])
    if len(hex_color) != 6:
        # VAAMG brand default fallback (#8a1e02)
        return (138/255, 30/255, 2/255)
    r = int(hex_color[0:2], 16)/255.0
    g = int(hex_color[2:4], 16)/255.0
    b = int(hex_color[4:6], 16)/255.0
    return (r, g, b)

def build_pdf_bytes(meta: dict, tabs: dict,
                    brand_name="VAAMG Consulting",
                    brand_tagline="Agile in Mind. Enterprise in Delivery.",
                    brand_hex="#8a1e02") -> bytes:
    # Verdana 11pt with fallbacks (Render base image rarely has Verdana)
    matplotlib.rcParams["font.family"] = ["Verdana", "DejaVu Sans", "sans-serif"]
    matplotlib.rcParams["font.size"] = 11

    mapping = meta.get("mapping_name", "")
    workflow = meta.get("workflow_name", "")
    target = meta.get("target_name", "")
    headers = meta.get("source_headers", [])

    tgt_cols = []
    if "Target Fields" in tabs and not tabs["Target Fields"].empty:
        tgt_cols = list(tabs["Target Fields"]["Column"].astype(str).values)

    logic_lines = detect_transformation_logic(tabs, max_lines=40)

    fig = plt.figure(figsize=(8.27, 11.69))  # A4 portrait
    ax = plt.axes([0, 0, 1, 1])
    ax.axis("off")

    brand_rgb = hex_to_rgb_tuple(brand_hex or "#8a1e02")
    header_h = 0.12
    ax.add_patch(Rectangle((0, 1 - header_h), 1, header_h, color=brand_rgb))

    ax.text(0.05, 0.965, brand_name, color="white", fontsize=18, va="top", ha="left", weight="bold")
    ax.text(0.05, 0.935, brand_tagline, color="white", fontsize=11, va="top", ha="left")

    def section_title(y, title):
        ax.text(0.05, y, title, fontsize=13, weight="bold", va="top", ha="left")
        return y - 0.02

    def block_lines(y, items, line_gap=0.02, wrap_at=115):
        for item in items:
            wl = wrap(item, wrap_at) if len(item) > wrap_at else [item]
            for w in wl:
                ax.text(0.05, y, w, fontsize=11, va="top", ha="left")
                y -= line_gap
        return y - 0.01

    y = 0.84
    ax.text(0.05, y, "Informatica Mapping — Business Summary", fontsize=14, weight="bold", va="top", ha="left")
    y -= 0.035

    y = section_title(y, "Overview")
    y = block_lines(y, [
        f"Mapping: {mapping or '(n/a)'}",
        f"Workflow: {workflow or '(n/a)'}",
    ])

    y = section_title(y, "Source & Target")
    y = block_lines(y, [
        f"Source headers: {', '.join(headers) if headers else '(none found)'}",
        f"Target table: {target or '(n/a)'}",
        f"Target columns: {', '.join(tgt_cols) if tgt_cols else '(none found)'}",
    ])

    y = section_title(y, "Detected Transformation Logic")
    if logic_lines:
        y = block_lines(y, logic_lines, line_gap=0.021, wrap_at=115)
    else:
        y = block_lines(y, ["• No specific transformation expressions detected."], line_gap=0.021)

    ax.text(0.05, 0.06, "Auto-generated from Informatica XML", fontsize=9, color="gray", ha="left", va="bottom")
    ax.text(0.95, 0.06, brand_name, fontsize=9, color="gray", ha="right", va="bottom")

    bio = BytesIO()
    fig.savefig(bio, format="pdf", bbox_inches="tight")
    plt.close(fig)
    bio.seek(0)
    return bio.read()
