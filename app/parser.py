import xml.etree.ElementTree as ET
import pandas as pd
from io import BytesIO
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from textwrap import wrap

def findall(elem, tag):
    return elem.findall(f".//{tag}") if elem is not None else []

def findfirst(elem, tag):
    return elem.find(f".//{tag}") if elem is not None else None

def parse_xml_bytes(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)

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

    # Sources
    source_rows = []
    sources = findall(folder, "SOURCE")
    for s in sources:
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
    source_df = pd.DataFrame(source_rows)

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
    target_df = pd.DataFrame(target_rows)

    # Transformations
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
                if ta.get("NAME") in ("Lookup Sql Override","Lookup condition","Lookup table name"):
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

    # Connectors
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

    # Lineage (simple: to target columns)
    lineage_rows = []
    for _, row in conn_df.iterrows():
        if row.get("To Type") == "Target Definition":
            lineage_rows.append({
                "Target Table": target_name,
                "Target Column": row.get("To Field"),
                "Comes From Instance": row.get("From Instance"),
                "Comes From Field": row.get("From Field"),
            })
    lineage_df = pd.DataFrame(lineage_rows)

    # Session attributes
    session_attrs = {}
    if session is not None:
        for attr in findall(session, "ATTRIBUTE"):
            k = attr.get("NAME")
            v = attr.get("VALUE")
            session_attrs[k] = v
    session_attrs_df = pd.DataFrame([session_attrs]) if session_attrs else pd.DataFrame()

    overview_df = pd.DataFrame(list(overview.items()), columns=["Item", "Value"])

    tabs = {
        "Overview": overview_df,
        "Source Fields": source_df,
        "Target Fields": target_df,
        "Field Lineage": lineage_df,
        "Transformations": trans_df,
        "Connectors": conn_df,
        "Reader Settings": pd.DataFrame(),  # placeholders
        "Writer Settings": pd.DataFrame(),
        "Session Attributes": session_attrs_df,
    }

    meta = {
        "target_name": target_name,
        "mapping_name": overview["Mapping Name"],
        "workflow_name": overview["Workflow Name"],
        "source_headers": list(source_df["Field Name"].unique()) if not source_df.empty else []
    }
    return tabs, meta

def write_excel_bytes(tabs: dict) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as xlw:
        for name, df in tabs.items():
            if df is None or df.empty:
                continue
            sheet_name = name[:31]
            df.to_excel(xlw, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.read()

def oracle_type(datatype, precision, scale):
    dt = (str(datatype) or "").upper()
    precision = str(precision) if precision is not None else ""
    scale = str(scale) if scale is not None else ""
    if dt in ("VARCHAR", "VARCHAR2"):
        if precision.isdigit():
            return f"VARCHAR2({precision})"
        return "VARCHAR2(255)"
    if dt == "CHAR":
        if precision.isdigit():
            return f"CHAR({precision})"
        return "CHAR(1)"
    if dt in ("NUMBER","DECIMAL","NUMERIC","INTEGER","INT","SMALLINT"):
        if precision.isdigit():
            if scale.isdigit():
                return f"NUMBER({precision},{scale})"
            return f"NUMBER({precision})"
        return "NUMBER"
    if dt == "DATE":
        return "DATE"
    if dt.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    return "VARCHAR2(255)"

def build_target_sql(meta: dict, target_df: pd.DataFrame) -> str:
    tname = meta.get("target_name") or "TARGET_TABLE"
    cols = []
    pk_cols = []
    if target_df is None or target_df.empty:
        return f"/* No target found in XML; create your table manually: {tname} */"
    for _, r in target_df.iterrows():
        colname = r.get("Column")
        dtype = oracle_type(r.get("Datatype"), r.get("Precision"), r.get("Scale"))
        nullable = "" if (str(r.get("Nullable")).upper() == "NOTNULL") else " NULL"
        cols.append(f"  {colname} {dtype}{nullable}")
        if str(r.get("Key Type")).upper() == "PRIMARY KEY":
            pk_cols.append(colname)
    lines = [f"CREATE TABLE {tname} (", ",\n".join(cols), ");"]
    if pk_cols:
        lines.append(f"ALTER TABLE {tname} ADD CONSTRAINT PK_{tname} PRIMARY KEY ({', '.join(pk_cols)});")
    return "\n".join(lines)

def hex_to_rgb_tuple(hex_color: str):
    hex_color = hex_color.strip().lstrip("#")
    if len(hex_color) == 3:
        hex_color = "".join([c*2 for c in hex_color])
    if len(hex_color) != 6:
        return (138/255, 30/255, 2/255)  # VAAMG default
    r = int(hex_color[0:2], 16)/255.0
    g = int(hex_color[2:4], 16)/255.0
    b = int(hex_color[4:6], 16)/255.0
    return (r, g, b)

def build_pdf_bytes(meta: dict, tabs: dict,
                    brand_name="VAAMG Consulting",
                    brand_tagline="Agile in Mind. Enterprise in Delivery.",
                    brand_hex="#8a1e02") -> bytes:
    mapping = meta.get("mapping_name", "")
    workflow = meta.get("workflow_name", "")
    target = meta.get("target_name", "")
    headers = meta.get("source_headers", [])
    tgt_cols = []
    if "Target Fields" in tabs and not tabs["Target Fields"].empty:
        tgt_cols = list(tabs["Target Fields"]["Column"].astype(str).values)

    fig = plt.figure(figsize=(8.27, 11.69))
    ax = plt.axes([0,0,1,1])
    ax.axis('off')

    brand_rgb = hex_to_rgb_tuple(brand_hex or "#8a1e02")
    header_height = 0.12
    ax.add_patch(Rectangle((0, 1-header_height), 1, header_height, color=brand_rgb))

    ax.text(0.05, 0.965, brand_name, color="white", fontsize=20, va='top', ha='left', weight='bold')
    ax.text(0.05, 0.935, brand_tagline, color="white", fontsize=11, va='top', ha='left')

    ax.text(0.05, 0.84, "Informatica Mapping – Business Summary", fontsize=16, weight='bold', ha='left', va='top')

    overview_lines = [
        f"Mapping: {mapping}",
        f"Workflow: {workflow}",
        f"Source headers: {', '.join(headers) if headers else '(none found)'}",
        f"Target table: {target}",
        f"Target columns: {', '.join(tgt_cols)}" if tgt_cols else "Target columns: (none found)",
        "Business highlights:",
        " • Trims key identifiers prior to load (if defined in expressions)",
        " • Integration ID derivations and HR lookups (if present in XML)",
        " • Typical load settings: bulk insert / truncate-before-load (if set in session attributes)",
    ]
    wrapped = []
    for ln in overview_lines:
        if len(ln) > 110:
            wrapped.extend(wrap(ln, 110))
        else:
            wrapped.append(ln)
    body_text = "\n".join(wrapped)
    ax.text(0.05, 0.80, body_text, fontsize=11, va='top', ha='left')

    ax.text(0.05, 0.06, "Auto-generated from Informatica XML", fontsize=9, color="gray", ha='left', va='bottom')
    ax.text(0.95, 0.06, brand_name, fontsize=9, color="gray", ha='right', va='bottom')

    bio = BytesIO()
    fig.savefig(bio, format="pdf", bbox_inches="tight")
    plt.close(fig)
    bio.seek(0)
    return bio.read()
