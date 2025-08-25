from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, PlainTextResponse, JSONResponse
import pandas as pd
import io
import re
from typing import Dict, List, Tuple

app = FastAPI(title="Cruce Inventario API", version="1.0")

# Conjuntos de storages (según lo que nos pediste)
CBP_STORAGES = {"OLR", "DR", "BN", "RT", "RP", "MOV", "COL"}  # CBP
BUNKER_IN_SAP = {"AER"}                                      # BUNKER: SAP AER

# ---- utilidades ---------------------------------------------------------------

def _lower(s: str) -> str:
    return re.sub(r"\s+", "", s or "").lower()

def find_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """
    Localiza una columna por nombres candidatos (ignorando mayúsculas/espacios).
    Lanza 400 si no encuentra.
    """
    norm = {_lower(c): c for c in df.columns}
    for wanted in candidates:
        lw = _lower(wanted)
        if lw in norm:
            return norm[lw]
        # búsqueda aproximada
        for k in norm:
            if lw in k:
                return norm[k]
    raise HTTPException(
        status_code=400,
        detail=f"No pude detectar columna. Necesito una de: {candidates}. Columnas recibidas: {list(df.columns)}",
    )

def read_any_excel(file: UploadFile) -> pd.DataFrame:
    """
    Lee .xlsx con openpyxl y .xls con xlrd 1.2.0.
    Devuelve el primer sheet como DataFrame.
    """
    content = file.file.read()
    file.file.seek(0)
    bio = io.BytesIO(content)

    name = (file.filename or "").lower()
    if name.endswith(".xlsx"):
        engine = "openpyxl"
    elif name.endswith(".xls"):
        engine = "xlrd"        # <= importante para .xls
    else:
        raise HTTPException(400, f"Formato no soportado: {file.filename}")

    try:
        df = pd.read_excel(bio, engine=engine, dtype=str)
    except Exception as e:
        raise HTTPException(400, f"No pude leer {file.filename} ({engine}). Error: {e}")
    return df

def norm_code(code: str) -> str:
    """Normaliza el código: str, sin espacios y sin ceros a la izquierda típicos."""
    if code is None:
        return ""
    s = str(code).strip()
    # a veces vienen como 0000000MX04554A -> MX04554A
    s = re.sub(r"^0+([A-Za-z].*)$", r"\1", s)
    s = re.sub(r"^0+(\d.*)$", r"\1", s)
    return s

def parse_qty(x) -> int:
    """
    Convierte cantidades con formato español: 1.545,000 -> 1545
    y deja 0 para vacíos o no números.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0
    s = str(x).strip()
    if s == "":
        return 0
    # quitar separador de miles ".", cambiar "," por "."
    s = s.replace(".", "").replace(",", ".")
    try:
        return int(round(float(s)))
    except Exception:
        # último intento (por si ya era entero)
        try:
            return int(float(x))
        except Exception:
            return 0

def group_sap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta columnas en SAP y devuelve pivot por (codigo, storage)
    sumando 'BUM Quantity'.
    """
    c_code   = find_col(df, ["Material"])
    c_desc   = find_col(df, ["Material Description"])
    c_store  = find_col(df, ["Storage Location"])
    c_qty    = find_col(df, ["BUM Quantity", "Quantity", "Qty"])

    out = df[[c_code, c_desc, c_store, c_qty]].copy()
    out[c_code]  = out[c_code].map(norm_code)
    out[c_desc]  = out[c_desc].fillna("").astype(str).str.strip()
    out[c_store] = out[c_store].fillna("").astype(str).str.strip().str.upper()
    out[c_qty]   = out[c_qty].map(parse_qty)

    out = (
        out.groupby([c_code, c_desc, c_store], as_index=False)[c_qty]
           .sum()
           .rename(columns={c_code: "codigo", c_desc: "descripcion",
                            c_store: "storage", c_qty: "sap_qty"})
    )
    return out

def group_saad(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Detecta ubprod/itdesc/ubiest/ubcstk en SAAD y devuelve
    (codigo, storage, descripcion, qty)
    """
    c_code   = find_col(df, ["ubprod"])
    c_desc   = find_col(df, ["itdesc"])
    c_store  = find_col(df, ["ubiest"])
    c_qty    = find_col(df, ["ubcstk", "qty", "cantidad"])

    out = df[[c_code, c_desc, c_store, c_qty]].copy()
    out[c_code]  = out[c_code].map(norm_code)
    out[c_desc]  = out[c_desc].fillna("").astype(str).str.strip()
    out[c_store] = out[c_store].fillna("").astype(str).str.strip().str.upper()
    out[c_qty]   = out[c_qty].map(parse_qty)

    out = (
        out.groupby([c_code, c_desc, c_store], as_index=False)[c_qty]
           .sum()
           .rename(columns={c_code: "codigo", c_desc: "descripcion",
                            c_store: "storage", c_qty: f"saad_{label}_qty"})
    )
    return out

def compare_by_code(saad: pd.DataFrame, sap: pd.DataFrame, sheet_label: str) -> pd.DataFrame:
    """
    Compara por código (sin cruzar storage), sumando por código.
    Devuelve columnas: codigo, descripcion, SAAD_x, SAP, DIFERENCIA (SAAD - SAP)
    """
    saad_agg = (
        saad.groupby(["codigo", "descripcion"], as_index=False)
            [saad.columns[-1]].sum()
    )
    saad_col = saad_agg.columns[-1]
    sap_agg = sap.groupby(["codigo", "descripcion"], as_index=False)["sap_qty"].sum()

    df = pd.merge(saad_agg, sap_agg, on=["codigo", "descripcion"], how="outer")
    df[saad_col] = df[saad_col].fillna(0).astype(int)
    df["sap_qty"] = df["sap_qty"].fillna(0).astype(int)
    df["DIFERENCIA"] = df[saad_col] - df["sap_qty"]
    df = df.sort_values("DIFERENCIA", key=lambda s: s.abs(), ascending=False)
    # Renombrado legible en hoja
    if "bunker" in sheet_label.lower():
        df = df.rename(columns={saad_col: "SAAD BUNKER", "sap_qty": "SAP COLGATE"})
    else:
        df = df.rename(columns={saad_col: "SAAD CBP", "sap_qty": "SAP COLGATE"})
    return df[["codigo", "descripcion"] + [c for c in df.columns if c not in {"codigo","descripcion"}]]

# ---- endpoints ----------------------------------------------------------------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(
    files: List[UploadFile] = File(..., description="Archivos en orden: 1) SAP (.xlsx), 2) SAAD CBP (.xls), 3) SAAD BUNKER (.xls)")
):
    # Validación básica
    if len(files) != 3:
        raise HTTPException(400, "Debes subir 3 archivos: SAP (.xlsx), SAAD_CBP (.xls) y SAAD_BUNKER (.xls)")

    # Lee
    sap_df    = read_any_excel(files[0])
    cbp_df    = read_any_excel(files[1])
    bunker_df = read_any_excel(files[2])

    # Agrupa / normaliza
    sap = group_sap(sap_df)
    saad_cbp = group_saad(cbp_df, label="cbp")
    saad_bunker = group_saad(bunker_df, label="bunker")

    # Filtros por storage
    sap_cbp    = sap[sap["storage"].isin(CBP_STORAGES)].copy()
    sap_bunker = sap[sap["storage"].isin(BUNKER_IN_SAP)].copy()

    # Compara por código (sumado) – así no dependemos de que los nombres de storage coincidan 1 a 1
    sheet_cbp    = compare_by_code(saad_cbp, sap_cbp, sheet_label="CBP vs SAP")
    sheet_bunker = compare_by_code(saad_bunker, sap_bunker, sheet_label="BUNKER vs SAP")

    # Excel de salida con 2 hojas
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        sheet_cbp.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
        sheet_bunker.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'},
    )
