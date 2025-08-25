from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, PlainTextResponse
from typing import Dict, List
import pandas as pd
import numpy as np
from io import BytesIO
import re

app = FastAPI(title="Cruce Inventario API", version="1.0")

# Storages incluidos en cada cruce
CBP_SAP_STORAGES = {"AER", "MOV", "RT", "RP", "BN", "OLR"}
BUNKER_SAP_STORAGES = {"AER"}

# ------------------ Utilidades ------------------

def norm_colname(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[\s\-_]+", "", s)
    return s

def to_str_code(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s

def to_number_es(x) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return 0.0
    s = s.replace(" ", "")
    if s.count(",") == 1 and s.count(".") >= 1:
        s = s.replace(".", "").replace(",", ".")
    else:
        if s.count(",") > 1:
            s = s.replace(",", "")
        else:
            if s.count(",") == 1 and s.count(".") == 0:
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0

def pick_first(series: pd.Series) -> str:
    for v in series:
        if pd.notna(v) and str(v).strip() != "":
            return str(v).strip()
    return ""

SAP_MAP = {
    "codigo": {"material", "materialid", "sku", "codigo", "code"},
    "descripcion": {"materialdescription", "description", "itdesc", "desc", "descripcion"},
    "storage": {"storagelocation", "storage", "almacen", "ubiest", "sublocation"},
    "cantidad": {"bumquantity", "quantity", "qty", "cantidad", "stock", "qtytotal"},
}

SAAD_MAP = {
    "codigo": {"ubprod", "material", "codigo", "sku", "code"},
    "descripcion": {"itdesc", "descripcion", "desc"},
    "storage": {"ubiest", "storage", "almacen"},
    "cantidad": {"ubcstk", "cantidad", "qty", "stock"},
}

def find_colnames(df: pd.DataFrame, mapping: Dict[str, set]) -> Dict[str, str]:
    normalized = {norm_colname(c): c for c in df.columns}
    result = {}
    for want, aliases in mapping.items():
        found = None
        for a in aliases:
            na = norm_colname(a)
            for nc, real in normalized.items():
                if nc == na:
                    found = real
                    break
            if found:
                break
        if not found:
            for nc, real in normalized.items():
                for a in aliases:
                    if a in nc:
                        found = real
                        break
                if found:
                    break
        if not found:
            raise HTTPException(
                status_code=400,
                detail=f"No pude detectar columna '{want}' en el archivo. Esperaba algo como {sorted(list(aliases))}."
            )
        result[want] = found
    return result

# ----------- Lector robusto de Excel -----------

def _read_xls_via_xlrd(content: bytes) -> pd.DataFrame:
    """
    Lee .XLS con xlrd==1.2.0 sin pasar por pandas (evita el chequeo de versión).
    Toma la hoja 0, primera fila como encabezado.
    """
    try:
        import xlrd  # requiere 1.2.0
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"No se pudo importar xlrd para leer .XLS: {e}. Verifica requirements (xlrd==1.2.0)."
        )
    try:
        book = xlrd.open_workbook(file_contents=content)
        sheet = book.sheet_by_index(0)
        rows = []
        for i in range(sheet.nrows):
            rows.append([sheet.cell_value(i, j) for j in range(sheet.ncols)])
        if not rows:
            return pd.DataFrame()
        headers = [str(h).strip() for h in rows[0]]
        data = rows[1:]
        df = pd.DataFrame(data, columns=headers)
        return df
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error leyendo .XLS con xlrd: {e}"
        )

def read_excel_smart(file: UploadFile) -> pd.DataFrame:
    """
    .xlsx -> openpyxl (pandas)
    .xls  -> xlrd manual (sin pandas) para evitar el error de versión
    """
    content = file.file.read()
    name = (file.filename or "").lower()

    if name.endswith(".xlsx"):
        try:
            return pd.read_excel(BytesIO(content), engine="openpyxl")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error leyendo .XLSX: {e}")
    elif name.endswith(".xls"):
        return _read_xls_via_xlrd(content)
    else:
        # último intento: dejar que pandas decida
        try:
            return pd.read_excel(BytesIO(content))
        except Exception:
            raise HTTPException(
                status_code=400,
                detail=f"Formato no soportado para '{file.filename}'. Subí .xlsx o .xls reales."
            )

# ----------- Normalización -----------

def normalize_sap(df: pd.DataFrame) -> pd.DataFrame:
    cols = find_colnames(df, SAP_MAP)
    out = pd.DataFrame()
    out["codigo"] = df[cols["codigo"]].map(to_str_code)
    out["descripcion"] = df[cols["descripcion"]].astype(str).fillna("")
    out["storage"] = df[cols["storage"]].astype(str).str.strip().str.upper()
    out["cantidad"] = df[cols["cantidad"]].map(to_number_es).fillna(0.0)
    return out

def normalize_saad(df: pd.DataFrame) -> pd.DataFrame:
    cols = find_colnames(df, SAAD_MAP)
    out = pd.DataFrame()
    out["codigo"] = df[cols["codigo"]].map(to_str_code)
    out["descripcion"] = df[cols["descripcion"]].astype(str).fillna("")
    out["storage"] = df[cols["storage"]].astype(str).str.strip().str.upper()
    out["cantidad"] = df[cols["cantidad"]].map(to_number_es).fillna(0.0)
    return out

def aggregate_by_code(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["codigo", "descripcion", "cantidad"])
    g = df.groupby("codigo", as_index=False).agg(
        cantidad=("cantidad", "sum"),
        descripcion=("descripcion", pick_first),
    )
    return g[["codigo", "descripcion", "cantidad"]]

def compare_sheets(saad_df: pd.DataFrame, sap_df: pd.DataFrame,
                   saad_title: str, sap_storages: set) -> pd.DataFrame:
    sap_filtered = sap_df[sap_df["storage"].isin(sap_storages)].copy()
    sap_agg = aggregate_by_code(sap_filtered).rename(columns={"cantidad": "SAP COLGATE"})
    saad_agg = aggregate_by_code(saad_df).rename(columns={"cantidad": saad_title})

    merged = pd.merge(
        saad_agg, sap_agg, on=["codigo"], how="outer", suffixes=("", "_sap"), sort=False
    )

    merged["descripcion"] = merged["descripcion"].fillna("")
    if "descripcion_sap" in merged.columns:
        merged["descripcion"] = merged["descripcion"].where(
            merged["descripcion"] != "", merged["descripcion_sap"].astype(str)
        )
        merged.drop(columns=["descripcion_sap"], inplace=True, errors="ignore")

    if saad_title not in merged.columns:
        merged[saad_title] = 0.0
    if "SAP COLGATE" not in merged.columns:
        merged["SAP COLGATE"] = 0.0

    merged[saad_title] = merged[saad_title].fillna(0.0)
    merged["SAP COLGATE"] = merged["SAP COLGATE"].fillna(0.0)

    merged["DIFERENCIA"] = merged[saad_title] - merged["SAP COLGATE"]
    merged["absdiff"] = merged["DIFERENCIA"].abs()
    merged.sort_values(by=["absdiff", "codigo"], ascending=[False, True], inplace=True)
    merged.drop(columns=["absdiff"], inplace=True)
    return merged[["codigo", "descripcion", saad_title, "SAP COLGATE", "DIFERENCIA"]]

# ------------------ Endpoints ------------------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(files: List[UploadFile] = File(...)):
    if not files or len(files) != 3:
        raise HTTPException(
            status_code=400,
            detail="Debes subir exactamente 3 archivos: SAP.xlsx, SAAD_CBP, SAAD_BUNKER (en ese orden)."
        )

    sap_f, cbp_f, bunker_f = files
    try:
        sap_raw = read_excel_smart(sap_f)
        cbp_raw = read_excel_smart(cbp_f)
        bunker_raw = read_excel_smart(bunker_f)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error leyendo archivos: {e}")

    try:
        sap_df = normalize_sap(sap_raw)
        cbp_df = normalize_saad(cbp_raw)
        bunker_df = normalize_saad(bunker_raw)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error normalizando: {e}")

    hoja_cbp = compare_sheets(cbp_df, sap_df, "SAAD CBP", CBP_SAP_STORAGES)
    hoja_bunker = compare_sheets(bunker_df, sap_df, "SAAD BUNKER", BUNKER_SAP_STORAGES)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        hoja_cbp.to_excel(writer, sheet_name="CBP_vs_SAP", index=False)
        hoja_bunker.to_excel(writer, sheet_name="BUNKER_vs_SAP", index=False)
        for sn in ("CBP_vs_SAP", "BUNKER_vs_SAP"):
            ws = writer.sheets[sn]
            ws.set_column("A:A", 16)
            ws.set_column("B:B", 48)
            ws.set_column("C:E", 16)
    output.seek(0)

    headers = {"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
