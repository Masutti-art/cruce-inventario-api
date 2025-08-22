from __future__ import annotations
import io
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Cruce Inventario API", version="1.1.1")
logger = logging.getLogger("uvicorn.error")

# ===== Defaults y candidatos (Bunker/CBP + SAP) =====
DEFAULT_CODE_COL = "MATERIAL"       # SKU/código
DEFAULT_QTY_COL = "SALDO"           # cantidad
DEFAULT_STORAGE_COL = "ALMACEN"     # almacén / depósito
DEFAULT_DESC_COL = "DESCRIPCION"    # descripción

CODE_CANDIDATES = [
    "codigo","cod","sku","material","articulo","item","code",
    "cod_sap","codigo_sap","matnr","referencia","ref","nro_material","num_material",
    "ubprod"  # Bunker/CBP
]
STO_CANDIDATES = [
    "storage","almacen","almacén","dep","deposito","bodega","warehouse",
    "ubicacion","location","st","sucursal","planta",
    "ubiest",                # Bunker/CBP
    "storage location"       # SAP
]
DESC_CANDIDATES = [
    "descripcion","denominacion","denominación","description","desc","nombre","detalle",
    "material description",  # SAP
    "itdesc"                 # Bunker/CBP
]
QTY_CANDIDATES = [
    "cajas","cantidad","qty","stock","unidades","cant","saldo","existencia","existencias","inventario",
    "ubcstk",               # Bunker/CBP
    "bum quantity"          # SAP
]

# ===== Utilidades =====
def _sanitize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9_ \-]", "", s)
    return s

def _guess_col(df: pd.DataFrame, candidates: List[str], numeric: bool = False) -> Optional[str]:
    cols = list(df.columns)
    norm = {_sanitize(c): c for c in cols}
    cand = [_sanitize(x) for x in candidates]

    for c in cand:
        if c in norm:
            return norm[c]
    for c in cand:
        for k, orig in norm.items():
            if c in k:
                if not numeric or pd.api.types.is_numeric_dtype(df[orig]):
                    return orig
    if numeric:
        num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
        if num_cols:
            sums = df[num_cols].abs().sum(numeric_only=True).sort_values(ascending=False)
            if len(sums) > 0:
                return str(sums.index[0])
    return None

def _find_col_by_name(df: pd.DataFrame, name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    tgt = _sanitize(name)
    for c in df.columns:
        if _sanitize(str(c)) == tgt:
            return c
    for c in df.columns:
        if tgt in _sanitize(str(c)):
            return c
    return None

def _clean_code(x: str) -> Optional[str]:
    """Normaliza SKU: quita ceros a la izquierda, trim, upper. Vacíos y totales → None."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.upper() in {"TOTAL","RESUMEN","SUBTOTAL","NA","N/A","-"}:
        return None
    s = re.sub(r"^0+(?=[A-Za-z0-9])", "", s)
    s = s.upper()
    return s if s else None

def _clean_storage(x: str) -> str:
    if x is None:
        return ""
    return str(x).strip().upper()

def _read_excel_try(fobj, engine: str, header) -> pd.DataFrame:
    fobj.seek(0)
    return pd.read_excel(fobj, engine=engine, header=header)

def _read_any_table(file: UploadFile, header_row: Optional[int] = None) -> pd.DataFrame:
    """Lee xls/xlsx/csv/txt. En Excel prueba encabezado 0..30 o usa header_row si se indica."""
    name = file.filename or "archivo"
    suffix = Path(name).suffix.lower()
    try:
        if suffix in (".xls", ".xlsx", ".xlsm"):
            engine = "xlrd" if suffix == ".xls" else "openpyxl"
            if header_row is not None:
                return _read_excel_try(file.file, engine, header_row)
            for hdr in range(0, 31):  # 0..30
                try:
                    df = _read_excel_try(file.file, engine, hdr)
                    if not df.dropna(how="all").empty and len(df.columns) >= 2:
                        return df
                except Exception:
                    continue
            return _read_excel_try(file.file, engine, 0)
        elif suffix in (".csv", ".txt"):
            file.file.seek(0)
            return pd.read_csv(file.file, sep=None, engine="python")
        else:
            file.file.seek(0)
            return pd.read_excel(file.file)
    finally:
        try:
            file.file.seek(0)
        except Exception:
            pass

def _looks_bad_desc(s: str) -> bool:
    if not s:
        return True
    t = str(s).strip().upper()
    if t in {"-", "NA", "N/A", "NONE"}:
        return True
    if re.match(r"^\d+\s*\w{2,4}$", t):
        return True
    return False

def _friendly_label(label: str) -> str:
    U = label.upper()
    if "SAP" in U:    return "SAP"
    if "CBP" in U:    return "CBP"
    if "BUNKER" in U: return "BUNKER"
    return re.sub(r"[^A-Z0-9]+", "_", U)[:20].strip("_")

# ===== Normalización por archivo =====
def _normalize_df(
    df: pd.DataFrame,
    code_col_override: Optional[str] = None,
    storage_col_override: Optional[str] = None,
    desc_col_override: Optional[str] = None,
    qty_col_override: Optional[str] = None,
) -> pd.DataFrame:
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    codigo_col  = _find_col_by_name(df, code_col_override    or DEFAULT_CODE_COL)     or _guess_col(df, CODE_CANDIDATES)
    storage_col = _find_col_by_name(df, storage_col_override or DEFAULT_STORAGE_COL)  or _guess_col(df, STO_CANDIDATES)
    desc_col    = _find_col_by_name(df, desc_col_override    or DEFAULT_DESC_COL)     or _guess_col(df, DESC_CANDIDATES)
    qty_col     = _find_col_by_name(df, qty_col_override     or DEFAULT_QTY_COL)      or _guess_col(df, QTY_CANDIDATES, numeric=True)

    if not codigo_col:
        raise ValueError("No se encontró la columna de CÓDIGO en el archivo.")
    if not qty_col:
        raise ValueError("No se encontró la columna de CANTIDAD/CAJAS en el archivo.")

    out = pd.DataFrame()
    out["codigo"] = df[codigo_col].map(_clean_code)
    out["storage"] = df[storage_col].map(_clean_storage) if storage_col else ""
    out["descripcion"] = df[desc_col].astype(str) if desc_col else ""
    out["cajas"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    out = out.dropna(subset=["codigo"])
    out = (
        out.groupby(["codigo", "storage"], as_index=False)
           .agg({"descripcion": "first", "cajas": "sum"})
    )
    return out

# ===== Motor de cruce =====
def _maybe_int(v):
    try:
        return int(v) if float(v).is_integer() else float(v)
    except Exception:
        return v

async def procesar_cruce(
    files: List[UploadFile],
    overrides: Optional[Dict] = None,
    only_storage: Optional[str] = None,
    header_row: Optional[int] = None,
) -> Dict:
    overrides = overrides or {}
    if not files or len(files) < 2:
        raise HTTPException(status_code=400, detail="Subí al menos 2 archivos para cruzar.")

    etiquetas: List[str] = []
    norm_dfs: List[pd.DataFrame] = []

    for f in files:
        base = Path(f.filename or "archivo").stem
        label = _friendly_label(base)
        etiquetas.append(label)

        df_raw = _read_any_table(f, header_row=header_row)
        dfn = _normalize_df(
            df_raw,
            code_col_override=overrides.get("code_col"),
            storage_col_override=overrides.get("storage_col"),
            desc_col_override=overrides.get("desc_col"),
            qty_col_override=overrides.get("qty_col"),
        )
        if only_storage:
            val = only_storage.strip().upper()
            dfn = dfn[dfn["storage"] == val]

        dfn = dfn.rename(columns={"cajas": f"cajas_{label}", "descripcion": f"desc_{label}"})
        norm_dfs.append(dfn)

    base_keys = ["codigo", "storage"]
    merged: Optional[pd.DataFrame] = None
    for nd in norm_dfs:
        merged = nd if merged is None else merged.merge(nd, on=base_keys, how="outer")

    cajas_cols = [c for c in merged.columns if c.startswith("cajas_")]
    desc_cols  = [c for c in merged.columns if c.startswith("desc_")]
    for c in cajas_cols:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    merged["descripcion"] = ""
    sap_desc = [c for c in desc_cols if c.upper() == "DESC_SAP"]
    if sap_desc:
        merged["descripcion"] = merged[sap_desc[0]].astype(str)

    def _choose_desc(row):
        cur = str(row.get("descripcion","")).strip()
        if not _looks_bad_desc(cur):
            return cur
        for c in desc_cols:
            val = str(row.get(c,"")).strip()
            if not _looks_bad_desc(val):
                return val
        return cur
    merged["descripcion"] = merged.apply(_choose_desc, axis=1)

    baseline = cajas_cols[0]
    diff_cols: List[str] = []
    for c in cajas_cols[1:]:
        diff_name = f"diff_{c.replace('cajas_', '')}_vs_{baseline.replace('cajas_', '')}"
        merged[diff_name] = merged[c] - merged[baseline]
        diff_cols.append(diff_name)

    merged["dif_mayor_abs"] = merged[diff_cols].abs().max(axis=1) if diff_cols else 0

    prefer = ["codigo", "storage", "descripcion"]
    def _key(x: str) -> tuple:
        if x.endswith("BUNKER"): return (0, x)
        if x.endswith("CBP"):    return (1, x)
        if x.endswith("SAP"):    return (2, x)
        return (9, x)
    cajas_sorted = sorted(cajas_cols, key=_key)

    final_cols = prefer + cajas_sorted + diff_cols + ["dif_mayor_abs"]
    merged = merged[final_cols]
    merged = merged[merged["codigo"].notna() & (merged["codigo"].astype(str).str.strip()!="")]

    total = int(len(merged))
    con_dif = int(merged[diff_cols].ne(0).any(axis=1).sum()) if diff_cols else 0
    sin_dif = int(total - con_dif)

    res = {
        "resumen": {
            "archivos": etiquetas,
            "total_claves": total,
            "con_diferencias": con_dif,
            "sin_diferencias": sin_dif,
        },
        "diferencias": merged.to_dict(orient="records"),
    }
    return res

# ===== Excel =====
def _xlsx_from_res(res: Dict) -> bytes:
    df = pd.DataFrame(res.get("diferencias", []))
    if df.empty:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            pd.DataFrame([res.get("resumen", {})]).to_excel(w, "Resumen", index=False)
            pd.DataFrame([], columns=["sin_datos"]).to_excel(w, "Diferencias", index=False)
        buf.seek(0)
        return buf.read()

    diff_cols = [c for c in df.columns if str(c).startswith("diff_")]
    if diff_cols:
        df["dif_mayor_abs"] = df[diff_cols].abs().max(axis=1)
        df = df[df["dif_mayor_abs"] != 0].sort_values("dif_mayor_abs", ascending=False)
        first_cols = ["dif_mayor_abs"]
        other_cols = [c for c in df.columns if c not in first_cols]
        df = df[first_cols + other_cols]

    resumen = res.get("resumen", {})
    resumen_df = pd.DataFrame([{
        "total_claves": resumen.get("total_claves", 0),
        "con_diferencias": resumen.get("con_diferencias", 0),
        "sin_diferencias": resumen.get("sin_diferencias", 0),
        "archivos": ", ".join(resumen.get("archivos", [])),
    }])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df.to_excel(w, sheet_name="Diferencias", index=False)
        resumen_df.to_excel(w, sheet_name="Resumen", index=False)
    buf.seek(0)
    return buf.read()

# ===== Endpoints =====
@app.post("/cruce")
async def cruce(
    files: List[UploadFile] = File(...),
    code_col: Optional[str] = None,
    storage_col: Optional[str] = None,
    desc_col: Optional[str] = None,
    qty_col: Optional[str] = None,
    only_storage: Optional[str] = None,
    header_row: Optional[int] = None,
):
    try:
        overrides = {
            "code_col": code_col,
            "storage_col": storage_col,
            "desc_col": desc_col,
            "qty_col": qty_col,
        }
        return await procesar_cruce(
            files,
            overrides=overrides,
            only_storage=only_storage,
            header_row=header_row,
        )
    except Exception as e:
        logger.exception("Error en /cruce")
        return JSONResponse(status_code=500, content={"detail": f"/cruce: {e}"})

@app.post(
    "/cruce-xlsx",
    responses={200: {
        "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        "description": "Cruce en Excel (ordenado ↓ por mayor diferencia absoluta)",
    }},
)
async def cruce_xlsx(
    files: List[UploadFile] = File(...),
    code_col: Optional[str] = None,
    storage_col: Optional[str] = None,
    desc_col: Optional[str] = None,
    qty_col: Optional[str] = None,
    only_storage: Optional[str] = None,
    header_row: Optional[int] = None,
):
    try:
        overrides = {
            "code_col": code_col,
            "storage_col": storage_col,
            "desc_col": desc_col,
            "qty_col": qty_col,
        }
        res = await procesar_cruce(
            files,
            overrides=overrides,
            only_storage=only_storage,
            header_row=header_row,
        )
        xlsx_bytes = _xlsx_from_res(res)
        return StreamingResponse(
            io.BytesIO(xlsx_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
        )
    except Exception as e:
        logger.exception("Error en /cruce-xlsx")
        raise HTTPException(status_code=500, detail=f"/cruce-xlsx: {e}")

@app.get("/healthz")
def healthz():
    import pandas as _pd
    return {"ok": True, "python": sys.version.split()[0], "pandas": _pd.__version__}
