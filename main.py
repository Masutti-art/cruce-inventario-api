# main.py
from __future__ import annotations
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional, Dict
import pandas as pd
import io, re, sys, unicodedata, logging
from pathlib import Path

app = FastAPI(title="Cruce Inventario API", version="1.0")
logger = logging.getLogger("uvicorn.error")

# -----------------------
# Configuración / candidatos de nombres
# -----------------------
CODE_CANDIDATES = [
    "codigo","cod","sku","material","articulo","item","code",
    "cod_sap","codigo_sap","matnr","referencia","ref",
    "nro_material","num_material"
]
STO_CANDIDATES  = [
    "storage","almacen","almacén","dep","deposito","bodega",
    "warehouse","ubicacion","location","st","sucursal","planta"
]
DESC_CANDIDATES = [
    "descripcion","denominacion","denominación","description","desc","nombre","detalle"
]
QTY_CANDIDATES  = [
    "cajas","cantidad","qty","stock","unidades","cant","saldo",
    "existencia","existencias","inventario"
]

# -----------------------
# Utilidades
# -----------------------
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

    # 1) coincidencia exacta
    for c in cand:
        if c in norm:
            return norm[c]
    # 2) contiene
    for c in cand:
        for k, orig in norm.items():
            if c in k:
                if not numeric or pd.api.types.is_numeric_dtype(df[orig]):
                    return orig
    # 3) si piden numérica, tomar la de mayor suma absoluta
    if numeric:
        num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
        if num_cols:
            sums = (df[num_cols].abs().sum(numeric_only=True)).sort_values(ascending=False)
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

def _read_excel_try(fobj, engine: str, header) -> pd.DataFrame:
    fobj.seek(0)
    return pd.read_excel(fobj, engine=engine, header=header)

def _read_any_table(file: UploadFile) -> pd.DataFrame:
    """Lee xls/xlsx/csv/txt. En Excel prueba encabezado en filas 0..10."""
    name = file.filename or "archivo"
    suffix = Path(name).suffix.lower()
    try:
        if suffix in (".xls", ".xlsx", ".xlsm"):
            engine = "xlrd" if suffix == ".xls" else "openpyxl"
            for hdr in range(0, 11):  # prueba header en 0..10
                try:
                    df = _read_excel_try(file.file, engine, hdr)
                    if not df.dropna(how="all").empty and len(df.columns) >= 2:
                        return df
                except Exception:
                    continue
            # último intento default
            return _read_excel_try(file.file, engine, 0)
        elif suffix in (".csv", ".txt"):
            file.file.seek(0)
            return pd.read_csv(file.file, sep=None, engine="python")
        else:
            file.file.seek(0)
            return pd.read_excel(file.file)  # que intente con lo que pueda
    finally:
        try:
            file.file.seek(0)
        except Exception:
            pass

def _normalize_df(
    df: pd.DataFrame,
    code_col_override: Optional[str] = None,
    storage_col_override: Optional[str] = None,
    desc_col_override: Optional[str] = None,
    qty_col_override: Optional[str] = None,
) -> pd.DataFrame:
    """Devuelve columnas normalizadas: codigo, storage, descripcion, cajas (agrupadas)."""
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    # Overrides explícitos tienen prioridad
    codigo_col  = _find_col_by_name(df, code_col_override) or _guess_col(df, CODE_CANDIDATES)
    storage_col = _find_col_by_name(df, storage_col_override) or _guess_col(df, STO_CANDIDATES)
    desc_col    = _find_col_by_name(df, desc_col_override) or _guess_col(df, DESC_CANDIDATES)
    qty_col     = _find_col_by_name(df, qty_col_override)  or _guess_col(df, QTY_CANDIDATES, numeric=True)

    if not codigo_col:
        raise ValueError("No se encontró la columna de CÓDIGO en el archivo.")
    if not qty_col:
        raise ValueError("No se encontró la columna de CANTIDAD/CAJAS en el archivo.")

    out = pd.DataFrame()
    out["codigo"] = df[codigo_col].astype(str).str.strip()
    out["storage"] = df[storage_col].astype(str).str.strip() if storage_col else ""
    out["descripcion"] = df[desc_col].astype(str) if desc_col else ""
    out["cajas"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    out = (
        out.groupby(["codigo", "storage"], as_index=False)
           .agg({"descripcion": "first", "cajas": "sum"})
    )
    return out

def _maybe_int(v):
    try:
        return int(v) if float(v).is_integer() else float(v)
    except Exception:
        return v

# -----------------------
# Núcleo del cruce
# -----------------------
async def procesar_cruce(files: List[UploadFile], overrides: dict | None = None) -> Dict:
    """Cruza N archivos por (codigo, storage); baseline = primer archivo."""
    overrides = overrides or {}
    if not files or len(files) < 2:
        raise HTTPException(status_code=400, detail="Subí al menos 2 archivos para cruzar.")

    etiquetas: List[str] = []
    norm_dfs: List[pd.DataFrame] = []

    def _norm(df_raw: pd.DataFrame) -> pd.DataFrame:
        return _normalize_df(
            df_raw,
            code_col_override=overrides.get("code_col"),
            storage_col_override=overrides.get("storage_col"),
            desc_col_override=overrides.get("desc_col"),
            qty_col_override=overrides.get("qty_col"),
        )

    # Leer y normalizar cada archivo
    for f in files:
        base = Path(f.filename or "archivo").stem
        etiquetas.append(base)
        df_raw = _read_any_table(f)
        df_norm = _norm(df_raw)
        norm_dfs.append(df_norm)

    # Merge progresivo (outer) por claves
    base_keys = ["codigo", "storage"]
    merged: Optional[pd.DataFrame] = None
    for label, nd in zip(etiquetas, norm_dfs):
        col = f"cajas_{label}"
        nd = nd.rename(columns={"cajas": col})
        merged = nd if merged is None else merged.merge(nd, on=base_keys, how="outer")

    # Descripción: toma la primera no vacía si hay varias
    if "descripcion" not in merged.columns:
        merged["descripcion"] = ""
    else:
        desc_cols = [c for c in merged.columns if c.startswith("descripcion")]
        if len(desc_cols) > 1:
            merged["descripcion"] = merged[desc_cols].bfill(axis=1).iloc[:, 0]
            merged.drop(columns=[c for c in desc_cols if c != "descripcion"], inplace=True)

    # Rellenar NaN en columnas de cajas
    cajas_cols = [c for c in merged.columns if c.startswith("cajas_")]
    for c in cajas_cols:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    # Diffs contra el PRIMER archivo (baseline)
    baseline = cajas_cols[0]
    diff_cols: List[str] = []
    for c in cajas_cols[1:]:
        diff_name = f"diff_{c.replace('cajas_', '')}_vs_{baseline.replace('cajas_', '')}"
        merged[diff_name] = merged[c] - merged[baseline]
        diff_cols.append(diff_name)

    # Métrica para ordenar
    merged["dif_mayor_abs"] = merged[diff_cols].abs().max(axis=1) if diff_cols else 0

    # Resumen
    total = int(len(merged))
    con_dif = int(merged[diff_cols].ne(0).any(axis=1).sum()) if diff_cols else 0
    sin_dif = int(total - con_dif)

    visibles = ["codigo", "storage", "descripcion"] + cajas_cols + diff_cols
    merged_vis = merged[visibles].copy()
    for c in cajas_cols + diff_cols:
        merged_vis[c] = merged_vis[c].apply(_maybe_int)

    res = {
        "resumen": {
            "archivos": etiquetas,
            "total_claves": total,
            "con_diferencias": con_dif,
            "sin_diferencias": sin_dif,
        },
        "diferencias": merged_vis.to_dict(orient="records"),
    }
    return res

# -----------------------
# Excel builder
# -----------------------
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
        # Métrica al principio
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

# -----------------------
# Endpoints
# -----------------------
@app.post("/cruce")
async def cruce(
    files: List[UploadFile] = File(...),
    code_col: Optional[str] = None,
    storage_col: Optional[str] = None,
    desc_col: Optional[str] = None,
    qty_col: Optional[str] = None,
):
    """Devuelve JSON: resumen + diferencias por clave (codigo, storage)."""
    try:
        overrides = {
            "code_col": code_col,
            "storage_col": storage_col,
            "desc_col": desc_col,
            "qty_col": qty_col,
        }
        return await procesar_cruce(files, overrides=overrides)
    except Exception as e:
        logger.exception("Error en /cruce")
        return JSONResponse(status_code=500, content={"detail": f"/cruce: {e}"})

@app.post(
    "/cruce-xlsx",
    responses={200: {
        "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        "description": "Cruce en Excel (ordenado por mayor diferencia absoluta)",
    }},
)
async def cruce_xlsx(
    files: List[UploadFile] = File(...),
    code_col: Optional[str] = None,
    storage_col: Optional[str] = None,
    desc_col: Optional[str] = None,
    qty_col: Optional[str] = None,
):
    """Devuelve un .xlsx: hoja 'Diferencias' (ordenada ↓ por dif_mayor_abs) y hoja 'Resumen'."""
    try:
        overrides = {
            "code_col": code_col,
            "storage_col": storage_col,
            "desc_col": desc_col,
            "qty_col": qty_col,
        }
        res = await procesar_cruce(files, overrides=overrides)
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
