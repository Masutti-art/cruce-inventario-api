# main.py
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

app = FastAPI(title="Cruce Inventario API", version="1.5.0")
logger = logging.getLogger("uvicorn.error")

# =========================
# Configuración
# =========================
# BUNKER fijo según tu pedido:
BUNKER_STORAGES = {"AER", "MOV", "RT", "RP", "BN", "OLR"}

DEFAULT_CODE_COL = "MATERIAL"
DEFAULT_QTY_COL = "SALDO"
DEFAULT_STORAGE_COL = "ALMACEN"
DEFAULT_DESC_COL = "DESCRIPCION"

CODE_CANDIDATES = [
    "codigo", "cod", "sku", "material", "articulo", "item", "code",
    "cod_sap", "codigo_sap", "matnr", "referencia", "ref", "nro_material", "num_material",
    "ubprod",  # Bunker/CBP
]
STO_CANDIDATES = [
    "storage", "almacen", "almacén", "dep", "deposito", "bodega", "warehouse",
    "ubicacion", "location", "st", "sucursal", "planta",
    "ubiest",                # Bunker/CBP
    "storage location",      # SAP
]
DESC_CANDIDATES = [
    "descripcion", "denominacion", "denominación", "description", "desc", "nombre", "detalle",
    "material description",  # SAP
    "itdesc",                # Bunker/CBP
]
QTY_CANDIDATES = [
    "cajas", "cantidad", "qty", "stock", "unidades", "cant", "saldo", "existencia", "existencias", "inventario",
    "ubcstk",               # Bunker/CBP
    "bum quantity",         # SAP
]

# =========================
# Utilidades
# =========================
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
    # exacta
    for c in cand:
        if c in norm:
            return norm[c]
    # contiene
    for c in cand:
        for k, orig in norm.items():
            if c in k:
                if not numeric or pd.api.types.is_numeric_dtype(df[orig]):
                    return orig
    # numérica con mayor suma
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
    """Normaliza SKU: quita ceros a la izquierda, trim, upper. Vacíos/totales/NAN → None."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.upper() in {"TOTAL", "RESUMEN", "SUBTOTAL", "NA", "N/A", "-", "NAN"}:
        return None
    s = re.sub(r"^0+(?=[A-Za-z0-9])", "", s)  # 0000123 -> 123 ; 0000ABC -> ABC
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
            for hdr in range(0, 31):
                try:
                    df = _read_excel_try(file.file, engine, hdr)
                    if not df.dropna(hoy="all").empty and len(df.columns) >= 2:
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
    if re.match(r"^\d+\s*\w{2,4}$", t):  # ej. "3 CAJ"
        return True
    return False

def _friendly_label(label: str) -> str:
    U = label.upper()
    if "SAP" in U:
        return "SAP"
    if "CBP" in U:
        return "CBP"
    if "BUNKER" in U:
        return "BUNKER"
    return re.sub(r"[^A-Z0-9]+", "_", U)[:20].strip("_")

# ====== PASO B: conversión robusta de cantidades ======
def _parse_qty(val) -> float:
    """
    Convierte cantidades que vienen como texto con separadores
    (ej: '1.234', '12.345,00', '9,876', '  1 234 ') a número.
    Si no puede, devuelve 0.
    """
    if pd.isna(val):
        return 0.0
    s = str(val).strip().replace("\u00A0", " ")  # NBSP
    if s == "":
        return 0.0

    # Caso típico europeo: 12.345,67  ->  12345.67
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # Si solo hay comas y no puntos, suele ser decimal: 123,45 -> 123.45
        # Si solo hay puntos (miles) -> quitamos puntos
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        else:
            s = s.replace(".", "")

    try:
        return float(s)
    except Exception:
        # Plan B: quita todo menos dígitos y signo
        digits = re.sub(r"[^0-9\-]", "", s)
        if digits in ("", "-"):
            return 0.0
        try:
            return float(digits)
        except Exception:
            return 0.0

# =========================
# Normalización por archivo
# =========================
def _normalize_df(
    df: pd.DataFrame,
    code_col_override: Optional[str] = None,
    storage_col_override: Optional[str] = None,
    desc_col_override: Optional[str] = None,
    qty_col_override: Optional[str] = None,
) -> pd.DataFrame:
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]

    codigo_col  = _find_col_by_name(df, code_col_override    or DEFAULT_CODE_COL)    or _guess_col(df, CODE_CANDIDATES)
    storage_col = _find_col_by_name(df, storage_col_override or DEFAULT_STORAGE_COL) or _guess_col(df, STO_CANDIDATES)
    desc_col    = _find_col_by_name(df, desc_col_override    or DEFAULT_DESC_COL)    or _guess_col(df, DESC_CANDIDATES)
    qty_col     = _find_col_by_name(df, qty_col_override     or DEFAULT_QTY_COL)     or _guess_col(df, QTY_CANDIDATES, numeric=True)

    if not codigo_col:
        raise ValueError("No se encontró la columna de CÓDIGO en el archivo.")
    if not qty_col:
        raise ValueError("No se encontró la columna de CANTIDAD/CAJAS en el archivo.")

    out = pd.DataFrame()
    out["codigo"] = df[codigo_col].map(_clean_code)
    out["storage"] = df[storage_col].map(_clean_storage) if storage_col else ""
    out["descripcion"] = df[desc_col].astype(str) if desc_col else ""
    # ====== PASO B aplicado ======
    out["cajas"] = df[qty_col].map(_parse_qty).fillna(0)

    # descartar filas sin código
    out = out.dropna(subset=["codigo"])
    out = out[out["codigo"].astype(str).str.strip() != ""]

    # agrupar por clave
    out = (
        out.groupby(["codigo", "storage"], as_index=False)
           .agg({"descripcion": "first", "cajas": "sum"})
    )
    return out

# =========================
# Motor de cruce
# =========================
async def procesar_cruce(
    files: List[UploadFile],
    overrides: Optional[Dict] = None,
    only_storage: Optional[str] = None,
    header_row: Optional[int] = None,
    match_mode: str = "code",  # por código como default
) -> Dict:
    overrides = overrides or {}
    if not files or len(files) < 2:
        raise HTTPException(status_code=400, detail="Subí al menos 2 archivos para cruzar.")

    etiquetas: List[str] = []
    norm_dfs: List[pd.DataFrame] = []

    for f in files:
        base = Path(f.filename or "archivo").stem
        label = _friendly_label(base)  # SAP / CBP / BUNKER / otro
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

    # Clave de cruce
    base_keys = ["codigo", "storage"] if match_mode != "code" else ["codigo"]

    # Si hay cruce por código, consolidamos por archivo
    prepped = []
    for nd in norm_dfs:
        qty_col = [c for c in nd.columns if c.startswith("cajas_")][0]
        desc_col = [c for c in nd.columns if c.startswith("desc_")][0]
        if match_mode == "code":
            nd = nd.groupby("codigo", as_index=False).agg({
                qty_col: "sum",
                "storage": lambda s: ",".join(sorted({str(x) for x in s if str(x).strip()}))[:60],
                desc_col: "first",
            })
        prepped.append(nd)

    # Merge progresivo
    merged: Optional[pd.DataFrame] = None
    for nd in prepped:
        merged = nd if merged is None else merged.merge(nd, on=base_keys, how="outer")

    if merged is None or merged.empty:
        return {"resumen": {"archivos": etiquetas, "total_claves": 0, "con_diferencias": 0, "sin_diferencias": 0}, "diferencias": []}

    # Asegurar numéricos
    cajas_cols = [c for c in merged.columns if c.startswith("cajas_")]
    desc_cols  = [c for c in merged.columns if c.startswith("desc_")]
    for c in cajas_cols:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    # Descripción preferida: SAP > primera “buena”
    merged["descripcion"] = ""
    sap_desc = [c for c in desc_cols if c.upper() == "DESC_SAP"]
    if sap_desc:
        merged["descripcion"] = merged[sap_desc[0]].astype(str)

    def _choose_desc(row):
        cur = str(row.get("descripcion", "")).strip()
        if not _looks_bad_desc(cur):
            return cur
        for c in desc_cols:
            val = str(row.get(c, "")).strip()
            if not _looks_bad_desc(val):
                return val
        return cur

    merged["descripcion"] = merged.apply(_choose_desc, axis=1)

    # Dif. internas: baseline SAP si existe
    baseline_label = "SAP" if "SAP" in etiquetas else etiquetas[0]
    baseline = f"cajas_{baseline_label}"
    diff_cols: List[str] = []
    for lbl in etiquetas:
        if lbl == baseline_label:
            continue
        col = f"cajas_{lbl}"
        if col in merged.columns:
            dname = f"diff_{lbl}_vs_{baseline_label}"
            merged[dname] = merged[col] - merged[baseline]
            diff_cols.append(dname)

    merged["dif_mayor_abs"] = merged[diff_cols].abs().max(axis=1) if diff_cols else 0

    # Limpieza final de códigos en blanco
    if "codigo" in merged.columns:
        merged = merged[merged["codigo"].notna() & (merged["codigo"].astype(str).str.strip() != "")]

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

# =========================
# Excel: dos pestañas + resumen
# =========================
def _xlsx_from_res(res: Dict, match_mode: str = "code") -> bytes:
    """
    Genera un XLSX con:
     - 'SAAD CBP vs SAP'    (NO BUNKER_STORAGES)
     - 'SAAD BUNKER vs SAP' (SOLO BUNKER_STORAGES)
     - 'Resumen'
    En match_mode='code': no se muestra 'storage' (se agrupa por código).
    """
    df = pd.DataFrame(res.get("diferencias", []))

    # Libro vacío si no hay datos
    if df.empty:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            cols_cbp = ["codigo", "descripcion", "SAAD CBP", "SAP COLGATE", "DIFERENCIA"]
            cols_bun = ["codigo", "descripcion", "SAAD BUNKER", "SAP COLGATE", "DIFERENCIA"]
            pd.DataFrame([], columns=cols_cbp).to_excel(w, "SAAD CBP vs SAP", index=False)
            pd.DataFrame([], columns=cols_bun).to_excel(w, "SAAD BUNKER vs SAP", index=False)
            pd.DataFrame([res.get("resumen", {})]).to_excel(w, "Resumen", index=False)
        buf.seek(0)
        return buf.read()

    cols = list(df.columns)

    def _find_cajas(tag: str) -> Optional[str]:
        exact = f"cajas_{tag}"
        if exact in cols:
            return exact
        tagU = tag.upper()
        for c in cols:
            if c.startswith("cajas_") and tagU in c.upper():
                return c
        return None

    col_sap    = _find_cajas("SAP")
    col_cbp    = _find_cajas("CBP")
    col_bunker = _find_cajas("BUNKER")

    # numéricas a 0
    for c in [col for col in [col_sap, col_cbp, col_bunker] if col is not None]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # textos sin NaN
    for c in ["codigo", "storage", "descripcion"]:
        if c in df.columns:
            df[c] = df[c].astype(str).replace({"nan": "", "NaN": "", "NAN": ""}).fillna("")

    # columnas base
    base_cols = ["codigo", "descripcion"] if match_mode == "code" else ["codigo", "storage", "descripcion"]

    # ----- SAAD CBP vs SAP (NO BUNKER storages) -----
    df_cbp = df.copy()
    if "storage" in df_cbp.columns and match_mode != "code":
        df_cbp = df_cbp[~df_cbp["storage"].isin(BUNKER_STORAGES)]
    df_cbp["SAAD CBP"]    = df_cbp[col_cbp] if (col_cbp in df_cbp.columns) else 0
    df_cbp["SAP COLGATE"] = df_cbp[col_sap] if (col_sap in df_cbp.columns) else 0
    df_cbp["DIFERENCIA"]  = df_cbp["SAAD CBP"] - df_cbp["SAP COLGATE"]
    out_cbp_cols = base_cols + ["SAAD CBP", "SAP COLGATE", "DIFERENCIA"]
    df_cbp_out = df_cbp[out_cbp_cols].copy() if out_cbp_cols else pd.DataFrame()
    if not df_cbp_out.empty:
        df_cbp_out = df_cbp_out.sort_values("DIFERENCIA", key=lambda s: s.abs(), ascending=False)

    # ----- SAAD BUNKER vs SAP (SOLO BUNKER storages) -----
    df_bun = df.copy()
    if "storage" in df_bun.columns and match_mode != "code":
        df_bun = df_bun[df_bun["storage"].isin(BUNKER_STORAGES)]
    df_bun["SAAD BUNKER"] = df_bun[col_bunker] if (col_bunker in df_bun.columns) else 0
    df_bun["SAP COLGATE"] = df_bun[col_sap]    if (col_sap in df_bun.columns)    else 0
    df_bun["DIFERENCIA"]  = df_bun["SAAD BUNKER"] - df_bun["SAP COLGATE"]
    out_bun_cols = base_cols + ["SAAD BUNKER", "SAP COLGATE", "DIFERENCIA"]
    df_bun_out = df_bun[out_bun_cols].copy() if out_bun_cols else pd.DataFrame()
    if not df_bun_out.empty:
        df_bun_out = df_bun_out.sort_values("DIFERENCIA", key=lambda s: s.abs(), ascending=False)

    # Resumen
    resumen = res.get("resumen", {})
    resumen_df = pd.DataFrame([{
        "total_claves": resumen.get("total_claves", 0),
        "con_diferencias": resumen.get("con_diferencias", 0),
        "sin_diferencias": resumen.get("sin_diferencias", 0),
        "archivos": ", ".join(resumen.get("archivos", [])),
    }])

    # Escribir Excel
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        df_cbp_out.to_excel(w, sheet_name="SAAD CBP vs SAP", index=False)
        df_bun_out.to_excel(w, sheet_name="SAAD BUNKER vs SAP", index=False)
        resumen_df.to_excel(w, sheet_name="Resumen", index=False)

        for sht in ["SAAD CBP vs SAP", "SAAD BUNKER vs SAP"]:
            try:
                ws = w.sheets[sht]
                if match_mode == "code":
                    ws.set_column(0, 0, 18)   # codigo
                    ws.set_column(1, 1, 42)   # descripcion
                    ws.set_column(2, 4, 16)   # cifras
                else:
                    ws.set_column(0, 0, 18)   # codigo
                    ws.set_column(1, 1, 10)   # storage
                    ws.set_column(2, 2, 42)   # descripcion
                    ws.set_column(3, 5, 16)   # cifras
            except Exception:
                pass

    buf.seek(0)
    return buf.read()

# =========================
# Endpoints
# =========================
@app.post("/cruce")
async def cruce(
    files: List[UploadFile] = File(...),
    code_col: Optional[str] = None,
    storage_col: Optional[str] = None,
    desc_col: Optional[str] = None,
    qty_col: Optional[str] = None,
    only_storage: Optional[str] = None,
    header_row: Optional[int] = None,
    match_mode: str = "code",  # default por código
    bunker_storages: Optional[str] = None,  # opcional; sobreescribe si lo pasás
):
    try:
        global BUNKER_STORAGES
        if bunker_storages:
            BUNKER_STORAGES = {s.strip().upper() for s in bunker_storages.split(",") if s.strip()}

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
            match_mode=match_mode,
        )
    except Exception as e:
        logger.exception("Error en /cruce")
        return JSONResponse(status_code=500, content={"detail": f"/cruce: {e}"})

@app.post(
    "/cruce-xlsx",
    responses={200: {
        "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        "description": "Cruce en Excel (dos pestañas: SAAD CBP vs SAP / SAAD BUNKER vs SAP).",
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
    match_mode: str = "code",  # default por código
    bunker_storages: Optional[str] = None,
):
    try:
        global BUNKER_STORAGES
        if bunker_storages:
            BUNKER_STORAGES = {s.strip().upper() for s in bunker_storages.split(",") if s.strip()}

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
            match_mode=match_mode,
        )
        xlsx_bytes = _xlsx_from_res(res, match_mode=match_mode)
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
    return {"ok": True, "python": sys.version.split()[0], "pandas": _pd.__version__, "bunker_storages": list(BUNKER_STORAGES)}
