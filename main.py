# main.py
from __future__ import annotations
import io
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI(title="Cruce Inventario API", version="2.0.0")
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
            best_df = None
            best_score = -1
            for hdr in range(0, 31):
                try:
                    df = _read_excel_try(file.file, engine, hdr)
                    if df.empty:
                        continue
                    # score por cantidad de códigos detectables
                    tmp = df.dropna(how="all")
                    if tmp.empty:
                        continue
                    # heurística: cuántos "códigos" reconoce
                    code_col = _guess_col(df, CODE_CANDIDATES)
                    score = int(df[code_col].notna().sum()) if code_col else 0
                    if score > best_score:
                        best_score = score
                        best_df = df
                except Exception:
                    continue
            if best_df is not None:
                return best_df
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

# ====== Conversión robusta de cantidades ======
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
# Clasificación automática
# =========================
def _classify_label(df: pd.DataFrame, filename: str) -> str:
    name = (filename or "").lower()

    cols_norm = {_sanitize(c) for c in df.columns}

    has_ubprod = any("ubprod" in c for c in cols_norm)
    has_ubcstk = any("ubcstk" in c for c in cols_norm)
    has_ubiest = any("ubiest" in c for c in cols_norm)

    has_material = any("material" in c for c in cols_norm)
    has_storage_location = any("storage location" in c for c in cols_norm) or any("storage_location" in c for c in cols_norm)
    has_bum = any("bum quantity" in c for c in cols_norm) or any("quantity" in c for c in cols_norm)

    # Palabras clave del nombre del archivo
    if "bunker" in name:
        return "BUNKER"
    if "cbp" in name:
        return "CBP"
    if "sap" in name:
        return "SAP"

    # Por contenido:
    if has_ubprod or has_ubcstk or has_ubiest:
        # decidir CBP vs BUNKER por depósitos
        st_col = _guess_col(df, STO_CANDIDATES)
        if st_col:
            stor = df[st_col].astype(str).str.upper().str.strip()
            non_empty = stor[stor != ""]
            if len(non_empty) > 0:
                share_bunker = (non_empty.isin(BUNKER_STORAGES)).mean()
                if share_bunker >= 0.5:
                    return "BUNKER"
                else:
                    return "CBP"
        # fallback
        return "CBP"
    if has_material and has_storage_location and has_bum:
        return "SAP"

    # último intento por nombres
    if "saad" in name and "bunker" in name:
        return "BUNKER"
    if "saad" in name and "cbp" in name:
        return "CBP"
    return "SAP"  # mejor baseline

# =========================
# Motor de cruce común
# =========================
def _merge_and_build_result(
    labelled_dfs: List[Tuple[pd.DataFrame, str]],
    match_mode: str = "code",
) -> Dict:
    # normalizar nombres de columnas por label
    normed = []
    etiquetas = []
    for (dfn, lbl) in labelled_dfs:
        etiquetas.append(lbl)
        d = dfn.rename(columns={"cajas": f"cajas_{lbl}", "descripcion": f"desc_{lbl}"})
        normed.append(d)

    base_keys = ["codigo", "storage"] if match_mode != "code" else ["codigo"]

    # si match_mode='code', consolidar cada DF por código
    prepped = []
    for nd in normed:
        qty_col = [c for c in nd.columns if c.startswith("cajas_")][0]
        desc_col = [c for c in nd.columns if c.startswith("desc_")][0]
        if match_mode == "code":
            nd = nd.groupby("codigo", as_index=False).agg({
                qty_col: "sum",
                "storage": lambda s: ",".join(sorted({str(x) for x in s if str(x).strip()}))[:60],
                desc_col: "first",
            })
        prepped.append(nd)

    merged: Optional[pd.DataFrame] = None
    for nd in prepped:
        merged = nd if merged is None else merged.merge(nd, on=base_keys, how="outer")

    if merged is None or merged.empty:
        return {"resumen": {"archivos": etiquetas, "total_claves": 0, "con_diferencias": 0, "sin_diferencias": 0}, "diferencias": []}

    # numéricos
    for c in [c for c in merged.columns if c.startswith("cajas_")]:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    # descripción preferida: SAP > otras
    merged["descripcion"] = ""
    if "desc_SAP" in merged.columns:
        merged["descripcion"] = merged["desc_SAP"].astype(str)

    def _choose_desc(row):
        cur = str(row.get("descripcion", "")).strip()
        if not _looks_bad_desc(cur):
            return cur
        for c in merged.columns:
            if c.startswith("desc_"):
                val = str(row.get(c, "")).strip()
                if not _looks_bad_desc(val):
                    return val
        return cur

    merged["descripcion"] = merged.apply(_choose_desc, axis=1)
    if "codigo" in merged.columns:
        merged = merged[merged["codigo"].notna() & (merged["codigo"].astype(str).str.strip() != "")]

    # resumen
    total = int(len(merged))
    # baseline SAP si existe
    baseline_label = "SAP" if "cajas_SAP" in merged.columns else [c.replace("cajas_", "") for c in merged.columns if c.startswith("cajas_")][0]
    diff_cols = []
    for c in [c for c in merged.columns if c.startswith("cajas_")]:
        lbl = c.replace("cajas_", "")
        if lbl == baseline_label:
            continue
        dname = f"diff_{lbl}_vs_{baseline_label}"
        merged[dname] = merged[c] - merged[f"cajas_{baseline_label}"]
        diff_cols.append(dname)
    merged["dif_mayor_abs"] = merged[diff_cols].abs().max(axis=1) if diff_cols else 0
    con_dif = int(merged[diff_cols].ne(0).any(axis=1).sum()) if diff_cols else 0
    sin_dif = int(total - con_dif)

    return {
        "resumen": {
            "archivos": etiquetas,
            "total_claves": total,
            "con_diferencias": con_dif,
            "sin_diferencias": sin_dif,
        },
        "diferencias": merged.to_dict(orient="records"),
    }

# =========================
# Excel: dos pestañas + resumen
# =========================
def _xlsx_from_res(res: Dict, match_mode: str = "code") -> bytes:
    df = pd.DataFrame(res.get("diferencias", []))

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        # si no hay datos, hojas vacías
        if df.empty:
            pd.DataFrame(columns=["codigo","descripcion","SAAD CBP","SAP COLGATE","DIFERENCIA"]).to_excel(w, "SAAD CBP vs SAP", index=False)
            pd.DataFrame(columns=["codigo","descripcion","SAAD BUNKER","SAP COLGATE","DIFERENCIA"]).to_excel(w, "SAAD BUNKER vs SAP", index=False)
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

        for c in [col for col in [col_sap, col_cbp, col_bunker] if col is not None]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        for c in ["codigo","storage","descripcion"]:
            if c in df.columns:
                df[c] = df[c].astype(str).replace({"nan":"", "NaN":"", "NAN":""}).fillna("")

        base_cols = ["codigo","descripcion"] if match_mode=="code" else ["codigo","storage","descripcion"]

        # CBP vs SAP (sin depósitos bunker)
        df_cbp = df.copy()
        df_cbp["SAAD CBP"]    = df_cbp[col_cbp] if col_cbp else 0
        df_cbp["SAP COLGATE"] = df_cbp[col_sap] if col_sap else 0
        df_cbp["DIFERENCIA"]  = df_cbp["SAAD CBP"] - df_cbp["SAP COLGATE"]
        out_cbp_cols = base_cols + ["SAAD CBP","SAP COLGATE","DIFERENCIA"]
        if not df_cbp.empty:
            df_cbp = df_cbp[out_cbp_cols].sort_values("DIFERENCIA", key=lambda s: s.abs(), ascending=False)
        df_cbp.to_excel(w, "SAAD CBP vs SAP", index=False)

        # BUNKER vs SAP (solo depósitos bunker ya resumidos por SKU)
        df_bun = df.copy()
        df_bun["SAAD BUNKER"] = df_bun[col_bunker] if col_bunker else 0
        df_bun["SAP COLGATE"] = df_bun[col_sap]    if col_sap else 0
        df_bun["DIFERENCIA"]  = df_bun["SAAD BUNKER"] - df_bun["SAP COLGATE"]
        out_bun_cols = base_cols + ["SAAD BUNKER","SAP COLGATE","DIFERENCIA"]
        if not df_bun.empty:
            df_bun = df_bun[out_bun_cols].sort_values("DIFERENCIA", key=lambda s: s.abs(), ascending=False)
        df_bun.to_excel(w, "SAAD BUNKER vs SAP", index=False)

        pd.DataFrame([res.get("resumen", {})]).to_excel(w, "Resumen", index=False)

        # format widths
        for sht in ["SAAD CBP vs SAP","SAAD BUNKER vs SAP"]:
            try:
                ws = w.sheets[sht]
                if match_mode=="code":
                    ws.set_column(0,0,18)
                    ws.set_column(1,1,42)
                    ws.set_column(2,4,16)
                else:
                    ws.set_column(0,0,18)
                    ws.set_column(1,1,10)
                    ws.set_column(2,2,42)
                    ws.set_column(3,5,16)
            except Exception:
                pass

    buf.seek(0)
    return buf.read()

# =========================
# Endpoint AUTO — sin parámetros
# =========================
@app.post(
    "/cruce-auto-xlsx",
    responses={200: {
        "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        "description": "Subí SAP, SAAD CBP y SAAD BUNKER. Auto-detecta todo y devuelve Excel.",
    }},
)
async def cruce_auto_xlsx(files: List[UploadFile] = File(...)):
    try:
        if not files or len(files) < 2:
            raise HTTPException(status_code=400, detail="Subí al menos 2 archivos (SAP y SAAD). Idealmente 3: SAP, SAAD CBP y SAAD BUNKER.")

        labelled: List[Tuple[pd.DataFrame,str]] = []
        seen_labels = set()

        # leer + normalizar + clasificar automáticamente
        for f in files:
            df_raw = _read_any_table(f, header_row=None)
            dfn = _normalize_df(df_raw)
            lbl = _classify_label(df_raw, f.filename or "")
            # si ya existe label (ej. 2 SAAD detectados iguales), intenta resolver:
            if lbl in seen_labels and lbl in {"CBP","BUNKER"}:
                # si el nombre del archivo ayuda, úsalo
                name = (f.filename or "").lower()
                if "bunker" in name:
                    lbl = "BUNKER"
                elif "cbp" in name:
                    lbl = "CBP"
                else:
                    # heurística por depósitos
                    st = set(dfn["storage"].dropna().astype(str).str.upper().unique())
                    share = sum([s in BUNKER_STORAGES for s in st]) / max(1,len(st))
                    lbl = "BUNKER" if share >= 0.5 else "CBP"

            labelled.append((dfn, lbl))
            seen_labels.add(lbl)

        # si falta SAP, intenta forzar por nombre de archivo
        if "SAP" not in {lbl for _,lbl in labelled}:
            for i,(dfn,lbl) in enumerate(labelled):
                if lbl not in {"CBP","BUNKER"}:
                    labelled[i] = (dfn,"SAP")
                    break

        # merge por código (match_mode fijo = code)
        res = _merge_and_build_result(labelled, match_mode="code")
        xlsx = _xlsx_from_res(res, match_mode="code")
        return StreamingResponse(
            io.BytesIO(xlsx),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
        )
    except Exception as e:
        logger.exception("Error en /cruce-auto-xlsx")
        raise HTTPException(status_code=500, detail=f"/cruce-auto-xlsx: {e}")

# =========================
# Endpoints manuales (siguen disponibles)
# =========================
@app.get("/healthz")
def healthz():
    import pandas as _pd
    return {"ok": True, "python": sys.version.split()[0], "pandas": _pd.__version__, "bunker_storages": list(BUNKER_STORAGES)}
