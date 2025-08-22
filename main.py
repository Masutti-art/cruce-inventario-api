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
# Utilidades de lectura
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
    # 1) exact
    for c in cand:
        if c in norm:
            return norm[c]
    # 2) contiene
    for c in cand:
        for k, orig in norm.items():
            if c in k:
                # si piden numeric, verificamos
                if not numeric or pd.api.types.is_numeric_dtype(df[orig]):
                    return orig
    # 3) si numeric, tomar la numérica con mayor suma absoluta
    if numeric:
        num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
        if num_cols:
            sums = (df[num_cols].abs().sum(numeric_only=True)).sort_values(ascending=False)
            if len(sums) > 0:
                return str(sums.index[0])
    return None

def _read_any_table(file: UploadFile) -> pd.DataFrame:
    name = file.filename or "archivo"
    suffix = Path(name).suffix.lower()
    try:
        if suffix == ".xls":
            df = pd.read_excel(file.file, engine="xlrd")
        elif suffix in (".xlsx", ".xlsm"):
            df = pd.read_excel(file.file, engine="openpyxl")
        elif suffix in (".csv", ".txt"):
            file.file.seek(0)
            df = pd.read_csv(file.file, sep=None, engine="python")
        else:
            # intenta como excel por defecto
            df = pd.read_excel(file.file)
    finally:
        try:
            file.file.seek(0)
        except Exception:
            pass
    # quitar filas totalmente vacías
    df = df.dropna(how="all")
    # normalizar encabezados (quitar espacios duplicados)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Adivinar columnas clave
    codigo_col = _guess_col(df, ["codigo", "cod", "sku", "material", "articulo", "item", "code"])
    storage_col = _guess_col(df, ["storage", "almacen", "dep", "deposito", "bodega", "warehouse", "ubicacion", "location", "st"])
    desc_col = _guess_col(df, ["descripcion", "description", "desc", "nombre", "name"])
    qty_col = _guess_col(df, ["cajas", "cantidad", "qty", "stock", "unidades", "cant", "saldo"], numeric=True)

    if not codigo_col:
        raise ValueError("No se encontró la columna de CÓDIGO en el archivo.")
    if not qty_col:
        raise ValueError("No se encontró la columna de CANTIDAD/CAJAS en el archivo.")

    out = pd.DataFrame()
    out["codigo"] = df[codigo_col].astype(str).str.strip()

    if storage_col:
        out["storage"] = df[storage_col].astype(str).str.strip()
    else:
        out["storage"] = ""  # clave simple

    if desc_col:
        out["descripcion"] = df[desc_col].astype(str)
    else:
        out["descripcion"] = ""

    # cantidad numérica
    out["cajas"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    # agrupar por clave
    out = (
        out.groupby(["codigo", "storage"], as_index=False)
           .agg({"descripcion": "first", "cajas": "sum"})
    )
    return out


# -----------------------
# Núcleo del cruce
# -----------------------
async def procesar_cruce(files: List[UploadFile]) -> Dict:
    if not files or len(files) < 2:
        raise HTTPException(status_code=400, detail="Subí al menos 2 archivos para cruzar.")

    etiquetas = []
    norm_dfs = []
    for f in files:
        base = Path(f.filename or "archivo").stem
        etiquetas.append(base)
        df_raw = _read_any_table(f)
        df_norm = _normalize_df(df_raw)
        norm_dfs.append(df_norm)

    # Unimos claves (outer) y añadimos la columna de cajas por archivo
    base_keys = ["codigo", "storage"]
    merged = None
    for label, nd in zip(etiquetas, norm_dfs):
        col = f"cajas_{label}"
        nd = nd.rename(columns={"cajas": col})
        if merged is None:
            merged = nd
        else:
            merged = merged.merge(nd, on=base_keys, how="outer")

    # Descripción: la primera no vacía
    if "descripcion" not in merged.columns:
        merged["descripcion"] = ""
    else:
        # si vinieron varias 'descripcion_x', tomamos la primera no-nula
        desc_cols = [c for c in merged.columns if c.startswith("descripcion")]
        if len(desc_cols) > 1:
            merged["descripcion"] = merged[desc_cols].bfill(axis=1).iloc[:, 0]
            merged.drop(columns=[c for c in desc_cols if c != "descripcion"], inplace=True)

    # Rellenar ausentes con 0 en todas las columnas de cajas
    cajas_cols = [c for c in merged.columns if c.startswith("cajas_")]
    for c in cajas_cols:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0)

    # Diffs contra el PRIMER archivo como baseline
    baseline = cajas_cols[0]
    diff_cols = []
    for c in cajas_cols[1:]:
        diff_name = f"diff_{c.replace('cajas_', '')}_vs_{baseline.replace('cajas_', '')}"
        merged[diff_name] = merged[c] - merged[baseline]
        diff_cols.append(diff_name)

    # Métrica auxiliar para ordenar
    if diff_cols:
        merged["dif_mayor_abs"] = merged[diff_cols].abs().max(axis=1)
    else:
        merged["dif_mayor_abs"] = 0

    # Resumen
    total = int(len(merged))
    con_dif = int(merged[diff_cols].ne(0).any(axis=1).sum()) if diff_cols else 0
    sin_dif = int(total - con_dif)

    # Salida JSON
    visibles = ["codigo", "storage", "descripcion"] + cajas_cols + diff_cols
    merged_vis = merged[visibles].copy()

    # convertir floats enteros a int para que quede prolijo
    def _maybe_int(v):
        try:
            return int(v) if float(v).is_integer() else float(v)
        except Exception:
            return v
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
# Endpoints
# -----------------------
@app.post("/cruce")
async def cruce(files: List[UploadFile] = File(...)):
    """
    Devuelve JSON: resumen + listado de diferencias por clave.
    """
    try:
        return await procesar_cruce(files)
    except Exception as e:
        logger.exception("Error en /cruce")
        return JSONResponse(status_code=500, content={"detail": f"/cruce: {e}"})


def _xlsx_from_res(res: Dict) -> bytes:
    df = pd.DataFrame(res.get("diferencias", []))
    # ordenar por mayor diferencia absoluta
    diff_cols = [c for c in df.columns if str(c).startswith("diff_")]
    if diff_cols:
        df["dif_mayor_abs"] = df[diff_cols].abs().max(axis=1)
        df = df[df["dif_mayor_abs"] != 0].sort_values("dif_mayor_abs", ascending=False)
        # mover la métrica al principio
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

@app.post(
    "/cruce-xlsx",
    responses={200: {
        "content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}},
        "description": "Cruce en Excel (ordenado por mayor diferencia)",
    }},
)
async def cruce_xlsx(files: List[UploadFile] = File(...)):
    """
    Devuelve un .xlsx descargable con:
    - Hoja 'Diferencias' ordenada de mayor a menor por dif_mayor_abs (solo filas != 0)
    - Hoja 'Resumen' con contadores
    """
    try:
        res = await procesar_cruce(files)
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

