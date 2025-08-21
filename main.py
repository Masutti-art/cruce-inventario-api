from typing import List, Dict, Any
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import io, zipfile, csv, os

app = FastAPI(title="Cruce Inventario API", version="3.0.0")

REQUIRED_SHEET = "Comparacion Inventario"
FALLBACK_SHEETS = ["Comparación Inventario", "Data"]

# ---------------- CSV ----------------
def sniff_csv_delimiter(sample: bytes) -> str:
    try:
        txt = sample.decode("utf-8", errors="ignore")
        dialect = csv.Sniffer().sniff(txt[:2048], delimiters=[",",";","\t","|"])
        return dialect.delimiter
    except Exception:
        s = sample[:4096].decode("utf-8", errors="ignore")
        if s.count(";") > s.count(","): return ";"
        if "\t" in s: return "\t"
        return ","

def read_csv_like(file_bytes: bytes) -> pd.DataFrame:
    delim = sniff_csv_delimiter(file_bytes)
    return pd.read_csv(io.BytesIO(file_bytes), delimiter=delim)

# ---------------- Excel helpers ----------------
def pick_sheet_name(xl: pd.ExcelFile) -> str:
    sheets = set(xl.sheet_names)
    if REQUIRED_SHEET in sheets:
        return REQUIRED_SHEET
    for fb in FALLBACK_SHEETS:
        if fb in sheets:
            return fb
    norm_req = " ".join(REQUIRED_SHEET.lower().split())
    for s in xl.sheet_names:
        if " ".join(s.lower().split()) == norm_req:
            return s
    if xl.sheet_names:
        return xl.sheet_names[0]
    raise HTTPException(status_code=400, detail="El archivo Excel no contiene hojas.")

def read_xlsx(file_bytes: bytes) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))  # openpyxl por defecto
        sheet = pick_sheet_name(xl)
        return pd.read_excel(xl, sheet_name=sheet)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer .xlsx: {str(e)}")

def read_xls(file_bytes: bytes) -> pd.DataFrame:
    try:
        df_dict = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, engine="xlrd")
        # elegir hoja
        sheet = REQUIRED_SHEET if REQUIRED_SHEET in df_dict else None
        if not sheet:
            for fb in FALLBACK_SHEETS:
                if fb in df_dict:
                    sheet = fb
                    break
        if not sheet:
            sheet = next(iter(df_dict.keys()))
        return df_dict[sheet]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer .xls: {str(e)}")

def read_xlsb(file_bytes: bytes) -> pd.DataFrame:
    try:
        tmp = "/tmp/upload.xlsb"
        with open(tmp, "wb") as f:
            f.write(file_bytes)
        xl = pd.ExcelFile(tmp, engine="pyxlsb")
        sheet = pick_sheet_name(xl)
        df = pd.read_excel(xl, sheet_name=sheet, engine="pyxlsb")
        try: os.remove(tmp)
        except Exception: pass
        return df
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer .xlsb: {str(e)}")

def read_ods(file_bytes: bytes) -> pd.DataFrame:
    try:
        df_dict = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None, engine="odf")
        sheet = REQUIRED_SHEET if REQUIRED_SHEET in df_dict else None
        if not sheet:
            for fb in FALLBACK_SHEETS:
                if fb in df_dict:
                    sheet = fb
                    break
        if not sheet:
            sheet = next(iter(df_dict.keys()))
        return df_dict[sheet]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo leer .ods: {str(e)}")

def read_zip_single_table(file_bytes: bytes) -> pd.DataFrame:
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            candidates = [n for n in zf.namelist()
                          if not n.endswith("/") and
                          n.lower().split(".")[-1] in ("xlsx","xls","xlsb","ods","csv","tsv","txt")]
            if len(candidates) != 1:
                raise HTTPException(status_code=400,
                    detail=f"El .zip debe contener exactamente 1 archivo tabular (encontrados: {len(candidates)}).")
            inner = zf.read(candidates[0])
            return read_any_table(inner, candidates[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"ZIP inválido: {str(e)}")

def read_any_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    ext = filename.lower().split(".")[-1]
    if ext == "xlsx": return read_xlsx(file_bytes)
    if ext == "xls":  return read_xls(file_bytes)        # <-- usa xlrd
    if ext == "xlsb": return read_xlsb(file_bytes)
    if ext == "ods":  return read_ods(file_bytes)
    if ext in ("csv", "tsv", "txt"): return read_csv_like(file_bytes)
    if ext == "zip":  return read_zip_single_table(file_bytes)
    try:
        return read_xlsx(file_bytes)  # último intento
    except Exception:
        raise HTTPException(status_code=400,
            detail=f"Extensión no soportada: .{ext}. Usa .xlsx/.xls/.xlsb/.ods/.csv/.tsv/.txt o .zip con 1 archivo.")

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

# ---------------- Endpoints simples ----------------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    raw = await file.read()
    if len(raw) > 20 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Archivo demasiado grande (>20MB).")
    try:
        df = read_any_table(raw, file.filename or "upload.bin")
        df = normalize_df(df)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No se pudo procesar el archivo: {str(e)}")

    return JSONResponse({
        "archivo": file.filename,
        "filas": int(len(df)),
        "columnas": int(len(df.columns)),
        "columnas_detectadas": list(map(str, df.columns))[:30],
        "status": "ok"
    })
def _alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    alias = {
        # --- codigo ---
        "material": "codigo", "codigo": "codigo", "sku": "codigo", "item": "codigo",
        "item code": "codigo", "codigo articulo": "codigo", "cod articulo": "codigo",
        "cod": "codigo", "material code": "codigo", "sap code": "codigo",
        "ubprod": "codigo",  # BUNKER

        # --- descripcion ---
        "material description": "descripcion", "description": "descripcion",
        "descripción": "descripcion", "descripcion": "descripcion",
        "item name": "descripcion", "product": "descripcion", "producto": "descripcion",
        "nombre": "descripcion",
        "itdesc": "descripcion",  # BUNKER

        # --- storage / deposito ---
        "storage location": "storage", "storage": "storage", "almacen": "storage",
        "deposito": "storage", "depósito": "storage", "warehouse": "storage",
        "ubicacion": "storage", "ubicación": "storage", "location": "storage",
        "ubiest": "storage",    # BUNKER
        "emplaza": "storage",   # BUNKER variante nueva
        "estante": "storage",   # BUNKER variante nueva
        "columna": "storage",   # BUNKER variante nueva

        # --- cantidad (cajas) ---
        "cajas": "cajas", "bultos": "cajas", "bum quantity": "cajas",
        "qty": "cajas", "cantidad": "cajas", "cantidad cajas": "cajas",
        "cant cajas": "cajas", "boxes": "cajas", "box qty": "cajas",
        "cartones": "cajas", "ctns": "cajas",
        "ubcfisi": "cajas",     # BUNKER
    }

    # aplicar los alias
    df = df.rename(columns=lambda c: alias.get(c, c))

    return df
def _prepare_input_df(raw_df: pd.DataFrame, archivo: str) -> pd.DataFrame:
    """
    - Normaliza nombres y aplica alias
    - Valida columnas mínimas: codigo + storage (obligatorias)
    - Crea columnas opcionales si faltan: cajas=0, descripcion=""
    - Tipifica y agrupa por (codigo, storage) sumando cajas
    """
    df = normalize_df(raw_df)
    df = _alias_columns(df)

    # --- columnas mínimas obligatorias ---
    cols_min = {"codigo", "storage"}
    if not cols_min.issubset(df.columns):
        faltan = sorted(list(cols_min - set(df.columns)))
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"El archivo '{archivo}' no contiene columnas mínimas",
                "faltan": faltan,
                "esperadas": ["codigo", "storage", "cajas (opcional)", "descripcion (opcional)"],
                "columnas_detectadas": list(df.columns),
            },
        )

    # --- columnas opcionales ---
    if "cajas" not in df.columns:
        df["cajas"] = 0
    if "descripcion" not in df.columns:
        df["descripcion"] = ""

    # --- tipificación ---
    df["codigo"] = df["codigo"].astype(str).str.strip()
    df["storage"] = df["storage"].astype(str).str.strip()

    def _to_float(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return 0.0

    df["cajas"] = df["cajas"].map(_to_float)

    # --- reordenar y agrupar ---
    df = df[["codigo", "storage", "cajas", "descripcion"]]
    df = (
        df.sort_values(["codigo", "storage"])
          .groupby(["codigo", "storage"], as_index=False)
          .agg({"cajas": "sum", "descripcion": "first"})
    )

    df["archivo"] = archivo
    return df
@app.post("/cruce")
async def cruce_archivos(files: List[UploadFile] = File(...)) -> Dict[str, Any]:
    if len(files) < 2:
        raise HTTPException(status_code=400, detail="Sube al menos 2 archivos para cruzar.")
    cargados = []
    for up in files:
        raw = await up.read()
        try:
            base_name = (up.filename or "archivo").rsplit(".", 1)[0]
            df_raw = read_any_table(raw, up.filename or "upload.bin")
            df_pre = _prepare_input_df(df_raw, up.filename or base_name)
            df_pre = df_pre.rename(columns={"cajas": f"cajas_{base_name}"})
            cargados.append((base_name, df_pre))
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"No pude procesar '{up.filename}': {str(e)}")
    merged = None
    for i, (name, df_i) in enumerate(cargados):
        if merged is None:
            merged = df_i
        else:
            merged = pd.merge(
                merged,
                df_i[["codigo", "storage", f"cajas_{name}"]],
                on=["codigo", "storage"],
                how="outer",
            )
    if "descripcion" not in merged.columns:
        merged["descripcion"] = None
    for (_, df_i) in cargados[1:]:
        if "descripcion" in df_i.columns:
            merged["descripcion"] = merged["descripcion"].fillna(
                pd.merge(
                    merged[["codigo", "storage"]],
                    df_i[["codigo", "storage", "descripcion"]],
                    on=["codigo", "storage"],
                    how="left",
                )["descripcion"]
            )
    caja_cols = [c for c in merged.columns if c.startswith("cajas_")]
    for c in caja_cols:
        merged[c] = merged[c].fillna(0)
    base_name = cargados[0][0]
    base_col = f"cajas_{base_name}"
    diff_cols = []
    for name, _ in cargados[1:]:
        col = f"cajas_{name}"
        dcol = f"diff_{name}_vs_{base_name}"
        merged[dcol] = merged[col] - merged[base_col]
        diff_cols.append(dcol)

    diffs_only = merged.loc[(merged[diff_cols] != 0).any(axis=1)] if diff_cols else merged.copy()
    resumen = {
        "archivos": [n for (n, _) in cargados],
        "total_claves": int(len(merged)),
        "con_diferencias": int(len(diffs_only)),
        "sin_diferencias": int(len(merged) - len(diffs_only)),
    }
    cols_order = ["codigo", "descripcion", "storage"] + caja_cols + diff_cols
    merged = merged[cols_order].sort_values(["codigo", "storage"]).reset_index(drop=True)
    diffs_only = diffs_only[cols_order].sort_values(["codigo", "storage"]).reset_index(drop=True)

    return {
        "resumen": resumen,
        "diferencias": diffs_only.to_dict(orient="records"),
        "todo": merged.to_dict(orient="records"),
    }
