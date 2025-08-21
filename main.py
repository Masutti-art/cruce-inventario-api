# main.py
from typing import List, Dict, Any
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi import Response
from starlette.responses import StreamingResponse

import pandas as pd
import io, zipfile, csv, os, tempfile

app = FastAPI(title="Cruce Inventario API", version="4.0.0")

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
        xl = pd.ExcelFile(io.BytesIO(file_bytes))  # openpyxl default
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
    if ext == "xls":  return read_xls(file_bytes)        # xlrd >= 2.0.1
    if ext == "xlsb": return read_xlsb(file_bytes)
    if ext == "ods":  return read_ods(file_bytes)
    if ext in ("csv", "tsv", "txt"): return read_csv_like(file_bytes)
    if ext == "zip":  return read_zip_single_table(file_bytes)
    try:
        return read_xlsx(file_bytes)  # fallback
    except Exception:
        raise HTTPException(status_code=400,
            detail=f"Extensión no soportada: .{ext}. Usa .xlsx/.xls/.xlsb/.ods/.csv/.tsv/.txt o .zip con 1 archivo.")


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ---------------- Aliases + preparación ----------------
def _alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    alias = {
        # codigo
        "material": "codigo", "codigo": "codigo", "sku": "codigo", "item": "codigo",
        "item code": "codigo", "codigo articulo": "codigo", "cod articulo": "codigo",
        "cod": "codigo", "material code": "codigo", "sap code": "codigo",
        "ubprod": "codigo",  # BUNKER

        # descripcion
        "material description": "descripcion", "description": "descripcion",
        "descripción": "descripcion", "descripcion": "descripcion",
        "item name": "descripcion", "product": "descripcion", "producto": "descripcion",
        "nombre": "descripcion",
        "itdesc": "descripcion",  # BUNKER

        # storage / deposito
        "storage location": "storage", "storage": "storage", "almacen": "storage",
        "deposito": "storage", "depósito": "storage", "warehouse": "storage",
        "ubicacion": "storage", "ubicación": "storage", "location": "storage",
        "ubiest": "storage",  # BUNKER
        # variantes bunker nuevas
        "emplaza": "storage", "estante": "storage", "columna": "storage",

        # cantidad (cajas)
        "cajas": "cajas", "bultos": "cajas", "bum quantity": "cajas",
        "qty": "cajas", "cantidad": "cajas", "cantidad cajas": "cajas",
        "cant cajas": "cajas", "boxes": "cajas", "box qty": "cajas",
        "cartones": "cajas", "ctns": "cajas",
        "ubcfisi": "cajas",  # BUNKER
    }

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

    if "cajas" not in df.columns:
        df["cajas"] = 0
    if "descripcion" not in df.columns:
        df["descripcion"] = ""

    df["codigo"] = df["codigo"].astype(str).str.strip()
    df["storage"] = df["storage"].astype(str).str.strip()

    def _to_float(x):
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return 0.0

    df["cajas"] = df["cajas"].map(_to_float)

    df = df[["codigo", "storage", "cajas", "descripcion"]]
    df = (
        df.sort_values(["codigo", "storage"])
          .groupby(["codigo", "storage"], as_index=False)
          .agg({"cajas": "sum", "descripcion": "first"})
    )

    df["archivo"] = archivo
    return df


# ---------------- Endpoints base ----------------
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


# ---------------- Cruce por codigo+storage (JSON) ----------------
@app.post("/cruce")
async def cruce_archivos(files: List[UploadFile] = File(...)) -> Dict[str, Any]:
    """
    Cruce general por (codigo, storage) para todos los archivos.
    Devuelve JSON con merged y diferencias.
    """
    if len(files) < 2:
        raise HTTPException(status_code=400, detail="Sube al menos 2 archivos para cruzar.")

    dataframes = []
    for file in files:
        contents = await file.read()
        try:
            if not file.filename:
                raise ValueError("Archivo sin nombre")
            df_raw = read_any_table(contents, file.filename)
            df = _prepare_input_df(df_raw, file.filename)
        except Exception as e:
            return {"error": f"No se pudo leer {file.filename}: {str(e)}"}

        dataframes.append((file.filename.rsplit(".",1)[0], df))

    resultado = dataframes[0][1]
    nombres = [dataframes[0][0]]
    for name, df in dataframes[1:]:
        nombres.append(name)
        resultado = pd.merge(
            resultado,
            df[["codigo", "storage", "cajas"]],
            on=["codigo", "storage"],
            how="outer",
            suffixes=("", f"_{name}")
        )

    # Rellena NaN
    for c in resultado.columns:
        if c.startswith("cajas"):
            resultado[c] = resultado[c].fillna(0)

    # Calcula difs contra primera columna de cajas
    base_col = "cajas"
    diff_cols = []
    for name in nombres[1:]:
        col = f"cajas_{name}"
        dcol = f"diff_{name}_vs_{nombres[0]}"
        resultado[dcol] = resultado[col] - resultado[base_col]
        diff_cols.append(dcol)

    diffs_only = resultado.loc[(resultado[diff_cols] != 0).any(axis=1)] if diff_cols else resultado.copy()

    resumen = {
        "archivos": nombres,
        "total_claves": int(len(resultado)),
        "con_diferencias": int(len(diffs_only)),
        "sin_diferencias": int(len(resultado) - len(diffs_only)),
    }

    return {
        "resumen": resumen,
        "diferencias": diffs_only.to_dict(orient="records"),
        "todo": resultado.to_dict(orient="records"),
    }


# ---------------- Cruce por codigo+storage (EXCEL) ----------------
@app.post("/cruce/xlsx")
async def cruce_archivos_xlsx(files: List[UploadFile] = File(...), min_diff: float = 0.0):
    """
    Igual que /cruce (por codigo+storage), pero devuelve Excel (.xlsx) con:
    - Resumen (+ gráfico)
    - Diferencias (filtrado por min_diff)
    - Todo
    """
    # Reutilizamos la lógica de /cruce
    out_json = await cruce_archivos(files)  # type: ignore
    if "error" in out_json:
        raise HTTPException(status_code=400, detail=str(out_json["error"]))

    resumen = pd.DataFrame([out_json["resumen"]])
    df_todo = pd.DataFrame(out_json["todo"])
    df_diffs = pd.DataFrame(out_json["diferencias"])

    if min_diff and not df_diffs.empty:
        diff_cols = [c for c in df_diffs.columns if c.startswith("diff_")]
        if diff_cols:
            mask = (df_diffs[diff_cols].abs() >= float(min_diff)).any(axis=1)
            df_diffs = df_diffs.loc[mask]

    # Excel temporal con openpyxl + gráfico
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    with pd.ExcelWriter(tmp.name, engine="openpyxl") as writer:
        resumen.to_excel(writer, sheet_name="Resumen", index=False)
        df_diffs.to_excel(writer, sheet_name="Diferencias", index=False)
        df_todo.to_excel(writer, sheet_name="Todo", index=False)

    # Insertamos gráfico simple en Resumen
    from openpyxl import load_workbook
    from openpyxl.chart import BarChart, Reference
    wb = load_workbook(tmp.name)
    ws = wb["Resumen"]
    try:
        hdrs = {ws.cell(row=1, column=col).value: col for col in range(1, 30)}
        # columnas: total_claves, con_diferencias, sin_diferencias
        metric_cols = [hdrs.get("total_claves"), hdrs.get("con_diferencias"), hdrs.get("sin_diferencias")]
        metric_cols = [c for c in metric_cols if c]
        if metric_cols:
            chart = BarChart()
            chart.title = "Resumen Cruce (codigo+storage)"
            chart.y_axis.title = "Cantidad"
            chart.x_axis.title = "Métrica"
            data = Reference(ws, min_col=min(metric_cols), min_row=1, max_col=max(metric_cols), max_row=2)
            cats = Reference(ws, min_col=min(metric_cols), min_row=1, max_col=max(metric_cols), max_row=1)
            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)
            chart.height = 10
            chart.width = 22
            ws.add_chart(chart, "H2")
    except Exception:
       
