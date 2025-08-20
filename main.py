from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import io, zipfile, csv, os

app = FastAPI(title="Cruce Inventario API", version="3.0.0")

REQUIRED_SHEET = "Comparacion Inventario"
FALLBACK_SHEETS = ["Comparación Inventario", "Data"]

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
        sheet = REQUIRED_SHEET if REQUIRED_SHEET in df_dict else None
        if not sheet:
            for fb in [*FALLBACK_SHEETS]:
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
            for fb in [*FALLBACK_SHEETS]:
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
    if ext == "xls":  return read_xls(file_bytes)
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
    df.columns = [str(c).strip() for c in df.columns]
    return df

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
    from fastapi import UploadFile, File
import pandas as pd
import io

@app.post("/cruce")
async def cruce_archivos(files: list[UploadFile] = File(...)):
    """
    Recibe 2 o más archivos Excel/CSV y devuelve cruce por Codigo+Storage
    """

    dataframes = []
    for file in files:
        contents = await file.read()
        try:
            if file.filename.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(contents))
            else:  # Excel (xlsx, xls, xlsb, etc.)
                df = pd.read_excel(io.BytesIO(contents))
        except Exception as e:
            return {"error": f"No se pudo leer {file.filename}: {str(e)}"}

        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.lower()
        rename_map = {
            "material": "codigo",
            "codigo": "codigo",
            "description": "descripcion",
            "material description": "descripcion",
            "storage location": "storage",
            "almacen": "storage",
            "bultos": "cajas",
            "cajas": "cajas",
            "bum quantity": "cajas"
        }
        df = df.rename(columns=rename_map)

        # Solo columnas necesarias
        cols_needed = ["codigo", "descripcion", "storage", "cajas"]
        df = df[[c for c in cols_needed if c in df.columns]]
        df["archivo"] = file.filename
        dataframes.append(df)

    if len(dataframes) < 2:
        return {"error": "Necesitas al menos 2 archivos para cruzar."}

    # Unir por codigo + storage
    resultado = dataframes[0]
    for df in dataframes[1:]:
        resultado = pd.merge(
            resultado,
            df,
            on=["codigo", "storage"],
            how="outer",
            suffixes=("", "_"+df["archivo"].iloc[0].split('.')[0])
        )

    # Rellenar vacíos con 0
    resultado = resultado.fillna(0)

    # Convertir a lista de dicts
    return resultado.to_dict(orient="records")

