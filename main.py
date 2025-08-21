import io
import tempfile
from typing import List

import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
from openpyxl import load_workbook
from openpyxl.chart import BarChart, Reference
from openpyxl.utils import get_column_letter

app = FastAPI(title="Cruce Inventario API", version="3.1.0")

# --------------------------
# Config hojas preferidas
# --------------------------
REQUIRED_SHEET = "Comparacion Inventario"
FALLBACK_SHEETS = ["Comparación Inventario", "Data"]

def _pick_sheet(xl: pd.ExcelFile) -> str:
    """Selecciona la hoja preferida si existe; si no, la primera."""
    sheets = set(xl.sheet_names)
    if REQUIRED_SHEET in sheets:
        return REQUIRED_SHEET
    for fb in FALLBACK_SHEETS:
        if fb in sheets:
            return fb
    return xl.sheet_names[0] if xl.sheet_names else None

# --------------------------
# Lectura de archivos
# --------------------------
def _read_any_table_bytes(b: bytes) -> pd.DataFrame:
    """
    Lee XLSX / XLS / XLSB / CSV a DataFrame.
    - XLSX: por defecto openpyxl (via pandas)
    - XLS: xlrd (engine explícito)
    - XLSB: pyxlsb (engine explícito)
    - CSV: autodetect estándar de pandas
    """
    bio = io.BytesIO(b)
    header = b[:8]

    # XLSX (OOXML): empieza con "PK"
    if header.startswith(b"PK"):
        with pd.ExcelFile(bio) as xl:
            sheet = _pick_sheet(xl)
            return pd.read_excel(xl, sheet_name=sheet)

    # XLS (BIFF): magic D0 CF (Compound File)
    if header[:2] == b"\xD0\xCF":
        with pd.ExcelFile(bio, engine="xlrd") as xl:
            sheet = _pick_sheet(xl)
            return pd.read_excel(xl, sheet_name=sheet, engine="xlrd")

    # XLSB (Excel binario moderno): algunos headers típicos
    if header.startswith(b"\x09\x08\x10\x00") or header.startswith(b"\x09\x04\x06\x00"):
        with pd.ExcelFile(bio, engine="pyxlsb") as xl:
            sheet = _pick_sheet(xl)
            return pd.read_excel(xl, sheet_name=sheet, engine="pyxlsb")

    # CSV (fallback)
    try:
        return pd.read_csv(io.BytesIO(b))
    except Exception:
        raise ValueError("Formato no reconocido o archivo corrupto")

# --------------------------
# Normalización de columnas
# --------------------------
def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Nombres a minúscula
    - Alias -> ['codigo','storage','cajas','descripcion']
    - Crea 'storage' y/o 'cajas' si faltan
    """
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    alias = {
        # codigo
        "material": "codigo",
        "codigoproducto": "codigo",
        "sku": "codigo",
        "codigo": "codigo",
        # storage
        "almacen": "storage",
        "depósito": "storage",
        "deposito": "storage",
        "ubicacion": "storage",
        "ubicación": "storage",
        "storage": "storage",
        # cajas
        "cajas": "cajas",
        "bultos": "cajas",
        "cantidad": "cajas",
        "qty": "cajas",
        # descripcion
        "descripcionmaterial": "descripcion",
        "descripcion": "descripcion",
        "descripción": "descripcion",
        "desc": "descripcion",
    }
    df = df.rename(columns=lambda c: alias.get(c, c))

    if "codigo" not in df.columns:
        raise ValueError("Falta la columna obligatoria: 'codigo'")

    if "storage" not in df.columns:
        df["storage"] = ""

    if "cajas" not in df.columns:
        df["cajas"] = 0

    # Orden de columnas
    cols = ["codigo", "storage", "cajas"]
    if "descripcion" in df.columns:
        cols.append("descripcion")
    return df[cols]

# --------------------------
# Lógica de cruce
# --------------------------
def _merge_logic(dfs: dict) -> pd.DataFrame:
    """
    - SAP vs CBP => por ['codigo','storage']
    - SAP vs BUNKER => por ['codigo'] (SAP agregado por codigo)
    Devuelve un dataframe unificado con todas las comparaciones y columnas diff.
    """
    sap = dfs.get("sap")
    cbp = dfs.get("cbp")
    bunker = dfs.get("bunker")

    results = []

    # SAP vs CBP: por codigo + storage
    if sap is not None and cbp is not None:
        m = sap.merge(cbp, on=["codigo", "storage"], how="outer", suffixes=("_sap", "_cbp"))
        m["diff_cbp_vs_sap"] = m["cajas_cbp"].fillna(0) - m["cajas_sap"].fillna(0)
        results.append(m)

    # SAP vs BUNKER: por codigo (se ignora storage)
    if sap is not None and bunker is not None:
        sap_sum = sap.groupby("codigo", as_index=False).agg({"cajas": "sum", "descripcion": "first"})
        bunk_sum = bunker.groupby("codigo", as_index=False).agg({"cajas": "sum"})
        m = sap_sum.merge(bunk_sum, on="codigo", how="outer", suffixes=("_sap", "_bunker"))
        m["storage"] = ""  # columna para consistencia visual
        m["diff_bunker_vs_sap"] = m["cajas_bunker"].fillna(0) - m["cajas_sap"].fillna(0)
        results.append(m)

    if not results:
        raise ValueError("No hay suficientes archivos válidos para comparar (se necesitan SAP + CBP o SAP + BUNKER).")

    return pd.concat(results, ignore_index=True, sort=False)

# --------------------------
# Utilidades Excel
# --------------------------
def _autosize_columns(wb):
    """Auto‑ajuste de anchos en todas las hojas."""
    for ws in wb.worksheets:
        for col in ws.columns:
            max_len = max(len(str(c.value)) if c.value is not None else 0 for c in col)
            ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 2, 60)

def _add_chart(wb):
    """Grafico de barras con última columna diff en hoja Resumen (si existe Diferencias)."""
    if "Resumen" not in wb.sheetnames or "Diferencias" not in wb.sheetnames:
        return
    ws_diff = wb["Diferencias"]
    ws_res = wb["Resumen"]
    if ws_diff.max_row <= 1:
        return
    # Tomar la última columna (asumimos que es una diff)
    last_col = ws_diff.max_column
    data = Reference(ws_diff, min_col=last_col, min_row=1, max_row=ws_diff.max_row)
    chart = BarChart()
    chart.add_data(data, titles_from_data=True)
    chart.title = "Diferencias (última columna)"
    ws_res.add_chart(chart, "E5")

# --------------------------
# Endpoint
# --------------------------
@app.post("/cruce")
async def cruce(files: List[UploadFile] = File(...)):
    try:
        dfs = {}
        for f in files:
            name = (f.filename or "").lower()
            raw = await f.read()
            df = _normalize_df(_read_any_table_bytes(raw))

            # Detectar origen por nombre de archivo
            if "sap" in name:
                dfs["sap"] = df
            elif "cbp" in name:
                dfs["cbp"] = df
            elif "bunker" in name:
                dfs["bunker"] = df

        merged = _merge_logic(dfs)

        # Resumen
        diff_cols = [c for c in merged.columns if c.startswith("diff_")]
        df_dif = merged.loc[(merged[diff_cols] != 0).any(axis=1)] if diff_cols else merged.copy()
        resumen = {
            "total_claves": int(len(merged)),
            "con_diferencias": int(len(df_dif)),
            "sin_diferencias": int(len(merged) - len(df_dif)),
        }

        # Excel temporal
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp_name = tmp.name
        tmp.close()

        with pd.ExcelWriter(tmp_name, engine="openpyxl") as writer:
            pd.DataFrame([resumen]).to_excel(writer, sheet_name="Resumen", index=False)
            df_dif.to_excel(writer, sheet_name="Diferencias", index=False)
            merged.to_excel(writer, sheet_name="Todo", index=False)

            # Totales por storage si aplica
            if "storage" in merged.columns:
                caja_cols = [c for c in merged.columns if c.startswith("cajas_")]
                if caja_cols:
                    tot = merged.groupby(["storage"], as_index=False)[caja_cols].sum()
                    tot.to_excel(writer, sheet_name="Totales_por_storage", index=False)

        # Post-procesamiento con openpyxl
        wb = load_workbook(tmp_name)
        _autosize_columns(wb)
        _add_chart(wb)
        wb.save(tmp_name)

        return FileResponse(
            tmp_name,
            filename="reporte_cruce.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"detail": str(e)})

@app.get("/health")
def health():
    return {"status": "ok"}
