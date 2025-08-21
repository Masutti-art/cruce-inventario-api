from typing import List
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from starlette.background import BackgroundTask

import pandas as pd
import io
import csv
import tempfile  # <<< IMPORTANTE
import os

app = FastAPI(title="Cruce Inventario API", version="3.0.0")


# --------------------- Utilitarios de lectura ---------------------

def _sniff_csv_delimiter(sample: bytes) -> str:
    try:
        txt = sample.decode("utf-8", errors="ignore")
        dialect = csv.Sniffer().sniff(txt[:2048], delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        s = sample[:4096].decode("utf-8", errors="ignore")
        if s.count(";") > s.count(","):
            return ";"
        if "\t" in s:
            return "\t"
        return ","


def _read_any_table_bytes(filename: str, data: bytes) -> pd.DataFrame:
    ext = (filename or "").lower().rsplit(".", 1)[-1]
    bio = io.BytesIO(data)

    try:
        if ext == "xlsx":
            return pd.read_excel(bio)                              # openpyxl
        elif ext == "xls":
            return pd.read_excel(bio, engine="xlrd")               # xlrd==1.2.0
        elif ext == "xlsb":
            return pd.read_excel(bio, engine="pyxlsb")             # pyxlsb
        elif ext in ("csv", "tsv", "txt"):
            delim = _sniff_csv_delimiter(data)
            return pd.read_csv(bio, delimiter=delim)
        else:
            # último intento como xlsx
            bio.seek(0)
            return pd.read_excel(bio)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No pude leer '{filename}': {str(e)}")


# --------------------- Normalización / Aliases ---------------------

ALIASES = {
    # --- codigo ---
    "material": "codigo", "codigo": "codigo", "sku": "codigo", "item": "codigo",
    "item code": "codigo", "codigo articulo": "codigo", "cod articulo": "codigo",
    "cod": "codigo", "material code": "codigo", "sap code": "codigo", "product": "codigo",
    "ubprod": "codigo",  # BUNKER

    # --- descripcion ---
    "material description": "descripcion", "description": "descripcion",
    "descripción": "descripcion", "descripcion": "descripcion",
    "item name": "descripcion", "product description": "descripcion",
    "producto": "descripcion", "nombre": "descripcion", "itdesc": "descripcion",  # BUNKER

    # --- storage / deposito ---
    "storage location": "storage", "storage": "storage", "almacen": "storage",
    "almacén": "storage", "deposito": "storage", "depósito": "storage",
    "warehouse": "storage", "ubicacion": "storage", "ubicación": "storage",
    "location": "storage",
    "ubiest": "storage",                     # BUNKER
    "emplaza": "storage", "estante": "storage", "columna": "storage",  # BUNKER variantes

    # --- cantidad (cajas) ---
    "cajas": "cajas", "bultos": "cajas", "bum quantity": "cajas",
    "qty": "cajas", "cantidad": "cajas", "cantidad cajas": "cajas",
    "cant cajas": "cajas", "boxes": "cajas", "box qty": "cajas",
    "cartones": "cajas", "ctns": "cajas", "ubcfisi": "cajas",  # BUNKER
}


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.rename(columns=lambda c: ALIASES.get(c, c))
    return df


def _to_float(x) -> float:
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return 0.0


def _prepare_df(raw_df: pd.DataFrame, use_storage: bool, archivo: str) -> pd.DataFrame:
    """
    - Aplica aliases y normaliza nombres
    - Valida columnas mínimas: codigo (y storage si use_storage)
    - Crea columnas opcionales si faltan: cajas=0, descripcion=""
    - Tipifica, genera clave y agrupa
    """
    if raw_df is None:
        return None

    df = _normalize_df(raw_df)

    cols_min = {"codigo"}
    if use_storage:
        cols_min.add("storage")

    if not cols_min.issubset(set(df.columns)):
        faltan = sorted(list(cols_min - set(df.columns)))
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"El archivo '{archivo}' no contiene columnas mínimas",
                "faltan": faltan,
                "esperadas": ["codigo", "storage (si aplica)", "cajas (opcional)", "descripcion (opcional)"],
                "columnas_detectadas": list(df.columns),
            },
        )

    if "storage" not in df.columns:
        df["storage"] = ""  # para clave cuando no se usa storage

    if "cajas" not in df.columns:
        df["cajas"] = 0
    if "descripcion" not in df.columns:
        df["descripcion"] = ""

    df["codigo"] = df["codigo"].astype(str).str.strip()
    df["storage"] = df["storage"].astype(str).str.strip()
    df["cajas"] = df["cajas"].map(_to_float)

    if use_storage:
        df["clave"] = df["codigo"] + "_" + df["storage"]
    else:
        df["clave"] = df["codigo"] + "_"

    df = (
        df[["clave", "codigo", "storage", "cajas", "descripcion"]]
        .groupby(["clave", "codigo", "storage"], as_index=False)
        .agg({"cajas": "sum", "descripcion": "first"})
    )
    return df


# --------------------- Endpoints ---------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/cruce")
async def cruce_archivos(files: List[UploadFile] = File(...)):
    """
    - SAP vs BUNKER: por SKU (ignora storage)
    - SAP vs CBP: por SKU + storage
    Devuelve un Excel (Resumen + Diferencias) para descargar.
    """
    if len(files) < 2:
        raise HTTPException(status_code=400, detail="Sube al menos 2 archivos para cruzar.")

    # Leer todos los archivos primero (por bytes)
    cargados: dict[str, pd.DataFrame] = {}
    for up in files:
        raw = await up.read()
        if not raw:
            raise HTTPException(status_code=400, detail=f"Archivo vacío: {up.filename}")
        df_raw = _read_any_table_bytes(up.filename, raw)
        cargados[up.filename or f"archivo_{len(cargados)+1}"] = df_raw

    # Detectar por nombre
    def find_file(sub: str):
        sub = sub.lower()
        return next((k for k in cargados if sub in (k or "").lower()), None)

    sap_file = find_file("sap")
    bunker_file = find_file("bunker")
    cbp_file = find_file("cbp")

    if not sap_file:
        raise HTTPException(status_code=400, detail="No se detectó archivo SAP (usa 'SAP' en el nombre).")
    if not bunker_file and not cbp_file:
        raise HTTPException(status_code=400, detail="Falta archivo BUNKER y/o CBP (incluye 'BUNKER' o 'CBP' en el nombre).")

    df_sap = cargados.get(sap_file)
    df_bunker = cargados.get(bunker_file)
    df_cbp = cargados.get(cbp_file)

    # SAP usa storage
    df_sap_prep = _prepare_df(df_sap, use_storage=True, archivo=sap_file)
    df_sap_prep = df_sap_prep.rename(columns={"cajas": "cajas_SAP"})

    comparaciones = []
    if df_bunker is not None:
        comparaciones.append(("BUNKER", _prepare_df(df_bunker, use_storage=False, archivo=bunker_file)))
    if df_cbp is not None:
        comparaciones.append(("CBP", _prepare_df(df_cbp, use_storage=True, archivo=cbp_file)))

    merged = df_sap_prep.copy()
    diff_cols = []
    caja_cols = ["cajas_SAP"]

    for name, df_i in comparaciones:
        if df_i is None:
            continue
        col_i = f"cajas_{name}"
        caja_cols.append(col_i)

        merged = pd.merge(
            merged,
            df_i[["clave", "codigo", "storage", "cajas", "descripcion"]].rename(columns={"cajas": col_i}),
            on=["clave", "codigo", "storage"],
            how="outer",
        )
        merged[col_i] = merged[col_i].fillna(0)
        merged["cajas_SAP"] = merged["cajas_SAP"].fillna(0)

        dcol = f"diff_{name}_vs_SAP"
        merged[dcol] = merged[col_i] - merged["cajas_SAP"]
        diff_cols.append(dcol)

    # Descripción de respaldo si faltó en SAP
    if "descripcion" not in merged.columns:
        merged["descripcion"] = None
    for name, df_i in comparaciones:
        if df_i is None:
            continue
        merged["descripcion"] = merged["descripcion"].fillna(
            pd.merge(
                merged[["clave"]],
                df_i[["clave", "descripcion"]],
                on="clave",
                how="left",
            )["descripcion"]
        )

    # Filtrar diferencias
    df_dif = merged.loc[(merged[diff_cols] != 0).any(axis=1)] if diff_cols else merged.copy()

    # Resumen
    total_claves = int(merged["clave"].nunique())
    con_diferencias = int(len(df_dif))
    resumen = {
        "archivos": [x for x in [sap_file, bunker_file, cbp_file] if x],
        "total_claves": total_claves,
        "con_diferencias": con_diferencias,
        "sin_diferencias": int(total_claves - con_diferencias),
    }

    # Orden columnas
    orden = ["codigo", "descripcion", "storage"] + caja_cols + diff_cols
    merged = merged.reindex(columns=orden).sort_values(["codigo", "storage"]).reset_index(drop=True)
    df_dif = df_dif.reindex(columns=orden).sort_values(["codigo", "storage"]).reset_index(drop=True)

    # --------------------- Excel de salida ---------------------
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_name = tmp.name
    tmp.close()

    with pd.ExcelWriter(tmp_name, engine="openpyxl") as writer:
        pd.DataFrame([resumen]).to_excel(writer, sheet_name="Resumen", index=False)
        df_dif.to_excel(writer, sheet_name="Diferencias", index=False)

    # Insertar gráfico en Resumen (si falla, lo ignoramos)
    try:
        from openpyxl import load_workbook
        from openpyxl.chart import BarChart, Reference

        wb = load_workbook(tmp_name)
        ws = wb["Resumen"]

        # Buscar columnas por nombre
        hdrs = {ws.cell(row=1, column=col).value: col for col in range(1, 30)}
        metric_cols = [hdrs.get("total_claves"), hdrs.get("con_diferencias"), hdrs.get("sin_diferencias")]
        metric_cols = [c for c in metric_cols if c]

        if metric_cols:
            chart = BarChart()
            chart.title = "Resumen Cruce Inventario"
            chart.y_axis.title = "Cantidad"
            chart.x_axis.title = "Métrica"

            data = Reference(ws, min_col=min(metric_cols), min_row=1, max_col=max(metric_cols), max_row=2)
            cats = Reference(ws, min_col=min(metric_cols), min_row=1, max_col=max(metric_cols), max_row=1)

            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)
            chart.height = 10
            chart.width = 22
            ws.add_chart(chart, "H2")

        wb.save(tmp_name)
    except Exception:
        pass

    # Borrar el archivo temporal cuando termine la respuesta
    def _cleanup(path: str):
        try:
            os.remove(path)
        except Exception:
            pass

    return FileResponse(
        tmp_name,
        filename="reporte_cruce.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=BackgroundTask(_cleanup, tmp_name),
    )
