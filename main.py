# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional, Iterable
from pathlib import Path
from io import BytesIO
import pandas as pd
import numpy as np
import re
import logging

app = FastAPI(title="Cruce Inventario API")

logger = logging.getLogger("uvicorn.error")


# -------------------------
# Utilidades
# -------------------------

def norm_code(x: object) -> str:
    """
    Normaliza códigos de producto:
    - Convierte a string
    - Quita espacios
    - Quita ceros iniciales (para casos tipo 0000000MX04554A -> MX04554A)
    - Mayúsculas
    """
    s = str(x).strip()
    s = re.sub(r"\s+", "", s)
    # algunos excels vienen como "nan" y similares
    if s.lower() in {"nan", "none", ""}:
        return ""
    return s.lstrip("0").upper()


def to_number(series: pd.Series) -> pd.Series:
    """
    Convierte cantidades que pueden venir con formato local ES (puntos miles, coma decimal)
    o ya numéricas. Devuelve int sin signo negativo de -0.0.
    """
    if series.dtype.kind in "biufc":
        out = pd.to_numeric(series, errors="coerce")
    else:
        # Quita puntos de miles y cambia coma por punto
        out = (
            series.astype(str)
            .str.replace(r"\.", "", regex=True)
            .str.replace(",", ".", regex=False)
        )
        out = pd.to_numeric(out, errors="coerce")
    out = out.fillna(0)
    # algunas columnas deben ser enteras (cajas)
    out = np.rint(out).astype(np.int64)
    return out


def read_table(tmp_path: str, original_name: str) -> pd.DataFrame:
    """
    Lee un archivo excel/csv con el engine correcto según extensión.
    Si hay error, devuelve HTTP 400 con el detalle (para evitar 500 genérico).
    """
    ext = Path(original_name).suffix.lower()
    logger.info("Leyendo archivo: %s (%s)", original_name, ext)

    try:
        if ext == ".xlsx":
            return pd.read_excel(tmp_path, engine="openpyxl")
        elif ext == ".xls":
            # REQUIERE xlrd==1.2.0 en requirements
            return pd.read_excel(tmp_path, engine="xlrd")
        elif ext == ".csv":
            return pd.read_csv(tmp_path, encoding="latin-1")
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Formato no soportado: {ext} (archivo: {original_name})",
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error leyendo {original_name}: {e}",
        )


def identify_role(name: str, df: pd.DataFrame) -> str:
    """
    Identifica a qué dataset pertenece: 'SAP', 'CBP' o 'BUNKER'.
    Primero por nombre de archivo; si no, por columnas.
    """
    n = name.lower()
    if "sap" in n:
        return "SAP"
    if "cbp" in n:
        return "CBP"
    if "bunker" in n or "bkr" in n:
        return "BUNKER"

    # por contenido:
    cols = {c.lower().strip() for c in df.columns}
    if {"material", "material description"}.issubset(cols):
        return "SAP"
    if {"ubprod", "itdesc", "ubiest", "ubcstk"}.issubset(cols):
        # no sabemos si es CBP o BUNKER; nos quedamos con CBP por defecto
        return "CBP"

    raise HTTPException(
        status_code=400,
        detail=f"No pude detectar si '{name}' es SAP/CBP/BUNKER. Renómbralo (contenga SAP/CBP/BUNKER) o revisa encabezados."
    )


def prepare_sap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara el archivo SAP:
    - Columnas esperadas: Material, Material Description, Storage Location, BUM Quantity
    - Normaliza código
    - Convierte cantidad a número
    - Agrupa por (codigo, storage) y suma
    """
    # estandarizar nombres
    cols = {c.lower(): c for c in df.columns}
    try:
        mat = cols["material"]
        desc = cols.get("material description") or cols.get("material_description")
        stor = cols.get("storage location") or cols.get("storage_location")
        qty  = cols.get("bum quantity") or cols.get("bum_quantity")
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail="SAP: encabezados esperados: Material, Material Description, Storage Location, BUM Quantity.",
        )

    out = pd.DataFrame({
        "codigo": df[mat].map(norm_code),
        "descripcion_sap": df[desc].astype(str),
        "storage": df[stor].astype(str).str.strip().str.upper(),
        "cantidad": to_number(df[qty]),
    })

    out = out.groupby(["codigo", "descripcion_sap", "storage"], as_index=False)["cantidad"].sum()
    return out


def prepare_saad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara archivo SAAD (CBP/BUNKER):
    - Columnas: ubprod (código), itdesc (descripción), ubiest (storage), ubcstk (stock)
    - Suma por (codigo, storage)
    """
    cols = {c.lower(): c for c in df.columns}
    need = ["ubprod", "itdesc", "ubiest", "ubcstk"]
    if not set(need).issubset({c.lower() for c in df.columns}):
        raise HTTPException(
            status_code=400,
            detail="SAAD: encabezados esperados: ubprod, itdesc, ubiest, ubcstk.",
        )

    out = pd.DataFrame({
        "codigo": df[cols["ubprod"]].map(norm_code),
        "descripcion_saad": df[cols["itdesc"]].astype(str),
        "storage": df[cols["ubiest"]].astype(str).str.strip().str.upper(),
        "cantidad": to_number(df[cols["ubcstk"]]),
    })

    out = out.groupby(["codigo", "descripcion_saad", "storage"], as_index=False)["cantidad"].sum()
    return out


def compare_by_storage(
    saad: pd.DataFrame,
    sap: pd.DataFrame,
    sap_storages: Iterable[str],
    titulo_saad: str,
    titulo_sap: str,
) -> pd.DataFrame:
    """
    Cruza SAAD vs SAP filtrando los storages de SAP provistos.
    Devuelve DF con columnas: codigo, descripcion, <titulo_saad>, <titulo_sap>, DIFERENCIA
    Ordenado por |DIFERENCIA| desc.
    """

    sap_storages = {s.strip().upper() for s in sap_storages if s}
    sap_f = sap[sap["storage"].isin(sap_storages)].copy()

    # agrupar por código
    sap_g = sap_f.groupby("codigo", as_index=False)["cantidad"].sum().rename(columns={"cantidad": titulo_sap})
    saad_g = saad.groupby("codigo", as_index=False)["cantidad"].sum().rename(columns={"cantidad": titulo_saad})

    # descripcion: preferimos la de SAAD; si no, la de SAP
    # juntamos descripciones únicas por código
    desc_saad = saad.groupby("codigo", as_index=False)["descripcion_saad"].agg(lambda s: s.iloc[0] if len(s) else "")
    desc_sap  = sap.groupby("codigo", as_index=False)["descripcion_sap"].agg(lambda s: s.iloc[0] if len(s) else "")

    base = pd.merge(saad_g, sap_g, on="codigo", how="outer")
    base = pd.merge(base, desc_saad, on="codigo", how="left")
    base = pd.merge(base, desc_sap, on="codigo", how="left")

    base[titulo_saad] = base[titulo_saad].fillna(0).astype(int)
    base[titulo_sap]  = base[titulo_sap].fillna(0).astype(int)

    base["descripcion"] = base["descripcion_saad"].where(base["descripcion_saad"].notna() & (base["descripcion_saad"] != ""), base["descripcion_sap"])
    base = base.drop(columns=["descripcion_saad", "descripcion_sap"])

    base["DIFERENCIA"] = base[titulo_saad] - base[titulo_sap]
    base = base[["codigo", "descripcion", titulo_saad, titulo_sap, "DIFERENCIA"]]

    # ordenar por |dif| desc
    base = base.sort_values(by="DIFERENCIA", key=lambda s: s.abs(), ascending=False, ignore_index=True)
    return base


def build_workbook(cbp_vs_sap: pd.DataFrame, bunker_vs_sap: pd.DataFrame) -> BytesIO:
    """
    Construye el .xlsx con 2 hojas.
    """
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        # formatos
        fmt_header = writer.book.add_format({"bold": True, "bg_color": "#F2F2F2", "border": 1})
        fmt_int = writer.book.add_format({"num_format": "0"})

        def write_sheet(df: pd.DataFrame, sheet_name: str):
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]
            # formato encabezados
            for col, _ in enumerate(df.columns):
                ws.write(0, col, df.columns[col], fmt_header)
                ws.set_column(col, col, 22, fmt_int if col >= 2 else None)

        write_sheet(cbp_vs_sap, "CBP_vs_SAP")
        write_sheet(bunker_vs_sap, "BUNKER_vs_SAP")

    buf.seek(0)
    return buf


# -------------------------
# Endpoints
# -------------------------

@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(
    files: List[UploadFile] = File(..., description="Subir 3 archivos: SAP (.xlsx), SAAD CBP (.xls), SAAD BUNKER (.xls)"),
    sap_cbp_storages: str = Query("COL", description="Storages de SAP que se comparan contra CBP (separados por coma)"),
    sap_bunker_storages: str = Query("AER", description="Storages de SAP que se comparan contra BUNKER (separados por coma)"),
):
    if len(files) < 3:
        raise HTTPException(status_code=400, detail="Sube los 3 archivos: SAP, CBP y BUNKER.")

    # Guardar temporales y leer
    datasets = {}
    for f in files:
        tmp = Path("/tmp") / f.filename
        content = await f.read()
        tmp.write_bytes(content)

        df = read_table(str(tmp), f.filename)
        role = identify_role(f.filename, df)
        datasets[role] = df

    if "SAP" not in datasets or "CBP" not in datasets or "BUNKER" not in datasets:
        raise HTTPException(
            status_code=400,
            detail=f"Faltan archivos: encontrados {list(datasets.keys())}. Asegúrate de subir SAP, CBP y BUNKER."
        )

    # Preparar dataframes
    sap_df = prepare_sap(datasets["SAP"])
    cbp_df = prepare_saad(datasets["CBP"])
    bunker_df = prepare_saad(datasets["BUNKER"])

    # Comparaciones
    sap_cbp_set = [s.strip() for s in sap_cbp_storages.split(",") if s.strip()]
    sap_bunker_set = [s.strip() for s in sap_bunker_storages.split(",") if s.strip()]

    cbp_vs_sap = compare_by_storage(
        saad=cbp_df,
        sap=sap_df,
        sap_storages=sap_cbp_set,
        titulo_saad="SAAD CBP",
        titulo_sap="SAP COLGATE",
    )

    bunker_vs_sap = compare_by_storage(
        saad=bunker_df,
        sap=sap_df,
        sap_storages=sap_bunker_set,
        titulo_saad="SAAD BUNKER",
        titulo_sap="SAP COLGATE",
    )

    # Si no hay filas, devolvemos archivo igual con cabeceras
    xlsx_bytes = build_workbook(cbp_vs_sap, bunker_vs_sap)

    return StreamingResponse(
        xlsx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
    )
