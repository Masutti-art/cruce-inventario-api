# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, PlainTextResponse
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from io import BytesIO
import re

app = FastAPI(title="Cruce Inventario API", version="1.0")

# --- Conjuntos de storages para cada cruce ---
CBP_SAP_STORAGES = {"AER", "MOV", "RT", "RP", "BN", "OLR"}
BUNKER_SAP_STORAGES = {"AER"}

# --- Utilidades de normalización ---

def norm_colname(s: str) -> str:
    """
    Normaliza nombres de columnas: minúscula, sin espacios/guiones/barras bajas.
    """
    s = s.strip().lower()
    s = re.sub(r"[\s\-_]+", "", s)
    return s

def to_str_code(x) -> str:
    """
    Devuelve el código como texto (conserva alfanuméricos y ceros a la izquierda).
    """
    if pd.isna(x):
        return ""
    s = str(x).strip()
    # Evitar que 61004113.0 aparezca con .0
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    return s

def to_number_es(x) -> float:
    """
    Convierte cantidades con formato español:
    - '1.545,000' -> 1545
    - '12.345'    -> 12345
    - '12,34'     -> 12.34
    - vacíos/NaN  -> 0
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    if isinstance(x, (int, float, np.number)):
        return float(x)

    s = str(x).strip()
    if s == "":
        return 0.0

    # quitar espacios
    s = s.replace(" ", "")

    # caso típico español: miles con '.' y decimal con ','
    if s.count(",") == 1 and s.count(".") >= 1:
        s = s.replace(".", "").replace(",", ".")
    else:
        # si hay muchas comas -> miles
        if s.count(",") > 1:
            s = s.replace(",", "")
        else:
            # si hay una coma y ninguna o una sola cifra decimal
            # la interpretamos como decimal
            if s.count(",") == 1 and s.count(".") == 0:
                s = s.replace(",", ".")
            else:
                # en cualquier otro caso quitamos comas
                s = s.replace(",", "")

    try:
        return float(s)
    except Exception:
        return 0.0

def pick_first(series: pd.Series) -> str:
    """Primer no-nulo como string."""
    for v in series:
        if pd.notna(v) and str(v).strip() != "":
            return str(v).strip()
    return ""

# --- Mapeos flexibles de cabeceras ---

SAP_MAP = {
    "codigo": {"material", "materialid", "sku", "codigo", "code"},
    "descripcion": {"materialdescription", "description", "itdesc", "desc", "descripcion"},
    "storage": {"storagelocation", "storage", "almacen", "ubiest", "sublocation"},
    "cantidad": {"bumquantity", "quantity", "qty", "cantidad", "stock", "qtytotal"},
}

SAAD_MAP = {
    "codigo": {"ubprod", "material", "codigo", "sku", "code"},
    "descripcion": {"itdesc", "descripcion", "desc"},
    "storage": {"ubiest", "storage", "almacen"},
    "cantidad": {"ubcstk", "cantidad", "qty", "stock"},
}

def find_colnames(df: pd.DataFrame, mapping: Dict[str, set]) -> Dict[str, str]:
    """
    A partir de un DataFrame y un mapeo de sinonimias, devuelve
    {canonico: nombre_real_en_df}
    """
    normalized = {norm_colname(c): c for c in df.columns}
    result = {}
    for want, aliases in mapping.items():
        found = None
        for a in aliases:
            na = norm_colname(a)
            for nc, real in normalized.items():
                if nc == na:
                    found = real
                    break
            if found:
                break
        if not found:
            # búsqueda contains (más laxa)
            for nc, real in normalized.items():
                for a in aliases:
                    if a in nc:
                        found = real
                        break
                if found:
                    break
        if not found:
            raise HTTPException(
                status_code=400,
                detail=f"No pude detectar columna '{want}' en el archivo. Esperaba algo como {sorted(list(aliases))}."
            )
        result[want] = found
    return result

def read_excel_smart(file: UploadFile) -> pd.DataFrame:
    """
    Lee .xlsx/.xls en un DataFrame.
    - .xlsx -> openpyxl
    - .xls  -> xlrd
    """
    content = file.file.read()
    bio = BytesIO(content)
    name = (file.filename or "").lower()

    if name.endswith(".xlsx"):
        return pd.read_excel(bio, engine="openpyxl")
    elif name.endswith(".xls"):
        # requiere xlrd==1.2.0
        return pd.read_excel(bio, engine="xlrd")
    else:
        # último intento: dejá que pandas detecte
        try:
            return pd.read_excel(bio)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail=f"Formato no soportado para '{file.filename}'. Subí .xlsx o .xls auténticos."
            )

def normalize_sap(df: pd.DataFrame) -> pd.DataFrame:
    cols = find_colnames(df, SAP_MAP)
    out = pd.DataFrame()
    out["codigo"] = df[cols["codigo"]].map(to_str_code)
    out["descripcion"] = df[cols["descripcion"]].astype(str).fillna("")
    out["storage"] = df[cols["storage"]].astype(str).str.strip().str.upper()
    out["cantidad"] = df[cols["cantidad"]].map(to_number_es).fillna(0.0)
    return out

def normalize_saad(df: pd.DataFrame) -> pd.DataFrame:
    cols = find_colnames(df, SAAD_MAP)
    out = pd.DataFrame()
    out["codigo"] = df[cols["codigo"]].map(to_str_code)
    out["descripcion"] = df[cols["descripcion"]].astype(str).fillna("")
    out["storage"] = df[cols["storage"]].astype(str).str.strip().str.upper()
    out["cantidad"] = df[cols["cantidad"]].map(to_number_es).fillna(0.0)
    return out

def aggregate_by_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve DF con columnas: codigo, descripcion (first), cantidad (sum)
    """
    if df.empty:
        return pd.DataFrame(columns=["codigo", "descripcion", "cantidad"])
    g = df.groupby("codigo", as_index=False).agg(
        cantidad=("cantidad", "sum"),
        descripcion=("descripcion", pick_first),
    )
    # ordenar por código para estabilidad
    return g[["codigo", "descripcion", "cantidad"]]

def compare_sheets(
    saad_df: pd.DataFrame,
    sap_df: pd.DataFrame,
    saad_title: str,
    sap_storages: set
) -> pd.DataFrame:
    """
    Filtra SAP por storages, agrega y compara por código.
    """
    # Filtrar SAP por storages requeridos
    sap_filtered = sap_df[sap_df["storage"].isin(sap_storages)].copy()
    sap_agg = aggregate_by_code(sap_filtered)
    sap_agg.rename(columns={"cantidad": "SAP COLGATE"}, inplace=True)

    saad_agg = aggregate_by_code(saad_df)
    saad_agg.rename(columns={"cantidad": saad_title}, inplace=True)

    merged = pd.merge(
        saad_agg,
        sap_agg,
        on=["codigo"],
        how="outer",
        suffixes=("", "_sap"),
        sort=False,
    )

    # completar descripción
    merged["descripcion"] = merged["descripcion"].fillna("")
    merged["descripcion"] = merged["descripcion"].where(
        merged["descripcion"] != "",
        pd.Series(merged.get("descripcion_sap", ""), dtype=str)
    )
    if "descripcion_sap" in merged.columns:
        merged.drop(columns=["descripcion_sap"], inplace=True, errors="ignore")

    # completar cantidades faltantes
    if saad_title not in merged.columns:
        merged[saad_title] = 0.0
    if "SAP COLGATE" not in merged.columns:
        merged["SAP COLGATE"] = 0.0

    merged[saad_title] = merged[saad_title].fillna(0.0)
    merged["SAP COLGATE"] = merged["SAP COLGATE"].fillna(0.0)

    merged["DIFERENCIA"] = merged[saad_title] - merged["SAP COLGATE"]
    merged["absdiff"] = merged["DIFERENCIA"].abs()

    merged.sort_values(by=["absdiff", "codigo"], ascending=[False, True], inplace=True)
    merged.drop(columns=["absdiff"], inplace=True)
    return merged[["codigo", "descripcion", saad_title, "SAP COLGATE", "DIFERENCIA"]]

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(
    files: List[UploadFile] = File(..., description="Subí: 1) SAP.xlsx  2) SAAD CBP  3) SAAD BUNKER"),
):
    """
    Recibe 3 archivos en este orden:
      1) SAP (xlsx)
      2) SAAD CBP (xls/xlsx)
      3) SAAD BUNKER (xls/xlsx)

    Devuelve un Excel con dos hojas:
      - CBP_vs_SAP
      - BUNKER_vs_SAP
    """
    if not files or len(files) != 3:
        raise HTTPException(
            status_code=400,
            detail="Debes subir exactamente 3 archivos: SAP.xlsx, SAAD_CBP, SAAD_BUNKER (en ese orden)."
        )

    sap_file, cbp_file, bunker_file = files

    try:
        sap_raw = read_excel_smart(sap_file)
        cbp_raw = read_excel_smart(cbp_file)
        bunker_raw = read_excel_smart(bunker_file)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error leyendo archivos: {e}")

    # Normalizar
    try:
        sap_df = normalize_sap(sap_raw)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SAP: {e}")

    try:
        cbp_df = normalize_saad(cbp_raw)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SAAD CBP: {e}")

    try:
        bunker_df = normalize_saad(bunker_raw)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SAAD BUNKER: {e}")

    # Comparaciones
    hoja_cbp = compare_sheets(
        saad_df=cbp_df, sap_df=sap_df, saad_title="SAAD CBP", sap_storages=CBP_SAP_STORAGES
    )
    hoja_bunker = compare_sheets(
        saad_df=bunker_df, sap_df=sap_df, saad_title="SAAD BUNKER", sap_storages=BUNKER_SAP_STORAGES
    )

    # Exportar a Excel en memoria
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        hoja_cbp.to_excel(writer, sheet_name="CBP_vs_SAP", index=False)
        hoja_bunker.to_excel(writer, sheet_name="BUNKER_vs_SAP", index=False)

        # Ajuste de ancho de columnas
        for sheet in ["CBP_vs_SAP", "BUNKER_vs_SAP"]:
            ws = writer.sheets[sheet]
            ws.set_column("A:A", 16)  # codigo
            ws.set_column("B:B", 48)  # descripcion
            ws.set_column("C:E", 16)  # cantidades

    output.seek(0)
    headers = {
        "Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'
    }
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
