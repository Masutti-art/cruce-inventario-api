from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Optional, Tuple
import pandas as pd
import numpy as np
import io, re

app = FastAPI(title="Cruce Inventario API", version="1.0")

# ----------------------------
# Utilidades de parsing/normalización
# ----------------------------

DECIMAL_SEP_RX = re.compile(r"[.,]")

def _to_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def normalize_desc(s: str) -> str:
    s = _to_str(s).upper()
    # quitar tildes simples y simbolos
    s = re.sub(r"[ÁÀÄ]", "A", s)
    s = re.sub(r"[ÉÈË]", "E", s)
    s = re.sub(r"[ÍÌÏ]", "I", s)
    s = re.sub(r"[ÓÒÖ]", "O", s)
    s = re.sub(r"[ÚÙÜ]", "U", s)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def parse_qty(x) -> int:
    """
    Convierte textos tipo '1.545,000' / '20,000' / 20422 a int.
    Asume que las cantidades son en unidades (cajas), sin decimales reales.
    """
    if pd.isna(x) or _to_str(x) == "":
        return 0
    s = _to_str(x)
    # quitar separadores de miles y comas/ puntos
    s = s.replace(".", "").replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return 0

def ensure_xls_engine(file: UploadFile) -> dict:
    """Devuelve kwargs de read_excel según extensión."""
    name = (file.filename or "").lower()
    if name.endswith(".xls"):
        # Para .xls hay que usar xlrd<2.0. A nivel código, pedimos engine="xlrd"
        return {"engine": "xlrd"}
    return {}

# ----------------------------
# Lectura de archivos
# ----------------------------

def read_sap(file: UploadFile) -> pd.DataFrame:
    # columnas esperadas: Plant, Material, Material Description, Storage Location, BUM Quantity
    try:
        kwargs = ensure_xls_engine(file)
        df = pd.read_excel(file.file, dtype=str, **kwargs)
    finally:
        file.file.seek(0)

    # detectar columnas por nombre aproximado
    cols = {c.lower(): c for c in df.columns}
    def pick(*cands):
        for c in cands:
            if c in cols:
                return cols[c]
        return None

    col_mat  = pick("material")
    col_desc = pick("material description", "description", "material_description")
    col_stor = pick("storage location", "storage_location", "storage")
    col_qty  = pick("bum quantity", "quantity", "qty")

    required = [col_mat, col_desc, col_stor, col_qty]
    if any(c is None for c in required):
        raise HTTPException(400, detail="Archivo SAP: columnas requeridas no detectadas (Material, Material Description, Storage Location, BUM Quantity).")

    out = pd.DataFrame({
        "codigo": df[col_mat].map(_to_str),
        "descripcion": df[col_desc].map(_to_str),
        "storage": df[col_stor].map(lambda x: _to_str(x).upper()),
        "qty": df[col_qty].map(parse_qty),
    })
    out["desc_norm"] = out["descripcion"].map(normalize_desc)
    return out

def read_saad(file: UploadFile) -> pd.DataFrame:
    # columnas esperadas: ubprod, itdesc, ubiest, ubcstk
    try:
        kwargs = ensure_xls_engine(file)
        df = pd.read_excel(file.file, dtype=str, **kwargs)
    finally:
        file.file.seek(0)

    cols = {c.lower(): c for c in df.columns}

    # nom. frecuentes en CBP y BUNKER
    col_code = cols.get("ubprod") or cols.get("codigo") or cols.get("code")
    col_desc = cols.get("itdesc") or cols.get("descripcion") or cols.get("desc")
    col_stor = cols.get("ubiest") or cols.get("storage") or cols.get("almacen") or cols.get("storage location")
    col_qty  = cols.get("ubcstk") or cols.get("qty") or cols.get("cantidad") or cols.get("stock")

    if any(x is None for x in [col_code, col_desc, col_stor, col_qty]):
        raise HTTPException(400, detail="Archivo SAAD (CBP/BUNKER): no pude detectar columnas (ubprod/itdesc/ubiest/ubcstk).")

    out = pd.DataFrame({
        "codigo": df[col_code].map(_to_str),
        "descripcion": df[col_desc].map(_to_str),
        "storage": df[col_stor].map(lambda x: _to_str(x).upper()),
        "qty": df[col_qty].map(parse_qty),
    })

    # limpiar prefijos tipo '0000000' de SAAD sin romper alfanuméricos
    out["codigo"] = out["codigo"].str.replace(r"^0+(?=[A-Z0-9])", "", regex=True)
    out["desc_norm"] = out["descripcion"].map(normalize_desc)
    return out

# ----------------------------
# Lógica de filtros y cruce
# ----------------------------

CBP_STORAGES = {"COL", "OLR", "DR", "BN", "RT", "RP", "MOV"}
BUNKER_SAP_STORAGES = {"AER"}

def filtrar_sap_por_sitio(df_sap: pd.DataFrame, sitio: str) -> pd.DataFrame:
    if sitio == "CBP":
        return df_sap[df_sap["storage"].isin(CBP_STORAGES)].copy()
    elif sitio == "BUNKER":
        return df_sap[df_sap["storage"].isin(BUNKER_SAP_STORAGES)].copy()
    else:
        return df_sap.copy()

def agrupar(df: pd.DataFrame) -> pd.DataFrame:
    # Agrupa por código y descripción normalizada
    g = df.groupby(["codigo", "desc_norm"], dropna=False, as_index=False)["qty"].sum()
    return g

def cruce(df_saad: pd.DataFrame, df_sap: pd.DataFrame, etiqueta_saad: str, match_mode: str="auto") -> pd.DataFrame:
    """
    match_mode: 'code' (solo código), 'desc' (solo por desc_norm), 'auto' (código y luego desc)
    """
    a = agrupar(df_saad).rename(columns={"qty": "saad_qty"})
    s = agrupar(df_sap ).rename(columns={"qty": "sap_qty"})

    # 1) Merge por código
    df = a.merge(s, on="codigo", how="outer", suffixes=("",""))
    df["match"] = np.where(df["sap_qty"].notna() & df["saad_qty"].notna(), "code", "")

    if match_mode in ("auto", "desc"):
        # 2) donde no matcheó por código, intentar por descripción
        mask = df["match"].eq("")  # no match aún
        a_pending = a[~a["codigo"].isin(df.loc[~mask, "codigo"])]
        s_pending = s[~s["codigo"].isin(df.loc[~mask, "codigo"])]

        if not a_pending.empty and not s_pending.empty:
            ddesc = a_pending.merge(s_pending, on="desc_norm", how="outer", suffixes=("",""), indicator=True)
            ddesc = ddesc[ddesc["_merge"]!="right_only"].copy()
            # completar en df original los que faltaban
            if not ddesc.empty:
                # combinamos con lo ya existente
                resto = ddesc[["codigo_x","desc_norm","saad_qty","sap_qty"]].rename(columns={"codigo_x":"codigo"})
                resto["match"] = "desc"
                # unir y luego eliminar duplicados conservando los ya 'code'
                df = pd.concat([df, resto], ignore_index=True)
                df = df.sort_values(by=["codigo","match"], ascending=[True, True]).drop_duplicates(subset=["codigo","desc_norm"], keep="first")

    # completar descripciones legibles:
    # intentamos traer una buena 'descripcion' desde cualquiera de las fuentes
    # como no siempre coincide, regeneramos vía un diccionario auxiliar
    df_desc_saad = df_saad[["codigo","desc_norm","descripcion"]].drop_duplicates()
    df_desc_sap  = df_sap [["codigo","desc_norm","descripcion"]].drop_duplicates()

    df = df.merge(df_desc_saad, on=["codigo","desc_norm"], how="left")
    df = df.rename(columns={"descripcion": "desc_saad"})
    df = df.merge(df_desc_sap, on=["codigo","desc_norm"], how="left")
    df = df.rename(columns={"descripcion": "desc_sap"})

    # preferir desc de SAAD y si no existe usar SAP
    df["descripcion"] = df["desc_saad"].fillna(df["desc_sap"])
    df = df.drop(columns=["desc_saad","desc_sap"])

    df["saad_qty"] = df["saad_qty"].fillna(0).astype(int)
    df["sap_qty"]  = df["sap_qty"].fillna(0).astype(int)
    df["diferencia"] = df["saad_qty"] - df["sap_qty"]

    # presentación
    df_out = df[["codigo","descripcion","saad_qty","sap_qty","diferencia","match"]].copy()
    df_out = df_out.rename(columns={
        "saad_qty": etiqueta_saad,
        "sap_qty": "SAP COLGATE",
        "diferencia": "DIFERENCIA"
    })
    # ordenar por |dif|
    df_out = df_out.sort_values(by="DIFERENCIA", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
    return df_out

def escribir_excel(cbvssap: pd.DataFrame, bkvssap: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        # Resumen
        resumen = pd.DataFrame({
            "reporte": ["CBP_vs_SAP","BUNKER_vs_SAP"],
            "filas":   [len(cbvssap), len(bkvssap)],
            "sum_dif_abs": [int(cbvssap["DIFERENCIA"].abs().sum()),
                            int(bkvssap["DIFERENCIA"].abs().sum())],
        })
        resumen.to_excel(writer, index=False, sheet_name="Resumen")

        cbvssap.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
        bkvssap.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
    bio.seek(0)
    return bio.getvalue()

# ----------------------------
# Endpoints
# ----------------------------

@app.get("/healthz")
def healthz():
    return {"ok": True}

def detectar_archivos(uploads: List[UploadFile]) -> Tuple[UploadFile, UploadFile, UploadFile]:
    sap = cbp = bunker = None
    for f in uploads:
        name = (f.filename or "").upper()
        # por nombre si se puede
        if "SAP" in name and sap is None:
            sap = f
        elif "CBP" in name and cbp is None:
            cbp = f
        elif "BUNKER" in name and bunker is None:
            bunker = f

    # si por nombre no alcanza, intentar por columnas
    for f in uploads:
        if f in (sap, cbp, bunker):
            continue
        try:
            kwargs = ensure_xls_engine(f)
            df_head = pd.read_excel(f.file, nrows=1, dtype=str, **kwargs)
            f.file.seek(0)
            cols = set(c.lower() for c in df_head.columns)
            if {"plant", "material", "storage location", "bum quantity"} <= cols and sap is None:
                sap = f
            elif {"ubprod","itdesc","ubiest","ubcstk"} <= cols:
                # si falta alguno asigno al que esté libre
                if cbp is None:
                    cbp = f
                elif bunker is None:
                    bunker = f
        except Exception:
            f.file.seek(0)
            continue

    if not (sap and cbp and bunker):
        raise HTTPException(400, detail="Debes enviar 3 archivos: SAP, SAAD CBP y SAAD BUNKER.")
    return sap, cbp, bunker

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(files: List[UploadFile] = File(...)):
    if len(files) < 3:
        raise HTTPException(400, detail="Envía los 3 archivos (SAP, CBP, BUNKER).")
    sap_f, cbp_f, bunker_f = detectar_archivos(files)

    df_sap = read_sap(sap_f)
    df_cbp = read_saad(cbp_f)
    df_bkr = read_saad(bunker_f)

    sap_cbp = filtrar_sap_por_sitio(df_sap, "CBP")
    sap_bkr = filtrar_sap_por_sitio(df_sap, "BUNKER")

    cbp_vs_sap = cruce(df_cbp, sap_cbp, etiqueta_saad="SAAD CBP", match_mode="auto")
    bkr_vs_sap = cruce(df_bkr, sap_bkr, etiqueta_saad="SAAD BUNKER", match_mode="auto")

    xlsx_bytes = escribir_excel(cbp_vs_sap, bkr_vs_sap)
    return StreamingResponse(io.BytesIO(xlsx_bytes),
                             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'})

@app.post("/cruce-xlsx")
async def cruce_xlsx(
    files: List[UploadFile] = File(...),
    match_mode: str = Query("auto", regex="^(auto|code|desc)$")
):
    # mismo que el auto
    return await cruce_auto_xlsx(files)
