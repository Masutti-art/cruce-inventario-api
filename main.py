# main.py
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Dict, Tuple
import io
import pandas as pd
import numpy as np

app = FastAPI(title="Cruce Inventario API", version="1.0")

# ---------- Utilidades ---------- #

def _norm_cols(df: pd.DataFrame) -> Dict[str, str]:
    """
    Devuelve un dict {col_original: col_normalizada} para facilitar el match por nombre.
    """
    norm = {}
    for c in df.columns:
        k = (
            str(c)
            .strip()
            .lower()
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
            .replace("  ", " ")
        )
        norm[c] = k
    return norm

def _detect_sap(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    norm = _norm_cols(df)
    vals = set(norm.values())
    required = {"plant", "material", "material description", "storage location", "bum quantity"}
    # permitir pequeñas variaciones:
    alt_qty = {"bum qty", "quantity", "qty", "total quantity"}
    has_min = {"material", "material description", "storage location"} <= vals
    has_qty = bool(alt_qty & vals) or ("bum quantity" in vals)
    return has_min and has_qty

def _detect_saad(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    norm = _norm_cols(df)
    vals = set(norm.values())
    need = {"ubprod", "itdesc", "ubiest", "ubcstk"}
    return need <= vals

def _read_any(file: UploadFile) -> pd.DataFrame:
    # soporta .xls y .xlsx
    content = file.file.read()
    file.file.seek(0)
    try:
        if file.filename.lower().endswith(".xls"):
            # engine xlrd para .xls
            df = pd.read_excel(io.BytesIO(content), engine="xlrd")
        else:
            df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"No pude leer {file.filename}: {e}")
    return df

def _pick_column_by_norm(df: pd.DataFrame, targets: List[str]) -> str:
    """
    Devuelve el nombre real de columna del DF cuya versión normalizada
    matchee con alguno de los 'targets' normalizados.
    """
    norm = _norm_cols(df)
    inv = {v:k for k,v in norm.items()}
    for t in targets:
        tnorm = (
            t.strip().lower()
            .replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        )
        if tnorm in inv:
            return inv[tnorm]
    raise KeyError(f"No encontré ninguna de las columnas: {targets}")

def _clean_code(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    # quitar ceros a la izquierda sólo cuando el formato es claramente numérico
    # (no tocar códigos alfanuméricos como MX04554A)
    if s.isdigit():
        return str(int(s))
    return s

def _num(x) -> float:
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x)
    # SAP suele traer "1.545,000" -> usar coma como decimal
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        try:
            return float(str(x).replace(",", "."))
        except:
            return 0.0

def _site_from_saad(df: pd.DataFrame) -> str:
    """
    Intenta detectar si un SAAD es BUNKER o CBP en base a 'almacen'/'almacen'.
    Si contiene 'BKR' -> BUNKER, si no -> CBP.
    """
    norm = _norm_cols(df)
    inv = {v:k for k,v in norm.items()}
    alm_col = None
    for cand in ["almacen", "almacén"]:
        if cand in inv: alm_col = inv[cand]; break
    if alm_col is None:
        # si no existe la columna, asumimos CBP por defecto
        return "CBP"

    vals = df[alm_col].astype(str).str.upper()
    if vals.str.contains("BKR").any():
        return "BUNKER"
    return "CBP"

def _aggregate_sap(df: pd.DataFrame,
                   storages: List[str]) -> pd.DataFrame:
    norm = _norm_cols(df)

    col_code = _pick_column_by_norm(df, ["material"])
    col_desc = _pick_column_by_norm(df, ["material description", "description"])
    col_stor = _pick_column_by_norm(df, ["storage location"])
    # cantidad: admitir varias
    try:
        col_qty = _pick_column_by_norm(df, ["bum quantity", "bum qty", "quantity", "qty"])
    except:
        # fallback: si hay varias columnas de cantidad, tomar la primera numérica
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if not numeric_cols:
            raise HTTPException(status_code=400, detail="SAP: no encontré columna de cantidad (BUM Quantity / Quantity).")
        col_qty = numeric_cols[0]

    tmp = df[[col_code, col_desc, col_stor, col_qty]].copy()
    tmp.columns = ["codigo", "descripcion", "storage", "qty"]
    tmp["codigo"] = tmp["codigo"].map(_clean_code)
    tmp["descripcion"] = tmp["descripcion"].astype(str).str.strip()
    tmp["storage"] = tmp["storage"].astype(str).str.upper().str.strip()
    tmp["qty"] = tmp["qty"].map(_num)

    # filtrar storages
    storages_up = set([s.upper().strip() for s in storages])
    tmp = tmp[tmp["storage"].isin(storages_up)]

    # agrupar por (codigo, storage) – sumando todas las líneas (status UR/QI/BL etc.)
    sap = tmp.groupby(["codigo", "storage"], as_index=False)\
             .agg({"descripcion":"first", "qty":"sum"})
    sap.rename(columns={"qty":"sap_qty"}, inplace=True)
    return sap

def _aggregate_saad(df: pd.DataFrame,
                    storages: List[str]) -> pd.DataFrame:
    col_code = _pick_column_by_norm(df, ["ubprod"])
    col_desc = _pick_column_by_norm(df, ["itdesc"])
    col_stor = _pick_column_by_norm(df, ["ubiest"])
    col_qty  = _pick_column_by_norm(df, ["ubcstk"])

    tmp = df[[col_code, col_desc, col_stor, col_qty]].copy()
    tmp.columns = ["codigo", "descripcion", "storage", "qty"]
    tmp["codigo"] = tmp["codigo"].map(_clean_code)
    tmp["descripcion"] = tmp["descripcion"].astype(str).str.strip()
    tmp["storage"] = tmp["storage"].astype(str).str.upper().str.strip()
    tmp["qty"] = tmp["qty"].map(_num)

    storages_up = set([s.upper().strip() for s in storages])
    tmp = tmp[tmp["storage"].isin(storages_up)]

    saad = tmp.groupby(["codigo", "storage"], as_index=False)\
              .agg({"descripcion":"first", "qty":"sum"})
    saad.rename(columns={"qty":"saad_qty"}, inplace=True)
    return saad

def _merge_sheet(saad: pd.DataFrame, sap: pd.DataFrame,
                 saad_label: str,
                 solo_saad: bool) -> pd.DataFrame:
    """
    Cruce por (codigo, storage).
    DIFERENCIA = SAP - SAAD
    """
    merged = pd.merge(
        saad, sap,
        on=["codigo", "storage"],
        how="outer",
        suffixes=("", "_sap")
    )

    # completar descripción
    merged["descripcion"] = merged["descripcion"].fillna(merged.pop("descripcion_sap"))
    merged["saad_qty"] = merged["saad_qty"].fillna(0.0)
    merged["sap_qty"] = merged["sap_qty"].fillna(0.0)

    if solo_saad:
        merged = merged[merged["saad_qty"] != 0]

    merged["DIFERENCIA"] = merged["sap_qty"] - merged["saad_qty"]

    # ordenar por diferencia absoluta (mayor a menor)
    merged["absdiff"] = merged["DIFERENCIA"].abs()
    out = merged.sort_values("absdiff", ascending=False).drop(columns=["absdiff"])

    # reordenar columnas
    out = out[["codigo", "storage", "descripcion", "saad_qty", "sap_qty", "DIFERENCIA"]]
    out.rename(columns={
        "saad_qty": f"SAAD {saad_label}",
        "sap_qty": "SAP COLGATE"
    }, inplace=True)
    return out


def _summary_sheet(sheet_name: str, df: pd.DataFrame) -> Dict[str, float]:
    return {
        f"{sheet_name} · filas": int(len(df)),
        f"{sheet_name} · total SAAD": float(df.filter(regex=r"^SAAD").sum(numeric_only=True)),
        f"{sheet_name} · total SAP":  float(df["SAP COLGATE"].sum()),
        f"{sheet_name} · total DIFERENCIA": float(df["DIFERENCIA"].sum())
    }

# ---------- Endpoint principal ---------- #

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(
    files: List[UploadFile] = File(..., description="Subí: SAP + SAAD CBP + SAAD BUNKER (en cualquier orden)"),
    storages: str = Query("AER,MOV,RT,RP,BN,OLR", description="Storages a considerar, coma-separados"),
    solo_saad: int = Query(1, ge=0, le=1, description="1=mostrar solo filas con stock en SAAD; 0=mostrar todas")
):
    """
    - Detecta automáticamente SAP / SAAD CBP / SAAD BUNKER por encabezados.
    - Compara por (codigo, storage) y devuelve Excel con:
        · CBP_vs_SAP
        · BUNKER_vs_SAP
        · Resumen
    """
    if not files or len(files) < 3:
        raise HTTPException(status_code=400, detail="Subí mínimo 3 archivos: SAP, SAAD CBP y SAAD BUNKER.")

    stor_list = [s.strip().upper() for s in storages.split(",") if s.strip()]

    dfs = []
    for f in files:
        df = _read_any(f)
        dfs.append((f.filename, df))

    sap_df = None
    cbp_saad_df = None
    bunker_saad_df = None

    # 1) clasificar cada DF
    for fname, df in dfs:
        if _detect_sap(df):
            sap_df = df
            continue
        if _detect_saad(df):
            site = _site_from_saad(df)
            if site == "BUNKER":
                bunker_saad_df = df if bunker_saad_df is None else pd.concat([bunker_saad_df, df], ignore_index=True)
            else:
                cbp_saad_df = df if cbp_saad_df is None else pd.concat([cbp_saad_df, df], ignore_index=True)

    if sap_df is None or cbp_saad_df is None or bunker_saad_df is None:
        raise HTTPException(
            status_code=400,
            detail="No pude detectar claramente SAP/CBP/BUNKER. Verificá encabezados.\n"
                   "SAP requiere: Material, Material Description, Storage Location, BUM Quantity.\n"
                   "SAAD requiere: ubprod, itdesc, ubiest, ubcstk (almacen con 'BKR' => BUNKER)."
        )

    # 2) agregaciones
    sap_aggr      = _aggregate_sap(sap_df, stor_list)
    cbp_aggr      = _aggregate_saad(cbp_saad_df, stor_list)
    bunker_aggr   = _aggregate_saad(bunker_saad_df, stor_list)

    # 3) merges
    sheet_cbp    = _merge_sheet(cbp_aggr, sap_aggr, "CBP", bool(solo_saad))
    sheet_bunker = _merge_sheet(bunker_aggr, sap_aggr, "BUNKER", bool(solo_saad))

    # 4) resumen
    resumen = {}
    resumen.update(_summary_sheet("CBP_vs_SAP", sheet_cbp))
    resumen.update(_summary_sheet("BUNKER_vs_SAP", sheet_bunker))
    resumen_df = pd.DataFrame({
        "métrica": list(resumen.keys()),
        "valor": list(resumen.values())
    })

    # 5) exportar a xlsx
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        sheet_cbp.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
        sheet_bunker.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
        resumen_df.to_excel(writer, index=False, sheet_name="Resumen")
        # ancho de columnas:
        for s in ["CBP_vs_SAP", "BUNKER_vs_SAP"]:
            ws = writer.sheets[s]
            ws.set_column(0, 0, 15)  # codigo
            ws.set_column(1, 1, 10)  # storage
            ws.set_column(2, 2, 55)  # descripcion
            ws.set_column(3, 5, 14)  # cantidades
        writer.sheets["Resumen"].set_column(0, 1, 40)

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
    )
