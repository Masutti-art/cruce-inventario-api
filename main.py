from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import re

app = FastAPI(title="Cruce Inventario API", version="2.0")

# ------------------ Utilidades ------------------

def _pick_engine(filename: str) -> str:
    fn = filename.lower()
    if fn.endswith(".xlsx"):
        return "openpyxl"
    if fn.endswith(".xls"):
        return "xlrd"  # xlrd >= 2.0.1
    raise ValueError("Formato no soportado. Subí .xls o .xlsx")

def _to_int(x):
    if pd.isna(x):
        return 0
    if isinstance(x, (int, float)):
        return int(x)
    s = str(x).strip()
    s = s.replace(".", "").replace(",", "")
    try:
        return int(float(s))
    except:
        return 0

def _norm_code(s):
    if pd.isna(s):
        return ""
    return str(s).strip()

def _norm_text(s):
    if pd.isna(s):
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()

# Orden lógico de estados (UR > QI > BL > otros)
ESTADO_ORDER = {"UR": 0, "QI": 1, "BL": 2}
def _estado_key(val: str) -> int:
    val = (val or "").upper().strip()
    return ESTADO_ORDER.get(val, 99)

# ------------------ Lectura genérica y detección ------------------

async def _read_excel(upload: UploadFile) -> pd.DataFrame:
    """
    Lee el UploadFile en memoria respetando la extensión para elegir engine.
    Devuelve un DataFrame con todo en dtype=str (para normalizar).
    """
    try:
        engine = _pick_engine(upload.filename)
        content = await upload.read()
        bio = io.BytesIO(content)
        df = pd.read_excel(bio, engine=engine, dtype=str)
        # Restauro el puntero para permitir reusos si hiciera falta
        bio.seek(0)
        return df
    except Exception as e:
        raise HTTPException(400, f"No pude leer {upload.filename}: {e}")

def _has_cols(df: pd.DataFrame, *parts) -> bool:
    cols = [c.lower() for c in df.columns]
    return all(any(p in c for c in cols) for p in parts)

def _col_like(df: pd.DataFrame, token: str):
    for c in df.columns:
        if token in c.lower():
            return c
    return None

def _is_sap(df: pd.DataFrame) -> bool:
    return _has_cols(df, "material", "material description", "storage location", "bum")

def _is_saad(df: pd.DataFrame) -> bool:
    return _has_cols(df, "ubprod", "itdesc", "ubiest", "ubcstk")

def _looks_bunker(df: pd.DataFrame) -> bool:
    # BUNKER: ubiest suele traer 'OLR - UR', 'AER - QI', etc.
    col = _col_like(df, "ubiest")
    if not col:
        return False
    sample = df[col].astype(str).head(200).dropna().astype(str)
    if sample.empty:
        return False
    # Si la mayoría tiene " - " asumimos bunker
    has_sep = (sample.str.contains(r"\s-\s", regex=True, na=False)).mean()
    return has_sep > 0.2

def _parse_sap(df: pd.DataFrame) -> pd.DataFrame:
    col_code    = _col_like(df, "material")
    col_desc    = _col_like(df, "material description")
    col_ubiest  = _col_like(df, "storage location")
    col_qty     = _col_like(df, "bum")

    if not all([col_code, col_desc, col_ubiest, col_qty]):
        raise ValueError("SAP: faltan columnas (Material, Material Description, Storage Location, BUM Quantity).")

    sap = pd.DataFrame({
        "codigo":      df[col_code].map(_norm_code),
        "descripcion": df[col_desc].map(_norm_text),
        "ubiest":      df[col_ubiest].map(lambda x: _norm_text(x).upper()),
        "cajas":       df[col_qty].map(_to_int),
    })
    sap = sap.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
    return sap

def _parse_saad_cbp(df: pd.DataFrame) -> pd.DataFrame:
    col_code   = _col_like(df, "ubprod")
    col_desc   = _col_like(df, "itdesc")
    col_ubiest = _col_like(df, "ubiest")
    col_qty    = _col_like(df, "ubcstk")

    if not all([col_code, col_desc, col_ubiest, col_qty]):
        raise ValueError("SAAD CBP: faltan columnas (ubprod, itdesc, ubiest, ubcstk).")

    sad = pd.DataFrame({
        "codigo":      df[col_code].map(_norm_code),
        "descripcion": df[col_desc].map(_norm_text),
        "ubiest":      df[col_ubiest].map(lambda x: _norm_text(x).split(" - ")[0].upper()),
        "cajas":       df[col_qty].map(_to_int),
    })
    sad = sad.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
    return sad

def _parse_saad_bunker(df: pd.DataFrame, split_by_estado: bool=True) -> pd.DataFrame:
    col_code   = _col_like(df, "ubprod")
    col_desc   = _col_like(df, "itdesc")
    col_ubiest = _col_like(df, "ubiest")
    col_qty    = _col_like(df, "ubcstk")

    if not all([col_code, col_desc, col_ubiest, col_qty]):
        raise ValueError("SAAD BUNKER: faltan columnas (ubprod, itdesc, ubiest, ubcstk).")

    ub_raw = df[col_ubiest].astype(str)

    raw = pd.DataFrame({
        "codigo":      df[col_code].map(_norm_code),
        "descripcion": df[col_desc].map(_norm_text),
        "ubiest_raw":  ub_raw.map(_norm_text),
        "cajas":       df[col_qty].map(_to_int),
    })

    raw["ubiest"] = raw["ubiest_raw"].str.extract(r"^\s*([A-Za-z]+)").fillna("").str.upper()
    raw["estado"] = raw["ubiest_raw"].str.extract(r"-\s*([A-Za-z]+)\s*$").fillna("").str.upper()

    if split_by_estado:
        out = raw.groupby(["codigo", "descripcion", "ubiest", "estado"], as_index=False)["cajas"].sum()
    else:
        out = raw.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        out["estado"] = ""
    return out

def _cruzar(df_left: pd.DataFrame, df_right: pd.DataFrame,
           on_cols: list, left_name: str, right_name: str) -> pd.DataFrame:
    merged = pd.merge(df_left, df_right, on=on_cols, how="outer", suffixes=("_L", "_R"))
    merged[left_name]  = merged.pop("cajas_L").fillna(0).astype(int)
    merged[right_name] = merged.pop("cajas_R").fillna(0).astype(int)
    merged["DIFERENCIA"] = (merged[left_name] - merged[right_name]).astype(int)
    cols = [c for c in on_cols] + [left_name, right_name, "DIFERENCIA"]
    return merged[cols].fillna("")

# ------------------ Endpoint ------------------

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(
    files: list[UploadFile] = File(..., description="Subí los TRES archivos (en cualquier orden): SAP.xls/xlsx, SAAD_CBP.xls/xlsx, SAAD_BUNKER.xls/xlsx."),
    sap_cbp_storages: str = Query("", description="Storages CBP/SAP (coma-separado). Si está vacío se detecta automáticamente."),
    sap_bunker_storages: str = Query("", description="Storages BUNKER/SAP (coma-separado). Si está vacío se detecta automáticamente."),
    split_by_estado: bool = Query(True, description="Si True, BUNKER se separa por UR/QI/BL."),
):
    try:
        if len(files) != 3:
            raise HTTPException(400, "Subí exactamente 3 archivos: SAP, SAAD CBP y SAAD BUNKER (en cualquier orden).")

        # 1) Leo todo y detecto tipo
        raw = []
        for f in files:
            df = await _read_excel(f)
            raw.append((f.filename, df))

        sap_df = cbp_df = bunker_df = None
        for name, df in raw:
            if _is_sap(df):
                sap_df = df
            elif _is_saad(df) and _looks_bunker(df):
                bunker_df = df
            elif _is_saad(df):
                cbp_df = df

        if sap_df is None or cbp_df is None or bunker_df is None:
            raise HTTPException(400, "No pude identificar los tres archivos (SAP, SAAD CBP y SAAD BUNKER). Revisá los encabezados.")

        # 2) Parseo a formato normalizado
        sap    = _parse_sap(sap_df)
        cbp    = _parse_saad_cbp(cbp_df)
        bunker = _parse_saad_bunker(bunker_df, split_by_estado=split_by_estado)

        # 3) Storages automáticos (si no se mandan)
        sap_st = set(sap["ubiest"].unique())
        cbp_st = set(cbp["ubiest"].unique())
        bun_st = set(bunker["ubiest"].unique())

        if sap_cbp_storages.strip():
            stor_cbp = [s.strip().upper() for s in sap_cbp_storages.split(",") if s.strip()]
        else:
            inter = sorted(sap_st & cbp_st)
            stor_cbp = inter if inter else sorted(cbp_st or sap_st)

        if sap_bunker_storages.strip():
            stor_bun = [s.strip().upper() for s in sap_bunker_storages.split(",") if s.strip()]
        else:
            inter = sorted(sap_st & bun_st)
            stor_bun = inter if inter else sorted(bun_st or sap_st)

        # 4) Filtros por storage
        sap_cbp = sap[sap["ubiest"].isin(stor_cbp)] if stor_cbp else sap.copy()
        cbp_f   = cbp[cbp["ubiest"].isin(stor_cbp)] if stor_cbp else cbp.copy()

        sap_bun  = sap[sap["ubiest"].isin(stor_bun)] if stor_bun else sap.copy()
        bunker_f = bunker[bunker["ubiest"].isin(stor_bun)] if stor_bun else bunker.copy()

        # 5) Cruces
        # CBP vs SAP
        cbp_g = cbp_f.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        sap_g = sap_cbp.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        cbp_vs_sap = _cruzar(
            df_left=cbp_g,
            df_right=sap_g,
            on_cols=["codigo", "descripcion", "ubiest"],
            left_name="SAAD CBP",
            right_name="SAP COLGATE"
        ).sort_values(["ubiest", "codigo"])

        # BUNKER vs SAP
        if split_by_estado:
            left  = bunker_f.groupby(["codigo", "descripcion", "ubiest", "estado"], as_index=False)["cajas"].sum()
            right = sap_bun.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()

            bun_vs_sap = pd.merge(left, right, on=["codigo", "descripcion", "ubiest"], how="left")
            bun_vs_sap["SAAD BUNKER"] = bun_vs_sap.pop("cajas_x").fillna(0).astype(int)
            bun_vs_sap["SAP COLGATE"] = bun_vs_sap.pop("cajas_y").fillna(0).astype(int)
            bun_vs_sap["DIFERENCIA"]  = (bun_vs_sap["SAAD BUNKER"] - bun_vs_sap["SAP COLGATE"]).astype(int)
            bun_vs_sap["estado_ord"]  = bun_vs_sap["estado"].map(_estado_key)
            bun_vs_sap = bun_vs_sap[["codigo", "descripcion", "ubiest", "estado",
                                     "SAAD BUNKER", "SAP COLGATE", "DIFERENCIA", "estado_ord"]]
            bun_vs_sap = bun_vs_sap.sort_values(by=["ubiest", "codigo", "estado_ord"]).drop(columns=["estado_ord"])
        else:
            bun_vs_sap = _cruzar(
                df_left=bunker_f.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum(),
                df_right=sap_bun.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum(),
                on_cols=["codigo", "descripcion", "ubiest"],
                left_name="SAAD BUNKER",
                right_name="SAP COLGATE"
            ).sort_values(["ubiest", "codigo"])

        # 6) Resumen
        resumen_rows = []
        resumen_rows.append({"Sección": "Storages CBP usados", "Valor": ", ".join(stor_cbp) or "(todos)"})
        resumen_rows.append({"Sección": "Storages BUNKER usados", "Valor": ", ".join(stor_bun) or "(todos)"})
        resumen_rows.append({"Sección": "Split por estado (BUNKER)", "Valor": str(split_by_estado)})

        resumen = pd.DataFrame(resumen_rows)

        # 7) Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            resumen.to_excel(writer, index=False, sheet_name="Resumen")
            cbp_vs_sap.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
            bun_vs_sap.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
        )

    except ValueError as e:
        raise HTTPException(400, f"{e}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error al procesar: {e}")
