from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import re

app = FastAPI(title="Cruce Inventario API", version="1.0")

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

# ------------------ Lecturas ------------------

def read_sap(file: UploadFile) -> pd.DataFrame:
    """
    SAP: columnas esperadas:
    - Material (código)
    - Material Description (descripción)
    - Storage Location (ej. AER/OLR/COL)
    - BUM Quantity (cajas)
    """
    engine = _pick_engine(file.filename)
    df = pd.read_excel(file.file, engine=engine, dtype=str)

    cols = {c.lower(): c for c in df.columns}
    def get(namepart):
        for k, v in cols.items():
            if namepart in k:
                return v
        return None

    col_code     = get("material")
    col_desc     = get("material description")
    col_storage  = get("storage location")
    col_qty      = get("bum quantity")

    if not all([col_code, col_desc, col_storage, col_qty]):
        raise ValueError("SAP: no encuentro columnas (Material, Material Description, Storage Location, BUM Quantity).")

    sap = pd.DataFrame({
        "codigo":      df[col_code].map(_norm_code),
        "descripcion": df[col_desc].map(_norm_text),
        "ubiest":      df[col_storage].map(lambda x: _norm_text(x).upper()),
        "cajas":       df[col_qty].map(_to_int),
    })
    sap = sap.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
    return sap

def read_saad_cpb(file: UploadFile) -> pd.DataFrame:
    """
    SAAD CBP: columnas
    - ubprod (código)
    - itdesc (descripción)
    - ubiest (storage)
    - ubcstk (cajas)
    """
    engine = _pick_engine(file.filename)
    df = pd.read_excel(file.file, engine=engine, dtype=str)

    def pick_like(part):
        for c in df.columns:
            if part in c.lower():
                return c
        return None

    col_code    = pick_like("ubprod")
    col_desc    = pick_like("itdesc")
    col_ubiest  = pick_like("ubiest")
    col_qty     = pick_like("ubcstk")

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

def read_saad_bunker(file: UploadFile, split_by_estado: bool = True) -> pd.DataFrame:
    """
    SAAD BUNKER: columnas
    - ubprod (código)
    - itdesc (descripción)
    - ubiest (ej: 'OLR - UR')  -> storage + estado
    - ubcstk (cajas)
    """
    engine = _pick_engine(file.filename)
    df = pd.read_excel(file.file, engine=engine, dtype=str)

    def pick_like(part):
        for c in df.columns:
            if part in c.lower():
                return c
        return None

    col_code    = pick_like("ubprod")
    col_desc    = pick_like("itdesc")
    col_ubiest  = pick_like("ubiest")
    col_qty     = pick_like("ubcstk")

    if not all([col_code, col_desc, col_ubiest, col_qty]):
        raise ValueError("SAAD BUNKER: faltan columnas (ubprod, itdesc, ubiest, ubcstk).")

    # Forzamos string para que .str funcione sí o sí
    ub_raw = df[col_ubiest].astype(str)

    raw = pd.DataFrame({
        "codigo":      df[col_code].map(_norm_code),
        "descripcion": df[col_desc].map(_norm_text),
        "ubiest_raw":  ub_raw.map(_norm_text),
        "cajas":       df[col_qty].map(_to_int),
    })

    # ubiest_raw: 'OLR - UR' o 'AER - QI' ...
    raw["ubiest"] = raw["ubiest_raw"].str.extract(r"^\s*([A-Za-z]+)").fillna("").str.upper()
    raw["estado"] = raw["ubiest_raw"].str.extract(r"-\s*([A-Za-z]+)\s*$").fillna("").str.upper()

    if split_by_estado:
        out = raw.groupby(["codigo", "descripcion", "ubiest", "estado"], as_index=False)["cajas"].sum()
    else:
        out = raw.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        out["estado"] = ""
    return out

# ------------------ Cruces ------------------

def cruzar(df_left: pd.DataFrame, df_right: pd.DataFrame,
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
    files: list[UploadFile] = File(..., description="SAP.xlsx, CBP.xls/xlsx, BUNKER.xls/xlsx (en ese orden)"),
    sap_cbp_storages: str = Query("", description="Storages para CBP/SAP (coma-separado, ej: COL)"),
    sap_bunker_storages: str = Query("", description="Storages para BUNKER/SAP (coma-separado, ej: AER,OLR)"),
    split_by_estado: bool = Query(True, description="Si True, BUNKER separa por estado (UR/QI/BL)"),
):
    try:
        if len(files) < 3:
            raise HTTPException(400, "Subí 3 archivos: SAP, SAAD CBP y SAAD BUNKER.")

        sap_file, cbp_file, bunker_file = files

        sap    = read_sap(sap_file)
        cbp    = read_saad_cpb(cbp_file)
        bunker = read_saad_bunker(bunker_file, split_by_estado=split_by_estado)

        # Filtros por storage (ubiest)
        stor_cbp = [s.strip().upper() for s in sap_cbp_storages.split(",") if s.strip()]
        stor_bun = [s.strip().upper() for s in sap_bunker_storages.split(",") if s.strip()]

        sap_cbp = sap[sap["ubiest"].isin(stor_cbp)] if stor_cbp else sap.copy()
        cbp_f   = cbp[cbp["ubiest"].isin(stor_cbp)] if stor_cbp else cbp.copy()

        sap_bun   = sap[sap["ubiest"].isin(stor_bun)] if stor_bun else sap.copy()
        bunker_f  = bunker[bunker["ubiest"].isin(stor_bun)] if stor_bun else bunker.copy()

        # ---- CBP vs SAP (con ubiest) ----
        cbp_g = cbp_f.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        sap_g = sap_cbp.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        cbp_vs_sap = cruzar(
            df_left=cbp_g,
            df_right=sap_g,
            on_cols=["codigo", "descripcion", "ubiest"],
            left_name="SAAD CBP",
            right_name="SAP COLGATE"
        ).sort_values(["ubiest", "codigo"])

        # ---- BUNKER vs SAP ----
        if split_by_estado:
            left  = bunker_f.groupby(["codigo", "descripcion", "ubiest", "estado"], as_index=False)["cajas"].sum()
            right = sap_bun.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()

            bun_vs_sap = pd.merge(left, right, on=["codigo", "descripcion", "ubiest"], how="left")
            bun_vs_sap["SAAD BUNKER"] = bun_vs_sap.pop("cajas_x").fillna(0).astype(int)
            bun_vs_sap["SAP COLGATE"] = bun_vs_sap.pop("cajas_y").fillna(0).astype(int)
            bun_vs_sap["DIFERENCIA"]  = (bun_vs_sap["SAAD BUNKER"] - bun_vs_sap["SAP COLGATE"]).astype(int)

            # Orden robusto sin key (evita el error)
            bun_vs_sap["estado_ord"] = bun_vs_sap["estado"].map(_estado_key)
            bun_vs_sap = bun_vs_sap[["codigo", "descripcion", "ubiest", "estado",
                                     "SAAD BUNKER", "SAP COLGATE", "DIFERENCIA", "estado_ord"]]
            bun_vs_sap = bun_vs_sap.sort_values(by=["ubiest", "codigo", "estado_ord"]).drop(columns=["estado_ord"])
        else:
            bun_vs_sap = cruzar(
                df_left=bunker_f.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum(),
                df_right=sap_bun.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum(),
                on_cols=["codigo", "descripcion", "ubiest"],
                left_name="SAAD BUNKER",
                right_name="SAP COLGATE"
            ).sort_values(["ubiest", "codigo"])

        # ---- Excel ----
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
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
    except Exception as e:
        raise HTTPException(500, f"Error al procesar: {e}")
