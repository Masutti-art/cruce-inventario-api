from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import io, re

app = FastAPI(title="Cruce Inventario API")

# ----------------- utilidades -----------------

def _norm_header(df: pd.DataFrame) -> pd.DataFrame:
    # recorta espacios y colapsa múltiplos a uno solo
    return df.rename(columns=lambda c: re.sub(r"\s+", " ", str(c)).strip())

def _num_latino_a_float(x):
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}: return 0.0
    s = re.sub(r"[^0-9\.,-]", "", s)
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        elif s.count(".") > 1:
            s = s.replace(".", "")
    try:
        return float(s)
    except:
        return 0.0

def _pick_col(cols, candidates):
    pos = {re.sub(r"\s+", " ", c).strip().lower(): c for c in cols}
    for c in candidates:
        k = re.sub(r"\s+", " ", c).strip().lower()
        if k in pos: return pos[k]
    return None

def _read_all_sheets(file, wanted_checker):
    """lee todas las hojas y concatena las que tengan las columnas requeridas"""
    try:
        wb = pd.read_excel(file, dtype=str, sheet_name=None)
    except Exception:
        file.seek(0)
        wb = pd.read_excel(file, dtype=str, sheet_name=None, engine="openpyxl")
    frames = []
    for name, df in wb.items():
        if df is None or df.empty: continue
        df = _norm_header(df)
        if wanted_checker(df.columns):
            frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()

# ----------------- lectores -----------------

def leer_sap(f) -> pd.DataFrame:
    def tiene_cols(cols):
        return (_pick_col(cols, ["Material"]) and
                _pick_col(cols, ["Material Description"]) and
                _pick_col(cols, ["Storage Location"]) and
                _pick_col(cols, ["BUM Quantity", "Quantity"]))
    df = _read_all_sheets(f, tiene_cols)
    if df.empty:
        raise HTTPException(400, "No encontré columnas SAP (Material, Material Description, Storage Location, BUM Quantity).")
    c_code = _pick_col(df.columns, ["Material"])
    c_desc = _pick_col(df.columns, ["Material Description"])
    c_stg  = _pick_col(df.columns, ["Storage Location"])
    c_qty  = _pick_col(df.columns, ["BUM Quantity","Quantity"])
    df = df[[c_code, c_desc, c_stg, c_qty]].copy()
    df.columns = ["code","desc","storage","qty_raw"]
    df["qty"] = df["qty_raw"].map(_num_latino_a_float).fillna(0.0)
    df["storage"] = df["storage"].fillna("").astype(str).str.strip().str.upper()
    sap = (df.groupby(["code","storage"], as_index=False)
             .agg(desc=("desc","first"), qty=("qty","sum")))
    return sap

def leer_saad(f) -> pd.DataFrame:
    def tiene_cols(cols):
        return (_pick_col(cols, ["ubprod"]) and
                _pick_col(cols, ["itdesc"]) and
                _pick_col(cols, ["ubiest"]) and
                _pick_col(cols, ["ubcstk"]))
    df = _read_all_sheets(f, tiene_cols)
    if df.empty:
        raise HTTPException(400, "No encontré columnas SAAD (ubprod, itdesc, ubiest, ubcstk).")
    c_code = _pick_col(df.columns, ["ubprod"])
    c_desc = _pick_col(df.columns, ["itdesc"])
    c_stg  = _pick_col(df.columns, ["ubiest"])
    c_qty  = _pick_col(df.columns, ["ubcstk"])
    df = df[[c_code, c_desc, c_stg, c_qty]].copy()
    df.columns = ["code","desc","storage","qty_raw"]
    df["qty"] = df["qty_raw"].map(_num_latino_a_float).fillna(0.0)
    df["storage"] = df["storage"].fillna("").astype(str).str.strip().str.upper()
    saad = (df.groupby(["code","storage"], as_index=False)
              .agg(desc=("desc","first"), qty=("qty","sum")))
    return saad

# ----------------- comparadores -----------------

def comparar_cbp_vs_sap(sap: pd.DataFrame, cbp: pd.DataFrame) -> pd.DataFrame:
    sap_cbp = sap[sap["storage"].str.strip().str.upper() != "AER"]
    sap_cbp = sap_cbp.groupby("code", as_index=False).agg(desc=("desc","first"), sap_qty=("qty","sum"))
    saad_cbp = cbp.groupby("code", as_index=False).agg(desc=("desc","first"), saad_qty=("qty","sum"))
    m = pd.merge(saad_cbp, sap_cbp, on="code", how="outer", suffixes=("_saad","_sap"))
    m["descripcion"] = m["desc_saad"].fillna(m["desc_sap"])
    m["saad"] = m["saad_qty"].fillna(0.0).astype(float)
    m["sap"]  = m["sap_qty"].fillna(0.0).astype(float)
    m["diferencia"] = m["saad"] - m["sap"]
    out = m[["code","descripcion","saad","sap","diferencia"]].rename(
        columns={"code":"codigo","saad":"SAAD CBP","sap":"SAP COLGATE","diferencia":"DIFERENCIA"})
    return out.sort_values(by="DIFERENCIA", key=lambda s: s.abs(), ascending=False)

def comparar_bunker_vs_sap(sap: pd.DataFrame, bunker: pd.DataFrame) -> pd.DataFrame:
    sap_bun = sap[sap["storage"].str.strip().str.upper() == "AER"]
    sap_bun = sap_bun.groupby("code", as_index=False).agg(desc=("desc","first"), sap_qty=("qty","sum"))
    saad_bun = bunker.groupby("code", as_index=False).agg(desc=("desc","first"), saad_qty=("qty","sum"))
    m = pd.merge(saad_bun, sap_bun, on="code", how="outer", suffixes=("_saad","_sap"))
    m["descripcion"] = m["desc_saad"].fillna(m["desc_sap"])
    m["saad"] = m["saad_qty"].fillna(0.0).astype(float)
    m["sap"]  = m["sap_qty"].fillna(0.0).astype(float)
    m["diferencia"] = m["saad"] - m["sap"]
    out = m[["code","descripcion","saad","sap","diferencia"]].rename(
        columns={"code":"codigo","saad":"SAAD BUNKER","sap":"SAP COLGATE","diferencia":"DIFERENCIA"})
    return out.sort_values(by="DIFERENCIA", key=lambda s: s.abs(), ascending=False)

def detectar(f: UploadFile):
    name = (f.filename or "").upper()
    if "SAP" in name: return "sap"
    if "CBP" in name: return "cbp"
    if "BUNKER" in name or "BKR" in name: return "bunker"
    return None

@app.post("/healthz")
def healthz(): return {"ok": True}

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(files: list[UploadFile] = File(...)):
    if len(files) < 3:
        raise HTTPException(400, "Subí los 3 archivos: SAP, CBP y BUNKER.")
    buckets = {"sap": None, "cbp": None, "bunker": None}
    for f in files:
        t = detectar(f)
        if not t: raise HTTPException(400, f"No pude detectar tipo por nombre: {f.filename}")
        buckets[t] = f
    if not all(buckets.values()):
        raise HTTPException(400, "No pude detectar claramente SAP/CBP/BUNKER. Renombrá archivos.")

    sap_df    = leer_sap(buckets["sap"].file)
    cbp_df    = leer_saad(buckets["cbp"].file)
    bunker_df = leer_saad(buckets["bunker"].file)

    cbp_vs = comparar_cbp_vs_sap(sap_df, cbp_df)
    bun_vs = comparar_bunker_vs_sap(sap_df, bunker_df)

    # LOG para diagnóstico rápido
    log = pd.DataFrame([
        {"dato":"SAP filas", "valor": int(len(sap_df))},
        {"dato":"SAP storages", "valor": ", ".join(sorted(sap_df['storage'].unique()))},
        {"dato":"SAAD CBP filas", "valor": int(len(cbp_df))},
        {"dato":"SAAD BUNKER filas", "valor": int(len(bunker_df))},
        {"dato":"CBP_vs_SAP filas", "valor": int(len(cbp_vs))},
        {"dato":"BUNKER_vs_SAP filas", "valor": int(len(bun_vs))}
    ])

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as xw:
        cbp_vs.to_excel(xw, index=False, sheet_name="CBP_vs_SAP")
        bun_vs.to_excel(xw, index=False, sheet_name="BUNKER_vs_SAP")
        log.to_excel(xw, index=False, sheet_name="LOG")
        for sheet in ("CBP_vs_SAP","BUNKER_vs_SAP","LOG"):
            ws = xw.sheets[sheet]; ws.set_column(0, 5, 18)
        xw.sheets["CBP_vs_SAP"].set_column(1,1,45)
        xw.sheets["BUNKER_vs_SAP"].set_column(1,1,45)
    out.seek(0)
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
    )
