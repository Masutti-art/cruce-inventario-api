# main.py  (respetando storage en CBP y SAP)
from __future__ import annotations
import io, re, unicodedata
from pathlib import Path
from typing import List, Optional, Tuple
import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="Cruce Inventario API", version="3.0.0")

# ---- Config
BUNKER_STORAGES = {"AER","MOV","RT","RP","BN","OLR"}

CODE_CAND   = ["material","codigo","code","sku","ubprod","item","matnr","referencia"]
STO_CAND    = ["storage","almacen","almacén","deposito","bodega","warehouse","location","ubiest","storage location"]
DESC_CAND   = ["material description","descripcion","description","desc","itdesc","denominacion","nombre"]
QTY_CAND    = ["bum quantity","cantidad","qty","stock","saldo","existencias","inventario","ubcstk","cajas","unidades"]

def norm(s:str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip().lower()
    return re.sub(r"[^a-z0-9 _\-]", "", s)

def guess_col(df: pd.DataFrame, cand: list, numeric: bool=False) -> Optional[str]:
    cols = {norm(c):c for c in df.columns}
    for k in [norm(x) for x in cand]:
        if k in cols: 
            c = cols[k]
            if not numeric or pd.api.types.is_numeric_dtype(df[c]): return c
    # contains
    for k,orig in cols.items():
        for c in cand:
            if norm(c) in k:
                if not numeric or pd.api.types.is_numeric_dtype(df[orig]): return orig
    # última chance numérica
    if numeric:
        num = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if num:
            s = df[num].abs().sum().sort_values(ascending=False)
            return str(s.index[0])
    return None

def read_any(file: UploadFile) -> pd.DataFrame:
    name = file.filename or "file"
    suf = Path(name).suffix.lower()
    file.file.seek(0)
    if suf in (".xls",".xlsx",".xlsm"):
        engine = "xlrd" if suf==".xls" else "openpyxl"
        # probar varios headers
        best,score = None,-1
        for h in range(0,25):
            try:
                file.file.seek(0)
                df = pd.read_excel(file.file, engine=engine, header=h)
                if df is None or df.empty: continue
                c = guess_col(df, CODE_CAND)
                if c is None: continue
                sc = int(df[c].notna().sum())
                if sc>score: best,score = df,sc
            except Exception:
                continue
        if best is not None: 
            file.file.seek(0)
            return best
        file.file.seek(0); 
        return pd.read_excel(file.file, engine=engine)
    else:
        file.file.seek(0)
        return pd.read_csv(file.file, sep=None, engine="python")

def parse_qty(x) -> float:
    if pd.isna(x): return 0.0
    s = str(x).strip().replace("\u00A0"," ")
    if s=="" or s=="-": return 0.0
    # formateo ES
    if "." in s and "," in s:
        s = s.replace(".","").replace(",",".")
    elif "," in s and "." not in s:
        s = s.replace(",",".")
    else:
        # dejar miles si hay (los removeremos abajo si fallara)
        pass
    try:
        return float(s)
    except Exception:
        s2 = re.sub(r"[^0-9\-\.]", "", s)
        if s2 in ("","-",".","-."): return 0.0
        try: return float(s2)
        except Exception: return 0.0

def clean_code(x) -> Optional[str]:
    if x is None: return None
    s = str(x).strip()
    if s=="" or s.upper() in {"NAN","NA"}: return None
    s = re.sub(r"^0+(?=[A-Za-z0-9])","", s)
    return s.upper()

def clean_storage(x) -> str:
    return "" if x is None else str(x).strip().upper()

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df.columns = [str(c).strip() for c in df.columns]
    c_code  = guess_col(df, CODE_CAND)
    c_sto   = guess_col(df, STO_CAND)
    c_desc  = guess_col(df, DESC_CAND)
    c_qty   = guess_col(df, QTY_CAND, numeric=True)
    if c_code is None:  raise ValueError("No se encontró columna de CÓDIGO.")
    if c_qty  is None:  raise ValueError("No se encontró columna de CANTIDAD.")
    if c_sto  is None:  c_sto = "storage"      # si no existe, vacío
    if c_desc is None:  c_desc = "descripcion" # si no existe, vacío
    out = pd.DataFrame({
        "codigo": df[c_code].map(clean_code),
        "storage": df[c_sto].map(clean_storage) if c_sto in df else "",
        "descripcion": df[c_desc] if c_desc in df else "",
        "cantidad": df[c_qty].map(parse_qty)
    })
    out = out.dropna(subset=["codigo"])
    out["storage"] = out["storage"].fillna("").astype(str).str.upper()
    # agrupar iguales code+storage
    out = out.groupby(["codigo","storage","descripcion"], as_index=False)["cantidad"].sum()
    return out

def detect_source(df: pd.DataFrame) -> str:
    # heurística simple: si muchas storages ∈ BUNKER_STORAGES -> BUNKER
    # si tiene "bum quantity"/"material description" -> SAP
    cols = [norm(c) for c in df.columns]
    if any("bum quantity" in c for c in cols) or any("material description" in c for c in cols):
        return "SAP"
    stor_prop = (df["storage"].isin(BUNKER_STORAGES).mean() if "storage" in df.columns else 0)
    if stor_prop > 0.6:
        return "BUNKER"
    return "CBP"

def pivot_por_sku(df: pd.DataFrame, fuente: str) -> pd.DataFrame:
    # suma todas las storages para ver totales por SKU
    p = df.groupby(["codigo"], as_index=False)["cantidad"].sum()
    p = p.rename(columns={"cantidad": f"{fuente}"})
    return p

def cruce_por_storage(saad: pd.DataFrame, sap: pd.DataFrame, fuente: str) -> pd.DataFrame:
    # Join por (codigo, storage)
    m = pd.merge(
        saad[["codigo","storage","descripcion","cantidad"]],
        sap[["codigo","storage","descripcion","cantidad"]],
        on=["codigo","storage"],
        how="outer",
        suffixes=(f"_{fuente.lower()}", "_sap")
    )
    m["cantidad_"+fuente.lower()] = m["cantidad_"+fuente.lower()].fillna(0.0)
    m["cantidad_sap"] = m["cantidad_sap"].fillna(0.0)
    # descripción preferimos la de SAP si existe
    m["descripcion"] = m["descripcion_sap"].where(m["descripcion_sap"].notna(), m["descripcion_"+fuente.lower()])
    m = m.drop(columns=["descripcion_sap","descripcion_"+fuente.lower()], errors="ignore")
    m["diferencia"] = m["cantidad_"+fuente.lower()] - m["cantidad_sap"]
    m["presente_en_saad"] = (m["cantidad_"+fuente.lower()] > 0).astype(int)
    m["presente_en_sap"]  = (m["cantidad_sap"] > 0).astype(int)
    m = m.sort_values(["codigo","storage"]).reset_index(drop=True)
    # columnas ordenadas
    m = m[["codigo","storage","descripcion",f"cantidad_{fuente.lower()}","cantidad_sap","diferencia","presente_en_saad","presente_en_sap"]]
    m = m.rename(columns={
        f"cantidad_{fuente.lower()}": f"SAAD {fuente}",
        "cantidad_sap": "SAP COLGATE",
        "diferencia": "DIFERENCIA"
    })
    return m

@app.get("/healthz")
def healthz():
    return {"ok": True, "version": app.version}

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(files: List[UploadFile] = File(...)):
    if len(files) != 3:
        return JSONResponse({"detail": "Subí exactamente 3 archivos: SAP, SAAD CBP y SAAD BUNKER."}, status_code=400)
    # normalizar
    normed = []
    for f in files:
        df_raw = read_any(f)
        df = normalize(df_raw)
        src = detect_source(df)
        normed.append((src, df))
    # separar
    try:
        sap_df     = next(d for s,d in normed if s=="SAP")
        cbp_df     = next(d for s,d in normed if s=="CBP")
        bunker_df  = next(d for s,d in normed if s=="BUNKER")
    except StopIteration:
        return JSONResponse({"detail": "No pude detectar claramente SAP/CBP/BUNKER. Verificá encabezados."}, status_code=400)
    # BUNKER: filtrar storages válidos
    bunker_df = bunker_df[bunker_df["storage"].isin(BUNKER_STORAGES)].copy()

    # CRUCES por (codigo, storage)
    cbp_vs_sap     = cruce_por_storage(cbp_df, sap_df, "CBP")
    bunker_vs_sap  = cruce_por_storage(bunker_df, sap_df, "BUNKER")

    # Resúmenes por SKU (sumando storages)
    cbp_sku    = pivot_por_sku(cbp_df, "SAAD CBP")
    sap_sku    = pivot_por_sku(sap_df, "SAP")
    bunk_sku   = pivot_por_sku(bunker_df, "SAAD BUNKER")

    cbp_full = pd.merge(cbp_sku, sap_sku, on="codigo", how="outer").fillna(0.0)
    cbp_full["DIFERENCIA"] = cbp_full["SAAD CBP"] - cbp_full["SAP"]

    bunk_full = pd.merge(bunk_sku, sap_sku, on="codigo", how="outer").fillna(0.0)
    bunk_full["DIFERENCIA"] = bunk_full["SAAD BUNKER"] - bunk_full["SAP"]

    # Excel
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as xw:
        cbp_vs_sap.to_excel(xw, index=False, sheet_name="CBP_vs_SAP_por_storage")
        bunker_vs_sap.to_excel(xw, index=False, sheet_name="BUNKER_vs_SAP_por_storage")
        cbp_full.sort_values("codigo").to_excel(xw, index=False, sheet_name="CBP_por_SKU")
        bunk_full.sort_values("codigo").to_excel(xw, index=False, sheet_name="BUNKER_por_SKU")
    out.seek(0)
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="cruce_por_storage.xlsx"'}
    )

# Alias opcional para mantener compatibilidad
@app.post("/cruce-xlsx")
async def cruce_xlsx_compat(files: List[UploadFile] = File(...)):
    return await cruce_auto_xlsx(files)
