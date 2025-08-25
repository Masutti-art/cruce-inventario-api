# main.py  (100%)
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Tuple, Dict
import pandas as pd
import io, re

app = FastAPI(title="Cruce Inventario API", version="1.0")

# -------------------- utilidades --------------------

SAP_STATUS_VALIDOS = {"UR", "QI", "BL"}        # sumamos todos
DEFAULT_MAP_CBPsap  = {"COL"}                  # CBP vs SAP -> COL
DEFAULT_MAP_BUNKsap = {"AER"}                  # BUNKER vs SAP -> AER

def _norm_code(x: str) -> str:
    """Normaliza códigos: quita espacios, símbolos; recorta prefijo '0000000' delante de alfanuméricos"""
    s = str(x).strip().upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    # SAAD suele tener '0000000' delante cuando el código contiene letras
    if s.startswith("0000000") and any(c.isalpha() for c in s):
        s = s[7:]
    return s

def _parse_spanish_number(x) -> int:
    """Convierte '1.545,000' -> 1545000; '20,000' -> 20000; números vacíos -> 0"""
    if x is None:
        return 0
    s = str(x).strip()
    if s == "" or s.upper() == "NAN":
        return 0
    # hay casos que ya vienen como número
    try:
        return int(float(s))
    except Exception:
        pass
    # formato es-CL: miles con punto, decimales con coma
    s = s.replace(".", "").replace(",", "")
    if s == "" or not s.isdigit():
        return 0
    return int(s)

def _col_like(df: pd.DataFrame, *cands: str) -> str:
    """Busca una columna por candidatos (case-insensitive). Lanza si no existe."""
    cols = {c.lower(): c for c in df.columns}
    for name in cands:
        if name.lower() in cols:
            return cols[name.lower()]
    raise KeyError(f"No se encontró ninguna de las columnas {cands} en: {list(df.columns)}")

# -------------------- lectura y normalización --------------------

def leer_sap(raw: UploadFile) -> pd.DataFrame:
    # Columnas admitidas
    # Material, Material Description, Storage Location, Status, BUM Quantity
    df = pd.read_excel(raw.file, dtype=str, engine="openpyxl" if raw.filename.lower().endswith("xlsx") else None)
    # nombres reales
    c_mat   = _col_like(df, "Material", "material")
    c_desc  = _col_like(df, "Material Description", "material description", "descripcion", "description")
    c_sto   = _col_like(df, "Storage Location", "storage location", "storage", "almacen")
    # status a veces no viene; si no está, asumimos todo válido
    c_stat  = None
    try:
        c_stat = _col_like(df, "Status", "Estatus", "status")
    except Exception:
        pass
    c_qty   = _col_like(df, "BUM Quantity", "quantity", "qty", "bum quantity")

    out = pd.DataFrame({
        "code":  df[c_mat].map(_norm_code),
        "desc":  df[c_desc].fillna("").astype(str),
        "storage": df[c_sto].fillna("").str.upper().str.strip(),
        "qty":   df[c_qty].map(_parse_spanish_number)
    })
    if c_stat:
        out["status"] = df[c_stat].fillna("").str.upper().str.strip()
    else:
        out["status"] = "UR"   # si no hay status, lo tratamos como válido
    # sumamos por code, storage, status
    out = out.groupby(["code", "desc", "storage", "status"], as_index=False)["qty"].sum()
    return out

def leer_saad(raw: UploadFile) -> pd.DataFrame:
    # Columnas admitidas: ubprod, itdesc, ubiest, ubcstk
    df = pd.read_excel(raw.file, dtype=str, engine="xlrd" if raw.filename.lower().endswith(".xls") else "openpyxl")
    c_code = _col_like(df, "ubprod", "codigo", "code")
    c_desc = _col_like(df, "itdesc", "descripcion", "description")
    c_sto  = _col_like(df, "ubiest", "storage", "almacen")
    c_qty  = _col_like(df, "ubcstk", "cantidad", "qty", "stock")

    out = pd.DataFrame({
        "code":  df[c_code].map(_norm_code),
        "desc":  df[c_desc].fillna("").astype(str),
        "storage": df[c_sto].fillna("").str.upper().str.strip(),
        "qty":   df[c_qty].map(_parse_spanish_number)
    })
    # agregamos por code, storage
    out = out.groupby(["code", "desc", "storage"], as_index=False)["qty"].sum()
    return out

# -------------------- cruce --------------------

def _merge_cruce(saad: pd.DataFrame,
                 sap: pd.DataFrame,
                 storages_sap: set) -> pd.DataFrame:
    """
    Compara SAAD vs SAP para los storages_sap indicados.
    Lado SAP: suma qty por (code) considerando solo storages de storages_sap y status válidos (UR/QI/BL).
    """
    sap_fil = sap[(sap["status"].isin(SAP_STATUS_VALIDOS)) & (sap["storage"].isin(storages_sap))]
    sap_sum = sap_fil.groupby("code", as_index=False)["qty"].sum().rename(columns={"qty": "sap"})
    # lado SAAD: ya viene por storage específico (filtramos por coincidencia con storages_sap si aplica)
    saad_fil = saad[saad["storage"].isin(storages_sap)] if "storage" in saad.columns else saad.copy()
    saad_sum = saad_fil.groupby(["code", "desc"], as_index=False)["qty"].sum().rename(columns={"qty": "saad"})

    merged = pd.merge(saad_sum, sap_sum, on="code", how="outer")
    # descripción: preferimos la de SAAD; si no hay, buscamos alguna de SAP (no estricta)
    # para ello mapeamos cualquier desc de SAAD por code; si falta, queda vacío
    merged["desc"] = merged["desc"].fillna("")
    merged["saad"] = merged["saad"].fillna(0).astype(int)
    merged["sap"]  = merged["sap"].fillna(0).astype(int)
    merged["diferencia"] = merged["saad"] - merged["sap"]
    merged = merged[["code", "desc", "saad", "sap", "diferencia"]]
    merged = merged.sort_values(by="diferencia", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
    return merged

def _resumenhoja(df: pd.DataFrame, titulo: str) -> Dict[str, int]:
    return {
        f"{titulo}_items": int(len(df)),
        f"{titulo}_con_diferencia": int((df["diferencia"] != 0).sum()),
        f"{titulo}_sin_diferencia": int((df["diferencia"] == 0).sum()),
        f"{titulo}_saad_total": int(df["saad"].sum()),
        f"{titulo}_sap_total": int(df["sap"].sum()),
        f"{titulo}_delta_total": int(df["diferencia"].sum()),
    }

# -------------------- endpoint único --------------------

@app.post("/cruce-xlsx", summary="Sube SAP + SAAD (CBP y BUNKER) y devuelve un .xlsx con el cruce")
async def cruce_xlsx(files: List[UploadFile] = File(..., description="Sube EXACTAMENTE 3 archivos: SAP.xlsx, SAAD_CBP.xls y SAAD_BUNKER.xls (en cualquier orden)")):
    if len(files) != 3:
        raise HTTPException(400, detail="Debes subir exactamente 3 archivos: SAP, SAAD-CBP y SAAD-BUNKER")

    # Intentamos identificar quién es quién mirando cabeceras
    tipos = {}
    for f in files:
        name = f.filename.lower()
        if "sap" in name:
            tipos["sap"] = f
        elif "cbp" in name:
            tipos["cbp"] = f
        elif "bunker" in name or "bkr" in name:
            tipos["bunker"] = f

    # si los nombres no ayudan, igual leemos y determinamos por columnas
    try:
        sap = leer_sap(tipos.get("sap") or files[0])
        cbp = leer_saad(tipos.get("cbp") or files[1])
        bunker = leer_saad(tipos.get("bunker") or files[2])
    except Exception:
        # segundo intento: leer todos y luego decidir por columnas
        # (cuando los nombres no contienen pistas)
        tmp = [leer_sap(f) if any(k in f.filename.lower() for k in ["sap"]) else None for f in files]
        if sum(t is not None for t in tmp) != 1:
            # forzamos el primero como sap
            tmp[0] = leer_sap(files[0])
        sap = next(t for t in tmp if t is not None)
        otros = [files[i] for i, t in enumerate(tmp) if t is None]
        cbp = leer_saad(otros[0])
        bunker = leer_saad(otros[1])

    # Cruces (respeta storage): CBP<->SAP usando COL ; BUNKER<->SAP usando AER
    cbp_vs_sap = _merge_cruce(cbp, sap, DEFAULT_MAP_CBPsap)
    bkr_vs_sap = _merge_cruce(bunker, sap, DEFAULT_MAP_BUNKsap)

    # Resumen
    resumen = {**_resumenhoja(cbp_vs_sap, "CBP_vs_SAP"),
               **_resumenhoja(bkr_vs_sap, "BUNKER_vs_SAP")}
    resumen_df = pd.DataFrame([resumen]).T.reset_index()
    resumen_df.columns = ["metrica", "valor"]

    # Excel en memoria
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
        cbp_vs_sap.rename(columns={"code":"codigo","desc":"descripcion","saad":"SAAD CBP","sap":"SAP COLGATE","diferencia":"DIFERENCIA"}).to_excel(xw, index=False, sheet_name="CBP_vs_SAP")
        bkr_vs_sap.rename(columns={"code":"codigo","desc":"descripcion","saad":"SAAD BUNKER","sap":"SAP COLGATE","diferencia":"DIFERENCIA"}).to_excel(xw, index=False, sheet_name="BUNKER_vs_SAP")
        resumen_df.to_excel(xw, index=False, sheet_name="Resumen")
    buf.seek(0)

    headers = {"Content-Disposition": 'attachment; filename="cruce_inventario.xlsx"'}
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

@app.get("/healthz")
def healthz():
    return JSONResponse({"ok": True})
