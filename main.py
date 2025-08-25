from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, PlainTextResponse
from typing import List, Tuple
import pandas as pd
import io
import re

app = FastAPI(title="Cruce Inventario API", version="1.0")

# ---------- utilidades ----------

SAP_STORAGES_BUNKER = {"AER", "MOV", "RT", "RP", "BN", "OLR"}   # SAP que van a BUNKER
SAP_STORAGES_CBP    = {"COL"}                                   # SAP que comparan con CBP
BUNKER_PREFIXES     = {"OLR", "OLB"}                             # SAAD bunker (normalizo por prefijo)

def _engine_for(filename: str) -> str:
    fn = filename.lower()
    if fn.endswith(".xlsx"):
        return "openpyxl"
    if fn.endswith(".xls"):
        # xlrd>=2.0.1 sólo maneja .xls -> requisito en requirements
        return "xlrd"
    raise HTTPException(status_code=400, detail=f"Formato no soportado: {filename}")

def _clean_code(x) -> str:
    """Deja sólo A-Z/0-9, quita espacios y ceros a la izquierda."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    # números que vienen como 61029353.0
    if s.endswith(".0"):
        s = s[:-2]
    # saco todo lo que no sea alfanumérico
    s = re.sub(r"[^A-Z0-9]", "", s)
    # quito ceros a la izquierda
    s = s.lstrip("0")
    return s

def _to_number_series(s: pd.Series) -> pd.Series:
    """Convierte serie con miles/decimales latinos a número, NaN->0, redondea a int."""
    # muchas veces viene '1.611,000' => quito miles y pongo punto decimal
    s = s.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    s = pd.to_numeric(s, errors="coerce").fillna(0)
    # cantidades de cajas -> entero
    return s.round().astype(int)

def _read_sap(file: UploadFile) -> pd.DataFrame:
    # Columnas esperadas (nombres típicos): 'Material', 'Material Description', 'Storage Location', 'BUM Quantity'
    eng = _engine_for(file.filename)
    data = file.file.read()
    df = pd.read_excel(io.BytesIO(data), engine=eng, dtype=str)
    # intento mapear columnas por nombre "parecido"
    colmap = {}
    for c in df.columns:
        cl = c.strip().lower()
        if "material description" in cl or "descripcion" in cl:
            colmap["desc"] = c
        elif cl in ("material", "codigo", "code", "sku"):
            colmap["code"] = c
        elif "storage" in cl and "location" in cl:
            colmap["storage"] = c
        elif ("bum" in cl and "quantity" in cl) or "quantity" in cl or "qty" in cl:
            colmap["qty"] = c
    req = {"code","desc","storage","qty"}
    if not req.issubset(colmap):
        raise HTTPException(status_code=400, detail="SAP: no pude detectar columnas (Material, Material Description, Storage Location, BUM Quantity)")

    out = pd.DataFrame({
        "code": df[colmap["code"]].map(_clean_code),
        "desc": df[colmap["desc"]].astype(str).str.strip(),
        "storage": df[colmap["storage"]].astype(str).str.strip().str.upper(),
        "qty": _to_number_series(df[colmap["qty"]])
    })
    # desc por código (me quedo con la 1ª no vacía)
    desc_map = (out.loc[out["desc"].ne(""), ["code","desc"]]
                   .drop_duplicates(subset=["code"])
                   .set_index("code")["desc"])
    return out, desc_map

def _read_saad(file: UploadFile) -> pd.DataFrame:
    # Columnas SAAD: ubprod (código), itdesc (descripción), ubiest (storage), ubcstk (stock)
    eng = _engine_for(file.filename)
    data = file.file.read()
    df = pd.read_excel(io.BytesIO(data), engine=eng, dtype=str)
    colmap = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl == "ubprod":
            colmap["code"] = c
        elif cl == "itdesc":
            colmap["desc"] = c
        elif cl == "ubiest":
            colmap["storage"] = c
        elif cl == "ubcstk":
            colmap["qty"] = c
    req = {"code","desc","storage","qty"}
    if not req.issubset(colmap):
        raise HTTPException(status_code=400, detail="SAAD: no pude detectar columnas (ubprod, itdesc, ubiest, ubcstk)")

    out = pd.DataFrame({
        "code": df[colmap["code"]].map(_clean_code),
        "desc": df[colmap["desc"]].astype(str).str.strip(),
        "storage": df[colmap["storage"]].astype(str).str.strip().str.upper(),
        "qty": _to_number_series(df[colmap["qty"]])
    })
    # normalizo storages raros tipo "OLR - UR" -> "OLR"
    out["storage"] = out["storage"].str.split().str[0]
    return out

def _agg_by_code(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve df por código con qty sumada (y desc la primera disponible)."""
    qty = df.groupby("code", as_index=False)["qty"].sum()
    desc = (df.loc[df["desc"].ne(""), ["code","desc"]]
              .drop_duplicates(subset=["code"]))
    res = qty.merge(desc, on="code", how="left")
    return res[["code","desc","qty"]]

def _join_and_layout(left: pd.DataFrame, right: pd.DataFrame,
                     left_name: str, right_name: str,
                     fallback_desc: pd.Series) -> pd.DataFrame:
    # left y right deben venir como (code, desc, qty)
    m = left.merge(right, on="code", how="outer", suffixes=("_l","_r"))
    m["qty_l"] = m["qty_l"].fillna(0).astype(int)
    m["qty_r"] = m["qty_r"].fillna(0).astype(int)

    # descripción: SAAD si existe, si no SAP, y si no la del mapa
    desc = m["desc_l"].fillna("").replace("nan","")
    desc = desc.mask(desc.eq(""), m["desc_r"])
    desc = desc.mask(desc.eq(""), m["code"].map(fallback_desc))
    m["descripcion"] = desc.fillna("")

    m = m.rename(columns={
        "qty_l": left_name,
        "qty_r": right_name
    })

    m["DIFERENCIA"] = m[left_name] - m[right_name]
    # orden absoluto desc
    m = m[["code","descripcion", left_name, right_name, "DIFERENCIA"]]
    m = m.sort_values(by="DIFERENCIA", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)
    # códigos vacíos -> descartar
    m = m[m["code"].astype(str).str.len() > 0]
    m = m.rename(columns={"code":"codigo"})
    return m

# ---------- endpoints ----------

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(files: List[UploadFile] = File(...)):
    """
    Sube 3 archivos en cualquier orden:
      - SAP (xlsx)
      - SAAD CBP (xls)
      - SAAD BUNKER (xls)
    Devuelve un .xlsx con dos hojas: CBP_vs_SAP y BUNKER_vs_SAP
    """
    if len(files) != 3:
        raise HTTPException(status_code=400, detail="Subí exactamente 3 archivos: SAP (.xlsx), SAAD CBP (.xls), SAAD BUNKER (.xls)")

    # intento identificar por nombre
    f_sap = next((f for f in files if "sap" in f.filename.lower()), None)
    f_cbp = next((f for f in files if "cbp" in f.filename.lower()), None)
    f_bunker = next((f for f in files if "bunker" in f.filename.lower()), None)

    if not all([f_sap, f_cbp, f_bunker]):
        # si no pude por nombre, igual leo todo y dejo error si algo falla
        names = ", ".join(f.filename for f in files)
        raise HTTPException(status_code=400, detail=f"No pude detectar claramente SAP/CBP/BUNKER por nombre de archivo. Subiste: {names}")

    # leo datasets
    sap_df, sap_desc_map = _read_sap(f_sap)
    cbp_df = _read_saad(f_cbp)
    bunker_df = _read_saad(f_bunker)

    # filtros por storage
    sap_cbp = sap_df[sap_df["storage"].isin(SAP_STORAGES_CBP)]
    sap_bunker = sap_df[sap_df["storage"].isin(SAP_STORAGES_BUNKER)]
    cbp_only = cbp_df[cbp_df["storage"].eq("COL")]
    bunker_only = bunker_df[bunker_df["storage"].str.startswith(tuple(BUNKER_PREFIXES))]

    # agrego por código
    sap_cbp_g = _agg_by_code(sap_cbp).rename(columns={"qty":"qty"})
    sap_bunker_g = _agg_by_code(sap_bunker).rename(columns={"qty":"qty"})
    cbp_g = _agg_by_code(cbp_only).rename(columns={"qty":"qty"})
    bunker_g = _agg_by_code(bunker_only).rename(columns={"qty":"qty"})

    # joins finales
    hoja_cbp = _join_and_layout(cbp_g, sap_cbp_g, "SAAD CBP", "SAP COLGATE", sap_desc_map)
    hoja_bunker = _join_and_layout(bunker_g, sap_bunker_g, "SAAD BUNKER", "SAP COLGATE", sap_desc_map)

    # exporto a xlsx
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        hoja_cbp.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
        hoja_bunker.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
        # formato ancho
        for ws in ("CBP_vs_SAP","BUNKER_vs_SAP"):
            w = writer.sheets[ws]
            w.set_column("A:A", 18)  # codigo
            w.set_column("B:B", 45)  # descripcion
            w.set_column("C:E", 14)  # números
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'}
    )
