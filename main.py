from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import StreamingResponse, PlainTextResponse
from typing import List, Optional, Tuple
import io
import pandas as pd

app = FastAPI(title="Cruce Inventario API", version="1.0")

# ==========================
# Lectura universal de files
# ==========================

def read_any_table(upload: UploadFile, *, dtype=str) -> pd.DataFrame:
    """
    Lee automáticamente .xlsx, .xls, .xlsb o .csv desde UploadFile.
    Devuelve un DataFrame con dtype=str para evitar NaN raros.
    """
    raw = upload.file.read()
    upload.file.seek(0)
    name = (upload.filename or "").strip().lower()

    # Rutas por extensión
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(raw), dtype=dtype, encoding="utf-8", sep=",")

    if name.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(raw), engine="openpyxl", dtype=dtype)

    if name.endswith(".xls"):
        # Necesita xlrd==1.2.0
        return pd.read_excel(io.BytesIO(raw), engine="xlrd", dtype=dtype)

    if name.endswith(".xlsb"):
        # Necesita pyxlsb
        return pd.read_excel(io.BytesIO(raw), engine="pyxlsb", dtype=dtype)

    # Si no hay extensión fiable, probamos en cascada
    for engine in ("openpyxl", "xlrd", "pyxlsb"):
        try:
            return pd.read_excel(io.BytesIO(raw), engine=engine, dtype=dtype)
        except Exception:
            pass

    # CSV por último
    try:
        return pd.read_csv(io.BytesIO(raw), dtype=dtype, encoding="utf-8", sep=",")
    except Exception as e:
        raise ValueError(f"No pude identificar/leer el archivo '{upload.filename}': {e}")


# ======================================
# Utilitarios de normalización de campos
# ======================================

def norm_code(x: str) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    # Quitamos leading zeros excesivos pero mantenemos alfanuméricos
    # si empezara con letras, lo dejamos como está.
    if s.isdigit():
        return str(int(s))
    return s

def norm_text(x: str) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def parse_qty(x) -> int:
    """
    Convierte strings tipo '1.545,000' o '6,632,94' a enteros.
    Reglas simples:
     - quitamos espacios
     - si tiene coma y no punto -> coma = decimal -> reemplazamos por '.'
     - si tiene punto y coma -> asumimos '.' como miles y ',' como dec -> drop '.' y usamos parte entera antes de ','
     - si solo tiene puntos -> quitamos puntos (miles)
     - si es float/num -> casteamos
    """
    if pd.isna(x):
        return 0
    if isinstance(x, (int, float)):
        try:
            return int(round(float(x)))
        except Exception:
            return 0

    s = str(x).strip().replace(" ", "")
    if s == "":
        return 0

    # Caso "1.545,000" -> 1545
    if "." in s and "," in s:
        s = s.replace(".", "")
        s = s.split(",")[0]
        try:
            return int(s)
        except Exception:
            pass

    # Caso "6,632,94" (raro) -> quitamos comas y tomamos int
    if s.count(",") > 1 and "." not in s:
        s = s.replace(",", "")
        try:
            return int(s)
        except Exception:
            return 0

    # Caso "1545,000" -> 1545
    if "," in s and "." not in s:
        s = s.split(",")[0]
        try:
            return int(s)
        except Exception:
            pass

    # Caso "1.234.567" -> 1234567
    if "." in s and "," not in s:
        s = s.replace(".", "")
        try:
            return int(s)
        except Exception:
            pass

    # Último intento
    try:
        return int(round(float(s)))
    except Exception:
        return 0


# ======================================
# Detección de tipo de archivo (SAP/SAAD)
# ======================================

def looks_like_sap(df: pd.DataFrame) -> bool:
    cols = {c.lower() for c in df.columns}
    return (
        ("material" in cols or "sku" in cols)
        and ("material description" in cols or "description" in cols)
        and ("storage location" in cols)
        and any("bum" in c or "quantity" in c for c in cols)
    )

def looks_like_saad(df: pd.DataFrame) -> bool:
    cols = {c.lower() for c in df.columns}
    # columnas clave en SAAD
    return ("ubprod" in cols or "sku" in cols) and ("ubcstk" in cols or "qty" in cols or "cantidad" in cols)

def guess_saad_role(df: pd.DataFrame) -> str:
    """
    Intenta adivinar si SAAD es CBP o BUNKER mirando posibles columnas de 'almacen'.
    Si encuentra valores típicos -> 'bunker' si AER/OLR; 'cbp' si COL.
    Si no puede, devuelve 'unknown'.
    """
    # buscar columna tipo almacen
    cand = None
    for c in df.columns:
        cl = c.lower()
        if cl in ("almacen", "storage", "alm", "ubalm", "ubcia", "emplaza"):
            cand = c
            break
    if cand is None:
        # nada para inferir
        return "unknown"

    svals = {str(v).strip().upper() for v in df[cand].dropna().unique()[:300]}
    if any(v in svals for v in ("AER", "OLR", "RT", "RP", "BN", "MOV")):
        return "bunker"
    if "COL" in svals or "DR" in svals:
        return "cbp"
    return "unknown"


# ==============================
# Normalización SAP / SAAD (CBP/BUNKER)
# ==============================

def normalize_sap(
    df: pd.DataFrame,
    storages_filter: Optional[List[str]] = None
) -> pd.DataFrame:
    # mapeo de columnas SAP
    cols = {c.lower(): c for c in df.columns}

    code_col = cols.get("material") or cols.get("sku")
    desc_col = cols.get("material description") or cols.get("description")
    stor_col = cols.get("storage location")
    qty_col  = None
    # buscar BUM Quantity o Quantity
    for k in cols:
        if "bum" in k and "quantity" in k:
            qty_col = cols[k]
            break
    if qty_col is None:
        for k in cols:
            if "quantity" in k:
                qty_col = cols[k]
                break

    if not all([code_col, desc_col, stor_col, qty_col]):
        raise ValueError("No se pudieron mapear columnas SAP (Material, Material Description, Storage Location, BUM Quantity)")

    out = df[[code_col, desc_col, stor_col, qty_col]].copy()
    out.columns = ["codigo", "descripcion", "storage", "qty"]
    out["codigo"] = out["codigo"].map(norm_code)
    out["descripcion"] = out["descripcion"].map(norm_text)
    out["storage"] = out["storage"].map(norm_text).str.upper()
    out["qty"] = out["qty"].map(parse_qty)

    if storages_filter:
        filt = {s.strip().upper() for s in storages_filter if s.strip()}
        out = out[out["storage"].isin(filt)]

    # agrupamos por código (y opcionalmente por storage si querés auditar)
    out = out.groupby(["codigo"], as_index=False).agg({
        "descripcion": "first",
        "qty": "sum"
    })
    out.rename(columns={"qty": "sap_qty"}, inplace=True)
    return out


def normalize_saad(
    df: pd.DataFrame
) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}

    # columnas básicas
    code_col = cols.get("ubprod") or cols.get("sku")
    desc_col = cols.get("itdesc") or cols.get("descripcion") or cols.get("desc") or cols.get("description")
    estado_col = cols.get("ubiest")  # UR/QI/BL
    qty_col = cols.get("ubcstk") or cols.get("qty") or cols.get("cantidad")

    if not all([code_col, desc_col, qty_col]):
        raise ValueError("No se pudieron mapear columnas SAAD (ubprod/sku, itdesc/descripcion, ubcstk/qty).")

    out = df[[code_col, desc_col, qty_col]].copy()
    out.columns = ["codigo", "descripcion", "qty"]
    out["codigo"] = out["codigo"].map(norm_code)
    out["descripcion"] = out["descripcion"].map(norm_text)
    out["qty"] = out["qty"].map(parse_qty)

    if estado_col is not None:
        out["ubiest"] = df[estado_col].map(norm_text).str.upper()
    else:
        out["ubiest"] = ""  # por si el archivo no lo trae

    return out


# ======================
# Comparación y reporte
# ======================

def compare_saad_vs_sap(
    saad_df: pd.DataFrame,
    sap_df: pd.DataFrame,
    include_ubiest: bool
) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas:
    codigo, descripcion, [ubiest], SAAD, SAP, DIFERENCIA
    """
    if include_ubiest:
        g_saad = (saad_df
                  .groupby(["codigo", "descripcion", "ubiest"], as_index=False)
                  .agg({"qty": "sum"}))
    else:
        g_saad = (saad_df
                  .groupby(["codigo", "descripcion"], as_index=False)
                  .agg({"qty": "sum"}))
        g_saad["ubiest"] = ""

    # Merge con SAP
    merged = pd.merge(
        g_saad,
        sap_df[["codigo", "sap_qty"]],
        on="codigo",
        how="left"
    )
    merged["sap_qty"] = merged["sap_qty"].fillna(0).map(int)
    merged.rename(columns={"qty": "saad_qty"}, inplace=True)
    merged["DIFERENCIA"] = merged["saad_qty"] - merged["sap_qty"]

    # Orden por |dif| desc
    merged["_abs"] = merged["DIFERENCIA"].abs()
    merged.sort_values(["_abs", "codigo"], ascending=[False, True], inplace=True)
    merged.drop(columns=["_abs"], inplace=True)

    # Selección de columnas
    cols = ["codigo", "descripcion"]
    if include_ubiest:
        cols.append("ubiest")
    cols += ["saad_qty", "sap_qty", "DIFERENCIA"]
    merged = merged[cols]
    # Renombrar etiquetas visibles
    merged.rename(columns={
        "saad_qty": "SAAD",
        "sap_qty": "SAP COLGATE",
        "ubiest": "estado"
    }, inplace=True)
    return merged


def build_excel(
    cbp_df: pd.DataFrame,
    bunker_df: pd.DataFrame,
    sap_cbp_df: pd.DataFrame,
    sap_bunker_df: pd.DataFrame,
    split_bunker_by_estado: bool
) -> bytes:
    # Reportes
    cbp_out = compare_saad_vs_sap(cbp_df, sap_cbp_df, include_ubiest=True)

    bunker_total = compare_saad_vs_sap(bunker_df, sap_bunker_df, include_ubiest=False)

    # Opcional por estado UR/QI/BL
    bunker_ur = bunker_qi = bunker_bl = None
    if "ubiest" in bunker_df.columns and split_bunker_by_estado:
        def filt(e):
            return bunker_df[bunker_df["ubiest"] == e]
        bunker_ur = compare_saad_vs_sap(filt("UR"), sap_bunker_df, include_ubiest=False)
        bunker_qi = compare_saad_vs_sap(filt("QI"), sap_bunker_df, include_ubiest=False)
        bunker_bl = compare_saad_vs_sap(filt("BL"), sap_bunker_df, include_ubiest=False)

    # Escribir a XLSX en memoria
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        cbp_out.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
        if split_bunker_by_estado:
            (bunker_ur or pd.DataFrame(columns=["codigo","descripcion","SAAD","SAP COLGATE","DIFERENCIA"])
             ).to_excel(writer, index=False, sheet_name="BUNKER_UR_vs_SAP")
            (bunker_qi or pd.DataFrame(columns=["codigo","descripcion","SAAD","SAP COLGATE","DIFERENCIA"])
             ).to_excel(writer, index=False, sheet_name="BUNKER_QI_vs_SAP")
            (bunker_bl or pd.DataFrame(columns=["codigo","descripcion","SAAD","SAP COLGATE","DIFERENCIA"])
             ).to_excel(writer, index=False, sheet_name="BUNKER_BL_vs_SAP")
        else:
            bunker_total.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
    out.seek(0)
    return out.getvalue()


# ======================
# Endpoint principal
# ======================

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.post(
    "/cruce-auto-xlsx",
    responses={200: {"content": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}}}},
)
async def cruce_auto_xlsx(
    files: List[UploadFile] = File(..., description="Subí TRES archivos: SAP, SAAD_CBP, SAAD_BUNKER (en ese orden o usa 'roles')."),
    sap_cbp_storages: Optional[str] = Query(None, description="Storages de SAP que representan CBP, e.g. 'COL'. Coma-separado."),
    sap_bunker_storages: Optional[str] = Query(None, description="Storages de SAP que representan BUNKER, e.g. 'AER,OLR'. Coma-separado."),
    split_by_estado: bool = Query(False, description="Si True, BUNKER se separa por UR/QI/BL."),
    roles: Optional[str] = Query(None, description="Orden explícito: 'sap,cbp,bunker' (o el que uses).")
):
    if len(files) < 3:
        raise HTTPException(400, "Subí TRES archivos: SAP, SAAD_CBP y SAAD_BUNKER.")

    # 1) Leer todos
    try:
        dfs = [read_any_table(f) for f in files]
    except Exception as e:
        raise HTTPException(400, f"Error leyendo archivos: {e}")

    # 2) Identificar roles
    order: List[int] = [None, None, None]  # indices: [sap_idx, cbp_idx, bunker_idx]

    if roles:
        # roles explícitos, ej 'sap,cbp,bunker'
        parts = [p.strip().lower() for p in roles.split(",")]
        if set(parts) != {"sap", "cbp", "bunker"} or len(parts) != 3:
            raise HTTPException(400, "roles debe ser exactamente 'sap,cbp,bunker' (en cualquier orden).")
        for i, role in enumerate(parts):
            if role == "sap":
                order[0] = i
            elif role == "cbp":
                order[1] = i
            else:
                order[2] = i
        # Remapeamos dfs según ese orden
        try:
            dfs = [dfs[order[0]], dfs[order[1]], dfs[order[2]]]
            files = [files[order[0]], files[order[1]], files[order[2]]]
        except Exception:
            raise HTTPException(400, "No coinciden 'roles' con la cantidad/orden de archivos subidos.")
    else:
        # detección automática
        sap_idx = cbp_idx = bunker_idx = None

        for i, df in enumerate(dfs):
            if looks_like_sap(df):
                sap_idx = i
            elif looks_like_saad(df):
                role_guess = guess_saad_role(df)
                if role_guess == "cbp" and cbp_idx is None:
                    cbp_idx = i
                elif role_guess == "bunker" and bunker_idx is None:
                    bunker_idx = i

        if None in (sap_idx, cbp_idx, bunker_idx):
            raise HTTPException(400, "No pude identificar los tres archivos (SAP, SAAD CBP y SAAD BUNKER). Revisá los encabezados.")

        dfs = [dfs[sap_idx], dfs[cbp_idx], dfs[bunker_idx]]
        files = [files[sap_idx], files[cbp_idx], files[bunker_idx]]

    # 3) Normalizar
    try:
        sap_df = normalize_sap(
            dfs[0],
            storages_filter=[s.strip().upper() for s in (sap_cbp_storages or "").split(",") if s.strip()] +
                            [s.strip().upper() for s in (sap_bunker_storages or "").split(",") if s.strip()]
        )
        # Para comparar CBP vs SAP => filtramos SAP con sap_cbp_storages
        sap_cbp_df = normalize_sap(
            dfs[0],
            storages_filter=[s.strip().upper() for s in (sap_cbp_storages or "").split(",") if s.strip()] or None
        )
        # Para comparar BUNKER vs SAP => filtramos SAP con sap_bunker_storages
        sap_bunker_df = normalize_sap(
            dfs[0],
            storages_filter=[s.strip().upper() for s in (sap_bunker_storages or "").split(",") if s.strip()] or None
        )

        cbp_df = normalize_saad(dfs[1])
        bunker_df = normalize_saad(dfs[2])

    except Exception as e:
        raise HTTPException(400, f"Error normalizando archivos: {e}")

    # 4) Generar Excel
    try:
        content = build_excel(
            cbp_df=cbp_df,
            bunker_df=bunker_df,
            sap_cbp_df=sap_cbp_df,
            sap_bunker_df=sap_bunker_df,
            split_bunker_by_estado=split_by_estado
        )
    except Exception as e:
        raise HTTPException(500, f"Error al procesar: {e}")

    filename = "cruce_ordenado.xlsx"
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
