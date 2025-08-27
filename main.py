import io
import re
from typing import List, Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Query
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="Cruce Inventario API")


# ------------------------ Utilidades ------------------------

def _norm(s: str) -> str:
    """
    Normaliza un nombre de columna o texto:
    - pasa a minúscula
    - quita tildes/símbolos comunes
    - quita espacios repetidos
    """
    if s is None:
        return ""
    s = str(s).strip().lower()
    rep = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "ñ": "n", "\n": " ", "\r": " ", "\t": " ", "  ": " "
    }
    for a, b in rep.items():
        s = s.replace(a, b)
    s = re.sub(r"[^a-z0-9%/ _\-]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s


def _first_match(colnames: List[str], candidates: List[str]) -> Optional[str]:
    """
    Devuelve la primera columna en 'colnames' que coincida con
    alguno de los alias de 'candidates' (normalizados).
    """
    cols = { _norm(c): c for c in colnames }
    for cand in candidates:
        n = _norm(cand)
        for nc, original in cols.items():
            if n in nc or nc in n:
                return original
    return None


def _read_xlsx(upload: UploadFile) -> pd.DataFrame:
    """
    Lee .xlsx a cadena -> DataFrame con dtypes string para no perder miles/decimales.
    Recomendación: trabajar siempre con .xlsx (no .xls).
    """
    data = upload.file.read()
    upload.file.seek(0)
    df = pd.read_excel(io.BytesIO(data), engine="openpyxl", dtype=str)
    # Si el archivo tiene varias hojas, nos quedamos con la primera con más columnas “útiles”.
    if isinstance(df, dict):
        # Por si fuese excel con múltiples sheets (cuando read_excel devuelve dict)
        best = None
        best_cols = 0
        for _, dfi in df.items():
            if isinstance(dfi, pd.DataFrame) and dfi.shape[1] > best_cols:
                best = dfi
                best_cols = dfi.shape[1]
        df = best if best is not None else pd.DataFrame()
    return df


def _to_number(x) -> float:
    """
    Convierte textos como '6.632,94' o '8,055' a float.
    Si no se puede, devuelve 0.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return 0.0
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return 0.0
    # quita espacios
    s = s.replace(" ", "")
    # si tiene ambos separadores, asumimos . miles y , decimales
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # si sólo hay coma, probablemente usa coma como decimal
        if "," in s and "." not in s:
            s = s.replace(",", ".")
        # si sólo hay punto, lo tomamos como decimal
        # si no hay separador, queda tal cual
    try:
        return float(s)
    except Exception:
        # último intento: saca todo lo que no sea dígito/ .
        s2 = re.sub(r"[^0-9.]", "", s)
        try:
            return float(s2) if s2 else 0.0
        except Exception:
            return 0.0


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nombres de columnas (sin tocar sus valores)."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


# -------------------- Parsers por tipo de archivo --------------------

def _parse_sap(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    SAP esperado con columnas (nombres posibles):
      - Material -> código
      - Material Description -> descripción
      - Storage Location -> storage (AER, OLR, COL, …)
      - Status -> ubiest (UR, QI, BL)
      - BUM Quantity -> qty
    """
    df = _clean_df(df_raw)

    col_code = _first_match(
        list(df.columns),
        ["material", "codigo", "cod", "sku"]
    )
    col_desc = _first_match(
        list(df.columns),
        ["material description", "descripcion", "desc", "product description"]
    )
    col_storage = _first_match(
        list(df.columns),
        ["storage location", "storage", "almacen", "ubicacion", "sloc"]
    )
    col_status = _first_match(
        list(df.columns),
        ["status", "estado", "ubiest", "lote status"]
    )
    col_qty = _first_match(
        list(df.columns),
        ["bum quantity", "quantity", "qty", "cantidad", "stock", "inventario"]
    )

    missing = [n for n, v in [
        ("Material", col_code),
        ("Material Description", col_desc),
        ("Storage Location", col_storage),
        ("Status", col_status),
        ("BUM Quantity / Qty", col_qty),
    ] if v is None]

    if missing:
        raise ValueError(f"SAP: no pude encontrar columnas {missing}. Encabezados: {list(df.columns)}")

    out = df[[col_code, col_desc, col_storage, col_status, col_qty]].copy()
    out.columns = ["codigo", "descripcion", "storage", "ubiest", "qty"]
    out["codigo"] = out["codigo"].astype(str).str.strip()
    out["descripcion"] = out["descripcion"].astype(str).str.strip()
    out["storage"] = out["storage"].astype(str).str.strip().str.upper()
    out["ubiest"] = out["ubiest"].astype(str).str.strip().str.upper()
    out["qty"] = out["qty"].map(_to_number).fillna(0.0)

    return out


def _parse_saad(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    SAAD (CBP o BUNKER) esperado con columnas (nombres posibles):
      - ubprod -> código
      - itdesc -> descripción
      - ubiest -> estado (UR/QI/BL)
      - ubcstk -> cantidad
      - (opcional) almacen / emplaza / storage -> storage (si viene en el archivo)
    """
    df = _clean_df(df_raw)

    col_code = _first_match(list(df.columns), ["ubprod", "codigo", "cod", "sku"])
    col_desc = _first_match(list(df.columns), ["itdesc", "descripcion", "desc"])
    col_ubiest = _first_match(list(df.columns), ["ubiest", "estado", "status"])
    col_qty = _first_match(list(df.columns), ["ubcstk", "cantidad", "stock", "qty", "inventario"])
    col_storage = _first_match(list(df.columns), ["storage", "almacen", "emplaza", "s_loc", "ubicacion"])

    missing = [n for n, v in [
        ("ubprod", col_code),
        ("itdesc", col_desc),
        ("ubiest", col_ubiest),
        ("ubcstk", col_qty),
    ] if v is None]

    if missing:
        raise ValueError(f"SAAD: no pude encontrar columnas {missing}. Encabezados: {list(df.columns)}")

    use_cols = [col_code, col_desc, col_ubiest, col_qty]
    if col_storage is not None:
        use_cols.append(col_storage)

    out = df[use_cols].copy()
    new_cols = ["codigo", "descripcion", "ubiest", "qty"] + (["storage"] if col_storage is not None else [])
    out.columns = new_cols

    out["codigo"] = out["codigo"].astype(str).str.strip()
    out["descripcion"] = out["descripcion"].astype(str).str.strip()
    out["ubiest"] = out["ubiest"].astype(str).str.strip().str.upper()
    out["qty"] = out["qty"].map(_to_number).fillna(0.0)

    if "storage" in out.columns:
        out["storage"] = out["storage"].astype(str).str.strip().str.upper()
    else:
        out["storage"] = ""  # lo completamos luego desde SAP

    return out


def _identify_roles(dfs: List[pd.DataFrame], roles: Optional[str]) -> dict:
    """
    Intenta identificar cuál DF es SAP / CBP / BUNKER. Si 'roles'
    llega como 'sap,cbp,bunker', respeta ese orden contra files[].
    """
    if roles:
        tokens = [t.strip().lower() for t in roles.split(",")]
        if len(tokens) != 3 or set(tokens) != {"sap", "cbp", "bunker"}:
            raise ValueError("Parametro 'roles' debe ser 'sap,cbp,bunker' en algún orden.")
        mapping = {}
        for df, role in zip(dfs, tokens):
            mapping[role] = df
        return mapping

    # Autodetección básica
    marks = {}
    for i, df in enumerate(dfs):
        cols = set(_norm(c) for c in df.columns)
        score_sap = int(any("material" in c for c in cols)) + int(any("storage" in c for c in cols)) + int(any("status" in c for c in cols))
        score_saad = int(any("ubprod" in c for c in cols)) + int(any("ubiest" in c for c in cols)) + int(any("ubcstk" in c for c in cols))
        marks[i] = ("sap" if score_sap >= score_saad else "saad", score_sap, score_saad)

    # El que tenga más "score_sap" será SAP.
    sap_idx = max(marks.keys(), key=lambda i: marks[i][1])
    sap_df = dfs[sap_idx]

    # Los otros dos son SAAD. No siempre puedo distinguir CBP/BUNKER; los devuelvo sin etiqueta
    rest = [dfs[i] for i in range(len(dfs)) if i != sap_idx]
    return {"sap": sap_df, "saad1": rest[0], "saad2": rest[1]}


def _pivot_sum(df: pd.DataFrame, by_cols: List[str]) -> pd.DataFrame:
    g = df.groupby(by_cols, dropna=False, as_index=False)["qty"].sum()
    g["qty"] = g["qty"].fillna(0.0)
    return g


def _merge_cruce(left: pd.DataFrame, right: pd.DataFrame, on: List[str], lcol: str, rcol: str) -> pd.DataFrame:
    out = pd.merge(left, right, on=on, how="outer", suffixes=("_l", "_r"))
    out[lcol] = out["qty_l"].fillna(0.0)
    out[rcol] = out["qty_r"].fillna(0.0)
    out = out.drop(columns=["qty_l", "qty_r"])
    out["DIFERENCIA"] = out[lcol] - out[rcol]
    return out


# -------------------------- Endpoint principal --------------------------

@app.post("/cruce-auto-xlsx")
def cruce_auto_xlsx(
    files: List[UploadFile] = File(description="SUBÍ **tres** archivos: SAP.xlsx, SAAD_CBP.xlsx, SAAD_BUNKER.xlsx (en ese orden o usa 'roles')."),
    sap_cbp_storages: str = Query("", description="Storages de SAP que representan CBP (p.ej. 'COL'). Si vacío, toma todo."),
    sap_bunker_storages: str = Query("", description="Storages de SAP que representan BUNKER (p.ej. 'AER,OLR'). Si vacío, toma todo."),
    split_by_estado: bool = Query(False, description="Si True, cruza por estado UR/QI/BL (ubiest)."),
    roles: Optional[str] = Query(None, description="Orden explícito de archivos: 'sap,cbp,bunker' (o el orden que usaste).")
):
    try:
        if len(files) != 3:
            return JSONResponse(status_code=400, content={"detail": "Debes subir **tres** archivos: SAP, SAAD CBP y SAAD BUNKER."})

        # Lee los tres archivos como .xlsx
        dfs = []
        for f in files:
            try:
                dfi = _read_xlsx(f)
                dfi.columns = [str(c) for c in dfi.columns]
            except Exception as e:
                return JSONResponse(status_code=400, content={"detail": f"No pude leer '{f.filename}': {str(e)}"})
            dfs.append(dfi)

        mapping = _identify_roles(dfs, roles)

        # Resolver roles de SAAD (si no fue explícito)
        if "cbp" in mapping and "bunker" in mapping:
            saad_cbp_raw = mapping["cbp"]
            saad_bunker_raw = mapping["bunker"]
        else:
            # Los dos que no son SAP los tratamos como SAAD; luego decidimos a qué lado pertenece por storage/solapamiento
            saad1_raw = mapping["saad1"]
            saad2_raw = mapping["saad2"]
            # Parseo para ver qué columnas tienen
            try:
                t1 = _parse_saad(saad1_raw)
                t2 = _parse_saad(saad2_raw)
            except Exception as e:
                return JSONResponse(status_code=400, content={"detail": f"Error parseando SAAD: {str(e)}"})
            # Por defecto tomo el primero como CBP y el segundo como BUNKER (si no hay 'roles')
            saad_cbp_raw, saad_bunker_raw = t1, t2

        # Parseos finales (si los dos estaban etiquetados ya, caen aquí igual)
        try:
            sap = _parse_sap(mapping["sap"])
        except Exception as e:
            return JSONResponse(status_code=400, content={"detail": f"Error parseando SAP: {str(e)}"})

        # Si ya viene parseado (del bloque anterior), no lo vuelvas a parsear
        if isinstance(saad_cbp_raw, pd.DataFrame) and set(saad_cbp_raw.columns) >= {"codigo", "descripcion", "ubiest", "qty"}:
            saad_cbp = saad_cbp_raw.copy()
        else:
            saad_cbp = _parse_saad(saad_cbp_raw)

        if isinstance(saad_bunker_raw, pd.DataFrame) and set(saad_bunker_raw.columns) >= {"codigo", "descripcion", "ubiest", "qty"}:
            saad_bunker = saad_bunker_raw.copy()
        else:
            saad_bunker = _parse_saad(saad_bunker_raw)

        # FILTROS por storages de SAP (si indicás cuáles corresponden a CBP/BUNKER)
        stor_cbp = [s.strip().upper() for s in sap_cbp_storages.split(",") if s.strip()]
        stor_bunker = [s.strip().upper() for s in sap_bunker_storages.split(",") if s.strip()]

        sap_cbp = sap.copy()
        sap_bunker = sap.copy()
        if stor_cbp:
            sap_cbp = sap_cbp[sap_cbp["storage"].isin(stor_cbp)]
        if stor_bunker:
            sap_bunker = sap_bunker[sap_bunker["storage"].isin(stor_bunker)]

        # Agrupación por estado o no
        group_cols_c = ["codigo", "ubiest"] if split_by_estado else ["codigo"]
        group_cols_b = ["codigo", "ubiest"] if split_by_estado else ["codigo"]

        sap_cbp_g = _pivot_sum(sap_cbp, group_cols_c)
        sap_bunker_g = _pivot_sum(sap_bunker, group_cols_b)

        saad_cbp_g = _pivot_sum(saad_cbp, group_cols_c)
        saad_bunker_g = _pivot_sum(saad_bunker, group_cols_b)

        # Mergeo (por codigo y eventualmente ubiest)
        cbp_merge = _merge_cruce(
            left=saad_cbp_g,
            right=sap_cbp_g,
            on=group_cols_c,
            lcol="SAAD_CBP",
            rcol="SAP_COLGATE"
        )
        bunker_merge = _merge_cruce(
            left=saad_bunker_g,
            right=sap_bunker_g,
            on=group_cols_b,
            lcol="SAAD_BUNKER",
            rcol="SAP_COLGATE"
        )

        # Agrego descripción (la tomo primero de SAAD; si no, de SAP)
        # CBP
        desc_saad_cbp = saad_cbp[["codigo", "descripcion"]].drop_duplicates()
        desc_sap = sap[["codigo", "descripcion"]].drop_duplicates()
        cbp_merge = cbp_merge.merge(desc_saad_cbp, on="codigo", how="left")
        cbp_merge = cbp_merge.merge(desc_sap, on="codigo", how="left", suffixes=("", "_sap"))
        cbp_merge["descripcion"] = cbp_merge["descripcion"].fillna(cbp_merge["descripcion_sap"])
        cbp_merge = cbp_merge.drop(columns=[c for c in ["descripcion_sap"] if c in cbp_merge.columns])
        # BUNKER
        desc_saad_bunker = saad_bunker[["codigo", "descripcion"]].drop_duplicates()
        bunker_merge = bunker_merge.merge(desc_saad_bunker, on="codigo", how="left")
        bunker_merge = bunker_merge.merge(desc_sap, on="codigo", how="left", suffixes=("", "_sap"))
        bunker_merge["descripcion"] = bunker_merge["descripcion"].fillna(bunker_merge["descripcion_sap"])
        bunker_merge = bunker_merge.drop(columns=[c for c in ["descripcion_sap"] if c in bunker_merge.columns])

        # Ordeno columnas y orden de filas
        if split_by_estado:
            cbp_merge = cbp_merge[["codigo", "descripcion", "ubiest", "SAAD_CBP", "SAP_COLGATE", "DIFERENCIA"]]
            bunker_merge = bunker_merge[["codigo", "descripcion", "ubiest", "SAAD_BUNKER", "SAP_COLGATE", "DIFERENCIA"]]
        else:
            cbp_merge = cbp_merge[["codigo", "descripcion", "SAAD_CBP", "SAP_COLGATE", "DIFERENCIA"]]
            bunker_merge = bunker_merge[["codigo", "descripcion", "SAAD_BUNKER", "SAP_COLGATE", "DIFERENCIA"]]

        cbp_merge = cbp_merge.sort_values(by="DIFERENCIA", key=lambda s: s.abs(), ascending=False)
        bunker_merge = bunker_merge.sort_values(by="DIFERENCIA", key=lambda s: s.abs(), ascending=False)

        # Resumen
        resumen = {
            "archivos": [f.filename for f in files],
            "parametros": {
                "sap_cbp_storages": stor_cbp,
                "sap_bunker_storages": stor_bunker,
                "split_by_estado": split_by_estado,
                "roles": roles or "auto"
            },
            "totales": {
                "CBP_SAAD": float(saad_cbp_g["qty"].sum()),
                "CBP_SAP": float(sap_cbp_g["qty"].sum()),
                "BUNKER_SAAD": float(saad_bunker_g["qty"].sum()),
                "BUNKER_SAP": float(sap_bunker_g["qty"].sum())
            }
        }
        df_resumen = pd.json_normalize(resumen, sep="_")

        # Exporto a Excel
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
            cbp_merge.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
            bunker_merge.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
            df_resumen.to_excel(writer, index=False, sheet_name="Resumen")

            # Formatos simples
            for sh in ["CBP_vs_SAP", "BUNKER_vs_SAP"]:
                ws = writer.sheets[sh]
                ws.autofilter(0, 0, 0, cbp_merge.shape[1]-1)
                ws.set_column(0, 1, 22)  # codigo/descripcion
                ws.set_column(2, 5, 14)  # números

        bio.seek(0)
        filename = "cruce_ordenado.xlsx"
        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Error general: {str(e)}"})
