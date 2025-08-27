from __future__ import annotations

import io
import re
import unicodedata
from pathlib import Path
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import StreamingResponse

# ========================
# FastAPI app
# ========================

app = FastAPI(
    title="Cruce Inventario API",
    version="1.0",
    description="Cruce SAP vs SAAD (CBP/BUNKER) y exportación a XLSX.",
)


@app.get("/healthz")
def healthz():
    return {"ok": True}


# ========================
# Utilidades
# ========================

def _slug(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _num_to_int(x) -> int:
    """
    Convierte strings como '1.545,000' o '20 412' o '9,456' a int (solo dígitos y signo).
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0
    s = str(x)
    s = re.sub(r"[^\d\-]", "", s)  # deja solo 0-9 y -
    if s in ("", "-"):
        return 0
    try:
        return int(s)
    except Exception:
        return 0


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Busca en df la primera columna que "matchee" con alguno de los nombres candidatos.
    """
    cols = {col: _slug(col) for col in df.columns}
    for want in candidates:
        want_slug = _slug(want)
        for col, slug in cols.items():
            if want_slug == slug:
                return col
    # flexible: contiene
    for want in candidates:
        want_slug = _slug(want)
        for col, slug in cols.items():
            if want_slug in slug:
                return col
    return None


# ========================
# Lectura de archivos
# ========================

async def _read_excel(up: UploadFile) -> pd.DataFrame:
    try:
        raw = await up.read()
        ext = Path(up.filename or "").suffix.lower()
        engine = None
        if ext == ".xls":
            engine = "xlrd"  # requiere xlrd>=2.0.1
        df = pd.read_excel(io.BytesIO(raw), dtype=str, engine=engine)
        df = df.fillna("")
        return df
    except ImportError as e:
        if "xlrd" in str(e).lower():
            raise HTTPException(
                400,
                "Error leyendo archivos: Pandas requiere versión '2.0.1' o mayor de 'xlrd' para .XLS. "
                "Actualizá xlrd>=2.0.1 en requirements.txt.",
            )
        raise
    except Exception as e:
        raise HTTPException(400, f"Error leyendo {up.filename}: {e}")


# ========================
# Heurísticas de identificación
# ========================

def _is_sap(df: pd.DataFrame) -> bool:
    col_mat = _find_col(df, ["Material", "Codigo", "Cod"])
    col_desc = _find_col(df, ["Material Description", "Descripcion", "Description"])
    col_sloc = _find_col(df, ["Storage Location", "Storage", "Sloc"])
    col_qty = _find_col(df, ["BUM Quantity", "Quantity", "Qty", "Cajas", "Stock"])
    return all([col_mat, col_desc, col_sloc, col_qty])


def _is_saad(df: pd.DataFrame) -> bool:
    col_code = _find_col(df, ["ubprod", "codigo", "sku", "material"])
    col_desc = _find_col(df, ["itdesc", "descripcion", "desc"])
    col_st = _find_col(df, ["ubiest", "storage", "almacen", "storage location"])
    col_qty = _find_col(df, ["ubcstk", "cajas", "qty", "cantidad", "stock"])
    return all([col_code, col_desc, col_st, col_qty])


BUNKER_HINT_STORAGES = {"AER", "OLR", "BN", "RT", "RP", "MOV", "BKR", "BUK", "BLO", "BUNKER"}


def _looks_bunker(df: pd.DataFrame) -> bool:
    col_st = _find_col(df, ["ubiest", "storage", "almacen", "storage location"])
    if not col_st:
        return False
    vals = {str(v).upper().strip() for v in df[col_st].unique()}
    return any(any(h in v for h in BUNKER_HINT_STORAGES) for v in vals)


# ========================
# Parseadores
# ========================

def _parse_sap(df: pd.DataFrame) -> pd.DataFrame:
    c_code = _find_col(df, ["Material", "Codigo", "Cod"])
    c_desc = _find_col(df, ["Material Description", "Descripcion", "Description"])
    c_sloc = _find_col(df, ["Storage Location", "Storage", "Sloc"])
    c_qty = _find_col(df, ["BUM Quantity", "Quantity", "Qty", "Cajas", "Stock"])
    if not all([c_code, c_desc, c_sloc, c_qty]):
        raise ValueError("No encontré columnas SAP (Material / Material Description / Storage Location / Quantity).")

    out = pd.DataFrame({
        "codigo": df[c_code].astype(str).str.strip(),
        "descripcion": df[c_desc].astype(str).str.strip(),
        "ubiest": df[c_sloc].astype(str).str.upper().str.strip(),
        "cajas": df[c_qty].apply(_num_to_int),
    })
    out = out.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
    return out


def _parse_saad_cbp(df: pd.DataFrame) -> pd.DataFrame:
    c_code = _find_col(df, ["ubprod", "codigo", "sku", "material"])
    c_desc = _find_col(df, ["itdesc", "descripcion", "desc"])
    c_st = _find_col(df, ["ubiest", "storage", "almacen", "storage location"])
    c_qty = _find_col(df, ["ubcstk", "cajas", "qty", "cantidad", "stock"])
    if not all([c_code, c_desc, c_st, c_qty]):
        raise ValueError("No encontré columnas SAAD CBP (ubprod / itdesc / ubiest / ubcstk).")

    out = pd.DataFrame({
        "codigo": df[c_code].astype(str).str.strip(),
        "descripcion": df[c_desc].astype(str).str.strip(),
        "ubiest": df[c_st].astype(str).str.upper().str.strip(),
        "cajas": df[c_qty].apply(_num_to_int),
    })
    out = out.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
    return out


def _extraer_estado_de_row(row: pd.Series) -> str:
    # Busca UR / QI / BL en varias columnas típicas
    tokens = []
    for key in ["estado", "ubesta", "nitesta", "mensaje", "ubesta ", "estatus", "status"]:
        if key in row.index:
            tokens.append(str(row[key]).upper())
    joined = " ".join(tokens)
    m = re.search(r"\b(UR|QI|BL)\b", joined)
    return m.group(1) if m else ""


def _parse_saad_bunker(df: pd.DataFrame, split_by_estado: bool = True) -> pd.DataFrame:
    c_code = _find_col(df, ["ubprod", "codigo", "sku", "material"])
    c_desc = _find_col(df, ["itdesc", "descripcion", "desc"])
    c_st = _find_col(df, ["ubiest", "storage", "almacen", "storage location"])
    c_qty = _find_col(df, ["ubcstk", "cajas", "qty", "cantidad", "stock"])
    if not all([c_code, c_desc, c_st, c_qty]):
        raise ValueError("No encontré columnas SAAD BUNKER (ubprod / itdesc / ubiest / ubcstk).")

    base = pd.DataFrame({
        "codigo": df[c_code].astype(str).str.strip(),
        "descripcion": df[c_desc].astype(str).str.strip(),
        "ubiest": df[c_st].astype(str).str.upper().str.strip(),
        "cajas": df[c_qty].apply(_num_to_int),
    })

    if split_by_estado:
        # intentar derivar estado
        if "estado" not in df.columns:
            df = df.copy()
            df["estado"] = df.apply(_extraer_estado_de_row, axis=1)

        base["estado"] = df["estado"].astype(str).str.upper().str.strip()
        base.loc[~base["estado"].isin(["UR", "QI", "BL"]), "estado"] = ""
    else:
        base["estado"] = ""

    return base


def _cruzar(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    on_cols: List[str],
    left_name: str,
    right_name: str,
) -> pd.DataFrame:
    left = df_left.copy()
    right = df_right.copy()
    for c in ["cajas"]:
        if c in left.columns:
            left[c] = left[c].apply(_num_to_int)
        if c in right.columns:
            right[c] = right[c].apply(_num_to_int)

    m = pd.merge(left, right, on=on_cols, how="outer", suffixes=("_left", "_right"))
    m[left_name] = m.pop("cajas_left").fillna(0).astype(int)
    m[right_name] = m.pop("cajas_right").fillna(0).astype(int)
    m["DIFERENCIA"] = (m[left_name] - m[right_name]).astype(int)
    cols = on_cols + [left_name, right_name, "DIFERENCIA"]
    return m[cols]


def _estado_key(x: str) -> int:
    order = {"UR": 0, "QI": 1, "BL": 2, "": 3}
    return order.get(x, 99)


# ========================
# Endpoint principal
# ========================

@app.post("/cruce-auto-xlsx")
async def cruce_auto_xlsx(
    files: List[UploadFile] = File(..., description="Subí TRES archivos: SAP, SAAD_CBP y SAAD_BUNKER"),
    sap_cbp_storages: str = Query("", description="Storages CBP/SAP (coma-separado). Si vacío: automático."),
    sap_bunker_storages: str = Query("", description="Storages BUNKER/SAP (coma-separado). Si vacío: automático."),
    split_by_estado: bool = Query(True, description="Si True, BUNKER se separa por UR/QI/BL."),
    roles: str = Query("", description="Orden explícito de archivos: 'sap,cbp,bunker' (o el orden que uses)."),
):
    try:
        if len(files) != 3:
            raise HTTPException(400, "Subí exactamente 3 archivos: SAP, SAAD CBP y SAAD BUNKER (en cualquier orden).")

        # Leer todos
        triples = []
        for f in files:
            df = await _read_excel(f)
            triples.append((f.filename, df))

        # Mapeo por roles si viene
        sap_df = cbp_df = bunker_df = None
        rl = [r.strip().lower() for r in roles.split(",") if r.strip()]
        if len(rl) == 3:
            for (name, df), role in zip(triples, rl):
                if role == "sap":
                    sap_df = df
                elif role in ("cbp", "saad_cbp", "saad-cbp"):
                    cbp_df = df
                elif role in ("bunker", "saad_bunker", "saad-bunker"):
                    bunker_df = df
                else:
                    raise HTTPException(400, f"Rol '{role}' inválido. Usá sap / cbp / bunker.")
        else:
            # Detección por columnas
            for name, df in triples:
                if _is_sap(df) and sap_df is None:
                    sap_df = df
                elif _is_saad(df) and _looks_bunker(df) and bunker_df is None:
                    bunker_df = df
                elif _is_saad(df) and cbp_df is None:
                    cbp_df = df

            # Fallback por nombre de archivo
            if sap_df is None or cbp_df is None or bunker_df is None:
                for name, df in triples:
                    low = (name or "").lower()
                    if sap_df is None and "sap" in low:
                        sap_df = df
                    elif cbp_df is None and "cbp" in low:
                        cbp_df = df
                    elif bunker_df is None and ("bunker" in low or "bkr" in low):
                        bunker_df = df

        if sap_df is None or cbp_df is None or bunker_df is None:
            raise HTTPException(400, "No pude identificar los tres archivos (SAP, SAAD CBP y SAAD BUNKER). "
                                     "Revisá encabezados o usá ?roles=sap,cbp,bunker.")

        # Parseo normalizado
        sap = _parse_sap(sap_df)
        cbp = _parse_saad_cbp(cbp_df)
        bunker = _parse_saad_bunker(bunker_df, split_by_estado=split_by_estado)

        # Storages automáticos (o forzados)
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

        # Filtros
        sap_cbp = sap[sap["ubiest"].isin(stor_cbp)] if stor_cbp else sap
        cbp_f = cbp[cbp["ubiest"].isin(stor_cbp)] if stor_cbp else cbp
        sap_bun = sap[sap["ubiest"].isin(stor_bun)] if stor_bun else sap
        bunker_f = bunker[bunker["ubiest"].isin(stor_bun)] if stor_bun else bunker

        # Cruces
        cbp_g = cbp_f.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        sap_g = sap_cbp.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
        cbp_vs_sap = _cruzar(
            df_left=cbp_g,
            df_right=sap_g,
            on_cols=["codigo", "descripcion", "ubiest"],
            left_name="SAAD CBP",
            right_name="SAP COLGATE",
        ).sort_values(["ubiest", "codigo"])

        if split_by_estado:
            left = bunker_f.groupby(["codigo", "descripcion", "ubiest", "estado"], as_index=False)["cajas"].sum()
            right = sap_bun.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum()
            bun_vs_sap = pd.merge(left, right, on=["codigo", "descripcion", "ubiest"], how="left")
            bun_vs_sap["SAAD BUNKER"] = bun_vs_sap.pop("cajas_x").fillna(0).astype(int)
            bun_vs_sap["SAP COLGATE"] = bun_vs_sap.pop("cajas_y").fillna(0).astype(int)
            bun_vs_sap["DIFERENCIA"] = (bun_vs_sap["SAAD BUNKER"] - bun_vs_sap["SAP COLGATE"]).astype(int)
            bun_vs_sap["estado_ord"] = bun_vs_sap["estado"].map(_estado_key)
            bun_vs_sap = bun_vs_sap[
                ["codigo", "descripcion", "ubiest", "estado", "SAAD BUNKER", "SAP COLGATE", "DIFERENCIA", "estado_ord"]
            ].sort_values(["ubiest", "codigo", "estado_ord"]).drop(columns=["estado_ord"])
        else:
            bun_vs_sap = _cruzar(
                df_left=bunker_f.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum(),
                df_right=sap_bun.groupby(["codigo", "descripcion", "ubiest"], as_index=False)["cajas"].sum(),
                on_cols=["codigo", "descripcion", "ubiest"],
                left_name="SAAD BUNKER",
                right_name="SAP COLGATE",
            ).sort_values(["ubiest", "codigo"])

        # Resumen
        resumen = pd.DataFrame([
            {"Sección": "Storages CBP usados", "Valor": ", ".join(stor_cbp) or "(todos)"},
            {"Sección": "Storages BUNKER usados", "Valor": ", ".join(stor_bun) or "(todos)"},
            {"Sección": "Split por estado (BUNKER)", "Valor": str(split_by_estado)},
        ])

        # XLSX
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            resumen.to_excel(writer, index=False, sheet_name="Resumen")
            cbp_vs_sap.to_excel(writer, index=False, sheet_name="CBP_vs_SAP")
            bun_vs_sap.to_excel(writer, index=False, sheet_name="BUNKER_vs_SAP")
        buf.seek(0)

        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="cruce_ordenado.xlsx"'},
        )

    except ValueError as e:
        raise HTTPException(400, str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error al procesar: {e}")
