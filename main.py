@app.post("/cruce/xlsx")
async def cruce_archivos_xlsx(
    files: List[UploadFile] = File(...),
    min_diff: int = 0
):
    """
    Igual que /cruce, pero devuelve Excel (.xlsx)
    con resumen, diferencias y dashboard.
    """
    # ... (lógica de comparación, como en /cruce) ...

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        merged.to_excel(writer, sheet_name="Todo", index=False)
        diffs_only.to_excel(writer, sheet_name="Diferencias", index=False)

        # crear dashboard de diferencias
        resumen_df = pd.DataFrame([resumen])
        resumen_df.to_excel(writer, sheet_name="Resumen", index=False)

        # gráfico básico
        workbook  = writer.book
        worksheet = writer.sheets["Resumen"]
        chart = workbook.add_chart({"type": "column"})
        chart.add_series({
            "name": "Diferencias",
            "categories": ["Resumen", 1, 1, 1, 3],
            "values":     ["Resumen", 1, 2, 1, 3],
        })
        worksheet.insert_chart("E2", chart)

    output.seek(0)
    headers = {
        "Content-Disposition": 'attachment; filename="cruce_inventario.xlsx"'
    }
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)
