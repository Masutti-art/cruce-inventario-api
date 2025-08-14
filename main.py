from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
import pandas as pd
import io

app = FastAPI()

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    contents = await file.read()
    excel_file = pd.ExcelFile(io.BytesIO(contents))

    if "Comparacion Inventario" not in excel_file.sheet_names:
        return {"error": "La hoja 'Comparacion Inventario' no existe"}

    df = excel_file.parse("Comparacion Inventario")

    required_columns = ["Total_SAP", "Total_SAAD"]
    if not all(col in df.columns for col in required_columns):
        return {"error": "Faltan columnas necesarias: Total_SAP o Total_SAAD"}

    if "Diferencia_Total" not in df.columns:
        df["Diferencia_Total"] = df["Total_SAAD"] - df["Total_SAP"]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name="Resultado Cruce")

    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=cruce_resultado.xlsx"})