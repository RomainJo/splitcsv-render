import os
import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
import shutil
import tempfile

# Setup FastAPI app
app = FastAPI()

# Dossier temporaire pour les fichiers découpés
TEMP_DIR = "split_csv_files"
os.makedirs(TEMP_DIR, exist_ok=True)

# FastAPI endpoint to split CSV
@app.post("/split_csv/")
async def split_csv(file: UploadFile = File(...), rows_per_file: int = 5000):
    # Créer un fichier temporaire pour stocker l'upload
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        shutil.copyfileobj(file.file, tmp_file)
        tmp_file_path = tmp_file.name

    # Lire le fichier CSV en forçant toutes les colonnes comme chaîne
    df = pd.read_csv(tmp_file_path, dtype=str)

    output_paths = []

    # Découper le fichier en plusieurs morceaux
    for i, chunk in enumerate(range(0, len(df), rows_per_file)):
        chunk_df = df.iloc[chunk:chunk + rows_per_file]
        chunk_path = os.path.join(TEMP_DIR, f"split_{i+1}.csv")
        chunk_df.to_csv(chunk_path, index=False)
        output_paths.append(chunk_path)  # Ajouter le chemin du fichier découpé

    # Retourner les liens de téléchargement des fichiers
    download_links = [
        {"file_name": os.path.basename(path), "url": f"/download/{os.path.basename(path)}"}
        for path in output_paths
    ]

    return JSONResponse(content={"download_links": download_links})


# Route pour télécharger un fichier découpé
@app.get("/download/{file_name}")
async def download_file(file_name: str):
    file_path = os.path.join(TEMP_DIR, file_name)
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return {"error": "File not found"}

