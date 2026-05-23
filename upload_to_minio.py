# pyrefly: ignore [missing-import]
from minio import Minio
import os

def main():
    # 1. Initialiser le client MinIO
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False # Car on est en local sans HTTPS
    )

    bucket_name = "iot-lake"
    file_path = r"C:\Users\aya\Desktop\projet_iot\-Maintenance-pr-dictive-pour-IoT-industriel-avec-Data-Warehouse-temporel\data\train_FD001_avec_RUL.csv"
    object_name = "raw_data/train_FD001_avec_RUL.csv" # Le chemin dans MinIO

    # 2. Vérifier si le bucket existe
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' créé.")
    else:
        print(f"Le bucket '{bucket_name}' existe déjà.")

    # 3. Uploader le fichier
    print(f"Envoi du fichier {file_path} vers {bucket_name}/{object_name}...")
    try:
        client.fput_object(
            bucket_name, 
            object_name, 
            file_path,
        )
        print("✅ Fichier uploadé avec succès dans MinIO !")
    except Exception as e:
        print(f"❌ Erreur lors de l'upload: {e}")

if __name__ == "__main__":
    main()
