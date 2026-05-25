import os
import pandas as pd
import mlflow
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error

# Configuration de base pour l'exécution locale (hors conteneur Docker)
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
client = mlflow.tracking.MlflowClient()

# 1. Charger les données de test et obtenir uniquement le dernier cycle pour chaque moteur
test_df = pd.read_csv('../data/test_FD001.csv', sep=',')
# Assurons-nous que la colonne engine_model est de type category (si elle existe)
if "engine_model" in test_df.columns:
    test_df["engine_model"] = test_df["engine_model"].astype("category")

def add_rolling_features(df, windows=[5, 10, 15]):
    exclude_cols = ['unit_number', 'time_in_cycles', 'engine_model', 'RUL']
    sensor_cols = [c for c in df.columns if c not in exclude_cols]
    df = df.sort_values(['unit_number', 'time_in_cycles'])
    
    new_features = []
    for w in windows:
        rolling_grp = df.groupby('unit_number')[sensor_cols].rolling(window=w, min_periods=1)
        mean_df = rolling_grp.mean().reset_index(level=0, drop=True)
        std_df = rolling_grp.std().reset_index(level=0, drop=True).fillna(0)
        
        mean_df.columns = [f"{c}_mean_{w}" for c in sensor_cols]
        std_df.columns = [f"{c}_std_{w}" for c in sensor_cols]
        new_features.extend([mean_df, std_df])
        
    return pd.concat([df] + new_features, axis=1)

# Appliquer la fenêtre glissante sur TOUT le jeu de test AVANT de filtrer la dernière ligne
test_df = add_rolling_features(test_df, windows=[5, 10, 15])

# On groupe par moteur et on prend la ligne avec le maximum de 'time_in_cycles'
last_cycles_df = test_df.groupby('unit_number').last().reset_index()

# Les colonnes de features utilisées par le modèle (en excluant les IDs et RUL si existants)
# Dans notre cas d'entraînement, on a pris toutes les colonnes sauf "RUL"
# Comme il n'y a pas de RUL dans test_FD001.csv, on passe directement ces colonnes
feature_cols = [c for c in test_df.columns if c != "RUL"]
X_test = last_cycles_df[feature_cols]

# 2. Charger les vraies valeurs de RUL depuis le fichier texte
# Le fichier contient une valeur par ligne, dans l'ordre des unit_number (1 à 100)
true_rul_df = pd.read_csv('../data/RUL_FD001.txt', header=None)
y_true = true_rul_df.iloc[:, 0].values

# 3. Récupérer le dernier modèle entraîné depuis MLflow
try:
    experiment = client.get_experiment_by_name("rul_xgboost")
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="tags.mlflow.runName != 'concept_drift_monitoring'",
        order_by=["start_time DESC"],
        max_results=1
    )
    latest_run_id = runs[0].info.run_id
    model_uri = f"runs:/{latest_run_id}/model"
    print(f"Chargement du modèle MLflow depuis l'URI: {model_uri}")
    
    loaded_model = mlflow.xgboost.load_model(model_uri)
    
except Exception as e:
    print(f"Erreur lors de la récupération du modèle MLflow : {e}")
    exit(1)

# 4. Faire les prédictions
y_pred = loaded_model.predict(X_test)

# 5. Calculer et afficher les métriques
rmse = mean_squared_error(y_true, y_pred, squared=False)
mae = mean_absolute_error(y_true, y_pred)

print("\n=== RÉSULTATS DE L'ÉVALUATION SUR LE JEU DE TEST (RUL_FD001.txt) ===")
print(f"RMSE (Root Mean Squared Error) : {rmse:.2f} cycles")
print(f"MAE (Mean Absolute Error)      : {mae:.2f} cycles")

# Affichage des 10 premières prédictions vs vraies valeurs
print("\nAperçu des 10 premiers moteurs :")
preview_df = pd.DataFrame({
    'Moteur': last_cycles_df['unit_number'].head(10),
    'RUL_Prédit': y_pred[:10].round(1),
    'RUL_Réel': y_true[:10]
})
print(preview_df.to_string(index=False))
