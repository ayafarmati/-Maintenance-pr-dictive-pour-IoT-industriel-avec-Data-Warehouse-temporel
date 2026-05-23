import os

import mlflow
import mlflow.xgboost
from mlflow.models.signature import infer_signature
import pandas as pd
import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split


TRAIN_DATA_URI = os.getenv("TRAIN_DATA_URI")
TRAIN_DATA_DELIMITER = os.getenv("TRAIN_DATA_DELIMITER", ";")
S3_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL") or os.getenv("AWS_S3_ENDPOINT") or os.getenv("MLFLOW_S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

if not TRAIN_DATA_URI:
    raise RuntimeError("TRAIN_DATA_URI is required")

mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
mlflow.set_experiment("rul_xgboost")


def build_storage_options(uri: str | None) -> dict:
    if not uri or not uri.startswith("s3://"):
        return {}
    options: dict = {}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        options["key"] = AWS_ACCESS_KEY_ID
        options["secret"] = AWS_SECRET_ACCESS_KEY
    if AWS_SESSION_TOKEN:
        options["token"] = AWS_SESSION_TOKEN
    if S3_ENDPOINT_URL:
        options["client_kwargs"] = {"endpoint_url": S3_ENDPOINT_URL}
        options["config_kwargs"] = {"s3": {"addressing_style": "path"}}
    return options

train_df = pd.read_csv(
    TRAIN_DATA_URI,
    sep=TRAIN_DATA_DELIMITER,
    storage_options=build_storage_options(TRAIN_DATA_URI),
)

if "RUL" not in train_df.columns:
    raise RuntimeError("RUL column not found in training dataset")

if "engine_model" in train_df.columns:
    train_df["engine_model"] = train_df["engine_model"].astype("category")

feature_cols = [c for c in train_df.columns if c != "RUL"]
X = train_df[feature_cols]
y = train_df["RUL"]

X_train, X_val, y_train, y_val = train_test_split(
    X, y, test_size=0.2, random_state=42
)

params = {
    "n_estimators": 300,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "objective": "reg:squarederror",
    "random_state": 42,
    "enable_categorical": True,
}

with mlflow.start_run():
    model = xgb.XGBRegressor(**params)
    model.fit(X_train, y_train)

    preds = model.predict(X_val)
    rmse = mean_squared_error(y_val, preds, squared=False)
    mae = mean_absolute_error(y_val, preds)

    mlflow.log_params(params)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    signature = infer_signature(X_train, preds)
    mlflow.xgboost.log_model(model, "model", model_format="json", signature=signature)

