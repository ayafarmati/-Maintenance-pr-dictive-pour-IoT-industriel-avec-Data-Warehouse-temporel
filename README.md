# Maintenance Prédictive pour IoT Industriel avec Data Warehouse Temporel

![Architecture Globale](https://img.shields.io/badge/Architecture-Medallion-blue)
![Stack](https://img.shields.io/badge/Stack-Spark%20%7C%20Hudi%20%7C%20ClickHouse%20%7C%20Airflow-orange)
![ML](https://img.shields.io/badge/MLOps-MLflow%20%7C%20XGBoost-green)

## 📌 Description du Projet

Ce projet implémente une architecture Data & MLOps complète (End-to-End) pour la **maintenance prédictive de moteurs d'avion (turbofans)**. 
Il simule la réception de données de capteurs IoT en temps réel (basé sur le dataset NASA CMAPSS), traite ces flux à grande échelle via une architecture **Medallion (Bronze, Silver, Gold)**, applique un modèle de Machine Learning pour prédire la Durée de Vie Utile Restante (RUL - Remaining Useful Life), et stocke les résultats dans un Data Warehouse ultra-rapide structuré en modèle dimensionnel (Schéma en Étoile) pour la Business Intelligence.

---

## 🏗️ Architecture et Flux de Données

L'architecture repose sur des standards modernes de **Data Lakehouse** (Apache Hudi + MinIO) et de **MLOps** (MLflow).

### 1. Ingestion (IoT -> Bronze)
- **MQTT Broker (Mosquitto)** : Reçoit les données de télémétrie en temps réel (21 capteurs + 3 paramètres opérationnels par moteur).
- **MQTT Bridge** : Récupère les messages MQTT et les stocke temporairement au format JSON.
- **Spark Structured Streaming (`mqtt_to_hudi.py`)** : Lit les flux JSON en continu et les insère dans la couche **Bronze** (`brz_sensor_metrics_mqtt`) sur MinIO au format Apache Hudi (Merge-On-Read).

### 2. Nettoyage & Feature Store (Bronze -> Silver)
- **Traitement Batch (`bronze_to_silver.py`)** : Exécuté toutes les 30 minutes par Airflow.
- **Nettoyage et Imputation** : Remplace les valeurs manquantes/aberrantes en utilisant un référentiel historique (Baselines).
- **Agrégation** : Calcule des moyennes glissantes sur des fenêtres d'une minute pour lisser le bruit des capteurs.
- **Stockage Silver** : Les données propres sont sauvegardées dans `slv_sensor_features` (Hudi). Cette couche agit comme un véritable **Lakehouse Feature Store**.

### 3. Modélisation MLOps (MLflow + XGBoost)
- **Entraînement (`train_xgboost.py`)** : Un modèle XGBoost est entraîné sur les données historiques pour apprendre à prédire le RUL en fonction de la dégradation des capteurs.
- **MLflow** : Le modèle, ses hyperparamètres, et sa *Signature* (types de données attendus) sont versionnés et enregistrés dans le registre MLflow.

### 4. Inférence & Data Warehouse (Silver -> Gold)
- **Inférence Distribuée (`silver_to_gold_ml.py`)** : Ce job est déclenché par Airflow de manière dynamique (**Data-Aware Scheduling**) dès que la couche Silver est mise à jour. Il récupère le dernier modèle de MLflow, lit les nouvelles *features* Silver, et prédit le RUL pour chaque moteur.
- **Modélisation Dimensionnelle (ClickHouse)** : Le script transforme ensuite les données en un schéma en étoile ultra-performant et l'exporte vers **ClickHouse** (`iot_metrics_DW`) :
  - `dim_engine` : Dimension des moteurs.
  - `dim_date` : Dimension temporelle.
  - `dim_status` : Dimension de santé (Critique, Avertissement, Sain).
  - `fact_engine_health` : Table des faits contenant les 24 capteurs et la prédiction du RUL.

---

## 🚀 Stack Technique

* **Langage** : Python 3.11
* **Streaming & Traitement Distribué** : Apache Spark (PySpark)
* **Data Lakehouse / Stockage** : Apache Hudi, MinIO (compatible S3)
* **Data Warehouse / OLAP** : ClickHouse
* **Orchestration** : Apache Airflow (avec Data-Aware Scheduling / Datasets)
* **Machine Learning** : XGBoost, Pandas, Scikit-learn
* **MLOps** : MLflow
* **IoT / Messagerie** : Eclipse Mosquitto (MQTT)
* **Infrastructure** : Docker & Docker Compose

---

## 🛠️ Démarrage Rapide

### 1. Prérequis
- Docker et Docker Compose installés (avec au moins 8 Go de RAM alloués à Docker).

### 2. Lancer l'infrastructure
À la racine du projet, exécutez la commande suivante pour démarrer tous les services :
```bash
docker compose up -d
```
*(Patientez quelques minutes le temps que l'initialisation de Spark, Airflow et ClickHouse se termine).*

### 3. Lancer le simulateur de capteurs IoT
Pour commencer à envoyer des données artificielles vers le broker MQTT :
```bash
python tools/mqtt_test_stream.py
```

### 4. Suivi et Interfaces
Vous pouvez accéder aux différentes interfaces Web :
- **MinIO (Data Lake)** : [http://localhost:9001](http://localhost:9001) (User: `minio` / Pass: `minio123`)
- **MLflow (MLOps)** : [http://localhost:5000](http://localhost:5000)
- **Airflow (Orchestration)** : [http://localhost:8080](http://localhost:8080) (User: `admin` / Pass: `admin`)
- **ClickHouse (Data Warehouse)** : Port natif `9000` ou HTTP `8123` (User: `iot` / Pass: `iot123`)
- **Spark Master UI** : [http://localhost:8081](http://localhost:8081)

---

## 📊 Cas d'Usage BI (Business Intelligence)

Le Data Warehouse final dans ClickHouse (`iot_metrics_DW`) est prêt à être connecté à des outils de visualisation comme **Power BI**, **Grafana** ou **Apache Superset**.

Grâce à la dimension `dim_status`, un Data Analyst peut facilement créer un tableau de bord en temps réel qui :
1. Isole instantanément les moteurs en état **Critique** (RUL <= 15).
2. Met en corrélation la chute de la durée de vie prédite avec la montée en température ou pression des capteurs physiques.
3. Permet aux équipes de maintenance de planifier les réparations avant la panne critique (Moteur en état d'**Avertissement**).