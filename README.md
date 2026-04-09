\# Pipeline d'ingestion de logs e-commerce vers HDFS avec Airflow



\## Description

Ce projet implémente un pipeline Data Engineering pour ingérer des logs Apache dans HDFS (Hadoop Distributed File System) via Apache Airflow.



\## Architecture

\- \*\*Airflow 2.8.0\*\* : Orchestration du pipeline

\- \*\*Hadoop HDFS 3.2.1\*\* : Stockage distribué des logs

\- \*\*Docker Compose\*\* : Déploiement local



\## Structure



├── dags/ # DAGs Airflow

│ └── logs\_ecommerce\_dag.py

├── scripts/ # Scripts utilitaires

│ └── generer\_logs.py

├── plugins/ # Plugins Airflow

├── logs/ # Logs Airflow (ignoré)

└── docker-compose.yaml # Configuration Docker





\## Fonctionnalités

\- Génération de logs Apache réalistes

\- Upload automatique vers HDFS

\- Détection du taux d'erreur (5xx/4xx)

\- Branchement conditionnel selon le taux d'erreur

\- Archivage dans la zone processed



\## Technologies

\- Apache Airflow 2.8

\- Apache Hadoop HDFS 3.2.1

\- Docker \& Docker Compose

\- Python 3.8

