from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import subprocess
import logging
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generer_logs_journaliers(**context):
    """Génère 1000 lignes de logs Apache pour la date d'exécution du DAG."""
    execution_date = context["ds"]
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"
    
    result = subprocess.run(
        ["python3", script_path, execution_date, "1000", fichier_sortie],
        capture_output=True,
        text=True,
        check=True
    )
    
    logging.info(f"Script output: {result.stdout}")
    logging.info(f"Fichier généré: {fichier_sortie}")
    
    taille = os.path.getsize(fichier_sortie)
    logging.info(f"Taille du fichier: {taille} octets")
    
    return fichier_sortie

def brancher_selon_taux_erreur(**context):
    """Lit le taux d'erreur et décide quelle branche suivre."""
    execution_date = context["ds"]
    fichier_taux = f"/tmp/taux_erreur_{execution_date}.txt"
    SEUIL_ERREUR_PCT = 5.0
    
    try:
        with open(fichier_taux, 'r') as f:
            contenu = f.read().strip()
            erreurs, total = map(int, contenu.split())
        
        taux_pct = (erreurs / total) * 100
        logging.info(f"Taux d'erreur: {taux_pct:.2f}% ({erreurs}/{total})")
        
        if taux_pct > SEUIL_ERREUR_PCT:
            logging.warning(f"Taux d'erreur {taux_pct:.2f}% dépasse le seuil {SEUIL_ERREUR_PCT}%")
            return "alerter_equipe_ops"
        else:
            logging.info(f"Taux d'erreur normal: {taux_pct:.2f}%")
            return "archiver_rapport_ok"
    except Exception as e:
        logging.error(f"Erreur lecture taux: {e}")
        return "alerter_equipe_ops"

def alerter_equipe_ops(**context):
    """Simule l'envoi d'une alerte à l'équipe Ops."""
    execution_date = context["ds"]
    logging.warning(f"[ALERTE] Taux d'erreur HTTP anormal détecté pour les logs du {execution_date}")
    logging.warning("Vérifiez les serveurs web immédiatement!")

def archiver_rapport_ok(**context):
    """Logue que tout est nominal."""
    execution_date = context["ds"]
    logging.info(f"[OK] Taux d'erreur dans les seuils normaux pour les logs du {execution_date}")

with DAG(
    'logs_ecommerce_dag',
    default_args=default_args,
    description='Pipeline d ingestion de logs e-commerce vers HDFS',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'hdfs', 'logs'],
) as dag:
    
    t_generer = PythonOperator(
        task_id='generer_logs_journaliers',
        python_callable=generer_logs_journaliers,
    )
    
t_upload = BashOperator(
    task_id="uploader_vers_hdfs",
    bash_command="""
        set -e
        EXECUTION_DATE="{{ ds }}"
        FICHIER_LOCAL="/tmp/access_${EXECUTION_DATE}.log"
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        TEMP_FILE="/tmp/access_temp_${EXECUTION_DATE}.log"

        echo "[INFO] Upload de ${FICHIER_LOCAL} vers HDFS"

        if [ ! -f "${FICHIER_LOCAL}" ]; then
            echo "[ERROR] Fichier introuvable!"
            exit 1
        fi

        # Copier vers namenode via fichier temporaire partagé
        echo "[INFO] Copie du fichier vers namenode"
        
        # Créer un volume partagé temporaire ou utiliser /tmp partagé
        # Solution: copier le contenu via cat et pipe
        cat ${FICHIER_LOCAL} | docker exec -i namenode tee /tmp/access_${EXECUTION_DATE}.log > /dev/null

        # Upload vers HDFS
        echo "[INFO] Upload vers HDFS"
        docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/raw
        docker exec namenode hdfs dfs -put -f /tmp/access_${EXECUTION_DATE}.log ${CHEMIN_HDFS}

        # Nettoyer
        docker exec namenode rm /tmp/access_${EXECUTION_DATE}.log

        # Vérifier
        echo "[INFO] Vérification du fichier dans HDFS"
        docker exec namenode hdfs dfs -ls ${CHEMIN_HDFS}

        echo "[OK] Upload terminé"
    """
)
# Sensor utilisant BashOperator
t_sensor = BashOperator(
        task_id='hdfs_file_sensor',
        bash_command="""
        EXECUTION_DATE={{ ds }}
        CHEMIN_HDFS=/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log
        
        echo "[INFO] Attente du fichier dans HDFS: ${CHEMIN_HDFS}"
        
        # Boucle d'attente avec timeout (30 tentatives * 10 secondes = 5 minutes)
        for i in {1..30}; do
            if docker exec namenode hdfs dfs -test -e ${CHEMIN_HDFS}; then
                echo "[OK] Fichier trouvé dans HDFS après ${i} tentatives"
                exit 0
            fi
            echo "[INFO] Tentative $i/30 - Fichier non trouvé, nouvelle tentative dans 10s..."
            sleep 10
        done
        
        echo "[ERROR] Timeout: Fichier non trouvé dans HDFS après 5 minutes"
        exit 1
        """,
    )
    
t_analyser = BashOperator(
        task_id='analyser_logs_hdfs',
        bash_command="""
        EXECUTION_DATE={{ ds }}
        CHEMIN_HDFS="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        
        echo "[INFO] Lecture du fichier HDFS: ${CHEMIN_HDFS}"
        
        docker exec namenode hdfs dfs -cat "${CHEMIN_HDFS}" > /tmp/logs_analyse_${EXECUTION_DATE}.txt
        
        echo "=== STATUS CODES ==="
        grep -oE '"[A-Z]+ [^ ]+ HTTP/1\\.[01]" [0-9]{3}' /tmp/logs_analyse_${EXECUTION_DATE}.txt | grep -oE '[0-9]{3}$' | sort | uniq -c | sort -rn
        
        echo "=== TOP 5 URLS ==="
        grep -oE '"(GET|POST) [^"]+' /tmp/logs_analyse_${EXECUTION_DATE}.txt | cut -d' ' -f2 | sort | uniq -c | sort -rn | head -5
        
        TOTAL=$(wc -l < /tmp/logs_analyse_${EXECUTION_DATE}.txt)
        ERREURS=$(grep -cE '"[A-Z]+ [^ ]+ HTTP/1\\.[01]" [45][0-9]{2}' /tmp/logs_analyse_${EXECUTION_DATE}.txt || echo 0)
        
        echo "=== TAUX ERREUR ==="
        echo "Total: ${TOTAL}, Erreurs: ${ERREURS}"
        
        echo "${ERREURS} ${TOTAL}" > /tmp/taux_erreur_${EXECUTION_DATE}.txt
        rm /tmp/logs_analyse_${EXECUTION_DATE}.txt
        """,
    )
    
t_branch = BranchPythonOperator(
        task_id='brancher_selon_taux_erreur',
        python_callable=brancher_selon_taux_erreur,
    )
    
t_alerte = PythonOperator(
        task_id='alerter_equipe_ops',
        python_callable=alerter_equipe_ops,
    )
    
t_archive_ok = PythonOperator(
        task_id='archiver_rapport_ok',
        python_callable=archiver_rapport_ok,
    )
    
t_archiver = BashOperator(
        task_id='archiver_logs_hdfs',
        bash_command="""
        EXECUTION_DATE={{ ds }}
        SOURCE="/data/ecommerce/logs/raw/access_${EXECUTION_DATE}.log"
        DESTINATION="/data/ecommerce/logs/processed/access_${EXECUTION_DATE}.log"
        
        echo "[INFO] Déplacement HDFS: ${SOURCE} → ${DESTINATION}"
        
        docker exec namenode hdfs dfs -mv ${SOURCE} ${DESTINATION}
        
        echo "[OK] Fichier archivé dans la zone processed"
        """,
        trigger_rule='none_failed_min_one_success',
    )
    
    # Définition des dépendances
t_generer >> t_upload >> t_sensor >> t_analyser >> t_branch
t_branch >> [t_alerte, t_archive_ok] >> t_archiver