from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Définition des arguments par défaut du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Définition du DAG
with DAG(
    "velib_weather_pipeline",
    default_args=default_args,
    description="Pipeline d'ingestion et de transformation Vélib et Météo",
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 19),
    catchup=False,
    tags=["velib", "weather"],
) as dag:

    # 1. Extraction du status Vélib (temps réel)
    # L'URI S3 finale sera écrite sur stdout et poussée dans XCom
    extract_status = BashOperator(
        task_id="extract_status",
        bash_command="python /app/extract.py status --date '{{ dag_run.logical_date.isoformat() }}'",
        do_xcom_push=True,
    )

    # 2. Extraction des informations statiques des stations Vélib
    # L'URI S3 finale sera écrite sur stdout et poussée dans XCom
    extract_info = BashOperator(
        task_id="extract_info",
        bash_command="python /app/extract.py info --date '{{ dag_run.logical_date.isoformat() }}'",
        do_xcom_push=True,
    )

    # 3. Extraction de la météo correspondante
    # L'URI S3 finale sera écrite sur stdout et poussée dans XCom
    extract_weather = BashOperator(
        task_id="extract_weather",
        bash_command="python /app/extract.py weather --date '{{ dag_run.logical_date.isoformat() }}'",
        do_xcom_push=True,
    )

    # 4. Transformation et enrichissement des données avec PySpark
    # Les chemins S3 générés par les extractions sont récupérés depuis XCom (ti.xcom_pull)
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command="""
        python /app/transform.py \
            --velib-status-path "{{ ti.xcom_pull(task_ids='extract_status') }}" \
            --velib-info-path "{{ ti.xcom_pull(task_ids='extract_info') }}" \
            --weather-path "{{ ti.xcom_pull(task_ids='extract_weather') }}"
        """,
    )

    # Définition de l'ordre d'exécution
    # Les trois extractions se lancent en parallèle avant d'exécuter la transformation
    [extract_status, extract_info, extract_weather] >> transform_data
