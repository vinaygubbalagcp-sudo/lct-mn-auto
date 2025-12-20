from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from mock_intervies_setup import run_interview_matching

# -------------------------------------------------
# DEFAULT ARGS
# -------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------
with DAG(
    dag_id="daily_interview_slot_matching",
    default_args=default_args,
    description="Match interview slots from Google Sheets and notify users",
    schedule_interval="0 9 * * *",  # Daily at 9 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["google-sheets", "secret-manager", "email", "bigquery"],
) as dag:

    run_matching_job = PythonOperator(
        task_id="run_interview_matching_job",
        python_callable=run_interview_matching,
    )

    run_matching_job
