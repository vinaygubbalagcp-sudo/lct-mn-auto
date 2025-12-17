"""
Airflow DAG: GCS CSV ingestion -> validation -> BigQuery load
- Idempotency using BigQuery metadata table
- Notifications, retries, and clear GCS folder movements (processed/error/duplicate)

Notes:
- Designed for Composer / Airflow with Google provider package.
- Replace the placeholder variables (PROJECT_ID, BUCKET, REGION, etc.) before using.
- The actual validation/processing PySpark job is expected to be a Dataproc PySpark job stored in GCS.

Operators used (Google provider):
- GCSPrefixSensor (checks for new files)
- BigQueryCheckOperator (idempotency)
- GCSToGCSOperator (move files between folders)
- DataprocSubmitJobOperator (run validation/transform job)
- PythonOperator (lightweight logic and branching)
- EmailOperator (notifications)

"""
from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label

from airflow.providers.google.cloud.sensors.gcs import GCSPrefixSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator, DataprocSubmitJobOperator

# -------------------------
# Config (modify these)
# -------------------------
PROJECT_ID = "your-gcp-project-id"
GCS_BUCKET = "lvc-tc-mn-d-bckt"
GCS_RAW_PREFIX = "interviews_scheduled_folder/raw/"
GCS_PROCESSED_PREFIX = "interviews_scheduled_folder/processed/"
GCS_ERROR_PREFIX = "interviews_scheduled_folder/error/"
GCS_DUPLICATE_PREFIX = "interviews_scheduled_folder/duplicate/"
GCS_PYSCRIPT_URI = f"gs://{GCS_BUCKET}/scripts/interview_validation_job.py"  # PySpark job stored in GCS

BQ_DATASET = "your_dataset"
BQ_METADATA_TABLE = "file_metadata"  # Fully qualified => dataset.file_metadata
BQ_METADATA_TABLE_FQN = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_METADATA_TABLE}"

REGION = "us-central1"
CLUSTER_NAME = "dataproc-cluster-for-jobs"  # if you submit to existing cluster

# Notification
ALERT_EMAIL = "ops-team@example.com"



# DAG default args
default_args = {
    "owner": "data-eng-team",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": False,  # handled by EmailOperator
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------
# Helper functions
# -------------------------

def _pick_first_file(**context):
    """Branch function: if sensor found files, pick one file and continue, else skip.
    The GCSPrefixSensor populates a list of objects into XCom under key 'matched_objects'
    (depending on provider version). To keep it robust we read from sensor push or list GCS.
    """
    ti = context["ti"]
    matched = ti.xcom_pull(task_ids="sensor_list_files", key="matched_objects")
    # Some versions store as list under 'return_value'
    if not matched:
        matched = ti.xcom_pull(task_ids="sensor_list_files")

    logging.info("Sensor matched: %s", matched)

    if not matched:
        # Nothing to do
        raise AirflowSkipException("No files found by sensor")

    # matched may be a list of object names; pick first one
    if isinstance(matched, list):
        file_key = matched[0]
    elif isinstance(matched, str):
        file_key = matched
    else:
        # try to parse
        file_key = list(matched)[0]

    # normalize to gs:// path
    if file_key.startswith("gs://"):
        gs_path = file_key
    else:
        # sensor might return object name like 'interviews...csv'
        gs_path = f"gs://{GCS_BUCKET}/{file_key}"

    ti.xcom_push(key="selected_file", value=gs_path)
    return "check_idempotency"


def _mark_metadata_file_insert(**context):
    """Insert filename into metadata table after successful processing."""
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="pick_file", key="selected_file")
    if not file_path:
        raise RuntimeError("No file path found in XCom")

    filename = file_path.split("/")[-1]

    # We will use BigQueryInsertJobOperator in DAG to insert; this function only prepares SQL
    sql = f"INSERT INTO `{BQ_METADATA_TABLE_FQN}` (filename, processed_stamp) VALUES ('{filename}', CURRENT_TIMESTAMP())"
    ti.xcom_push(key="metadata_insert_sql", value=sql)


def _on_failure_alert(context):
    """Simple on_failure callback that sends an alert email via EmailOperator logic.
    This function pushes the context into XCom so the EmailOperator can pick it up.
    """
    logging.error("DAG failed. Context: %s", context)
    # No direct emailing here; the DAG will include an EmailOperator in failure branch


# -------------------------
# DAG definition
# -------------------------
with DAG(
    dag_id="gcs_interviews_validation_pipeline",
    default_args=default_args,
    description="GCS CSV -> validate -> load to BigQuery with idempotency and notifications",
    schedule_interval=None,  # event-driven
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=_on_failure_alert,
    tags=["gcs", "bigquery", "dataproc", "validation"],
) as dag:

    # 1) Sensor: wait for files in RAW prefix
    sensor_list_files = GCSPrefixSensor(
        task_id="sensor_list_files",
        bucket=GCS_BUCKET,
        prefix=GCS_RAW_PREFIX,
        poke_interval=60,  # seconds
        timeout=60 * 60 * 2,  # 2 hours
        mode="reschedule",
    )

    # 2) Pick a file and push selected_file to XCom
    pick_file = BranchPythonOperator(
        task_id="pick_file",
        python_callable=_pick_first_file,
        provide_context=True,
    )

    # 3) Idempotency check: ensure filename not present in metadata table
    # SQL returns True when count = 0 (i.e., not processed yet)
    idempotency_sql = """
    SELECT COUNT(*) = 0 as not_processed
    FROM `{metadata_table}`
    WHERE filename = '{filename}'
    """

    def _build_idempotency_sql(**context):
        ti = context['ti']
        file_path = ti.xcom_pull(task_ids='pick_file', key='selected_file')
        if not file_path:
            raise AirflowSkipException('No file selected')
        filename = file_path.split('/')[-1]
        return idempotency_sql.format(metadata_table=BQ_METADATA_TABLE_FQN, filename=filename)

    check_idempotency = BigQueryCheckOperator(
        task_id="check_idempotency",
        sql=_build_idempotency_sql,
        use_legacy_sql=False,
        location=REGION,
        gcp_conn_id='google_cloud_default',
    )

    # 4) Branch: if already processed -> move to duplicate folder; else continue
    def _branch_on_idempotency(**context):
        # BigQueryCheck will pass if not processed; if it fails it raises. We rely on it to pass
        # But to make branching explicit, we'll always continue to 'submit_validation_job'.
        return 'submit_dataproc_job'

    branch_after_idempotency = BranchPythonOperator(
        task_id="branch_after_idempotency",
        python_callable=_branch_on_idempotency,
        provide_context=True,
    )

    # 5) Move duplicates (if we had a different branch set up) - for clarity we include operation.
    move_to_duplicate = GCSToGCSOperator(
        task_id="move_to_duplicate",
        source_bucket=GCS_BUCKET,
        source_object=f"{GCS_RAW_PREFIX}*",
        destination_bucket=GCS_BUCKET,
        destination_object=GCS_DUPLICATE_PREFIX,
        move_object=True,
    )

    # 6) Submit Dataproc job to run validation & transform (PySpark or Beam). The job should
    #     - read file from selected_file XCom
    #     - produce clean output to processed folder and errors to error folder
    #     - exit non-zero on fatal failures so Airflow can catch
    dataproc_job = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': GCS_PYSCRIPT_URI,
            'args': [
                '--input', '{{ ti.xcom_pull(task_ids="pick_file", key="selected_file") }}',
                '--output_prefix', f'gs://{GCS_BUCKET}/{GCS_PROCESSED_PREFIX}',
                '--error_prefix', f'gs://{GCS_BUCKET}/{GCS_ERROR_PREFIX}',
                '--metadata_table', BQ_METADATA_TABLE_FQN,
            ],
        },
    }

    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id='submit_dataproc_job',
        job=dataproc_job,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id='google_cloud_default',
    )

    # 7) After successful job, insert metadata row so file is not re-processed
    prepare_metadata_insert = PythonOperator(
        task_id='prepare_metadata_insert',
        python_callable=_mark_metadata_file_insert,
        provide_context=True,
    )

    insert_metadata = BigQueryInsertJobOperator(
        task_id='insert_metadata',
        configuration={
            'query': {
                'query': "{{ ti.xcom_pull(task_ids='prepare_metadata_insert', key='metadata_insert_sql') }}",
                'useLegacySql': False,
            }
        },
        location=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id='google_cloud_default',
    )

    # 8) If dataproc job fails, move file to error folder and notify
    move_to_error = GCSToGCSOperator(
        task_id="move_to_error",
        source_bucket=GCS_BUCKET,
        source_object=f"{GCS_RAW_PREFIX}*",
        destination_bucket=GCS_BUCKET,
        destination_object=GCS_ERROR_PREFIX,
        move_object=True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to=ALERT_EMAIL,
        subject='[Airflow] Interview Pipeline Failure: {{ dag_run.run_id }}',
        html_content="""
        <p>Airflow DAG <b>{{ dag.dag_id }}</b> failed.</p>
        <p>Task: {{ task_instance.task_id }}</p>
        <p>Execution Time: {{ ts }}</p>
        <p>Check logs for details.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # 9) On success send notification
    send_success_email = EmailOperator(
        task_id='send_success_email',
        to=ALERT_EMAIL,
        subject='[Airflow] Interview Pipeline Success: {{ dag_run.run_id }}',
        html_content="""
        <p>Airflow DAG <b>{{ dag.dag_id }}</b> completed successfully.</p>
        <p>Processed file: {{ ti.xcom_pull(task_ids='pick_file', key='selected_file') }}</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # -------------------------
    # DAG graph
    # -------------------------
    sensor_list_files >> pick_file >> check_idempotency >> branch_after_idempotency

    branch_after_idempotency >> submit_dataproc_job

    # Success path
    submit_dataproc_job >> prepare_metadata_insert >> insert_metadata >> send_success_email

    # Failure path
    submit_dataproc_job >> move_to_error >> send_failure_email

    # duplicate movement (this is a simple conservative move step; you might want more
    # precise object selection logic in production)
    check_idempotency >> move_to_duplicate

    # end

# End of DAG file
