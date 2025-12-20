from airflow import DAG
from airflow.model import DAG
from airflow.operators.python import PythonOperator 
from airfllow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import smtplib
from google.cloud import bigquery

# -------------------------------------------------
# BIGQUERY CONFIG
# -------------------------------------------------
PROJECT_ID = "lvc-tc-mn-d"  # your GCP project
DATASET_ID = "interviews_stage"    # your BigQuery dataset
TABLE_ID = "scheduled_interviews"    # target table

# -------------------------------------------------
# EMAIL CONFIG
# -------------------------------------------------
SENDER_EMAIL = "letsmailrahul27@gmail.com"
SENDER_PASSWORD = "pkpd oqsi sfjj iwyo"

# -------------------------------------------------
# SERVICE ACCOUNT CONFIG
# -------------------------------------------------
SERVICE_ACCOUNT_FILE = r"C:\lct-dm-p\lct-mn-auto\gcp-source\secrets\service_account_credentials.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# -------------------------------------------------
# GOOGLE SHEET DETAILS
# -------------------------------------------------
SHEET_ID = "18LJ0dh7W8u698vPsB6Ew6xOgDONPynYcYE0SU6UzbiU"

GIVERS_WORKSHEET = "Candidate_Availability"
TAKERS_WORKSHEET = "Panel_Availability"

#------------------
# defualt-args
#------------------

default_args = {
    'owner' : 'lavuscloud',
    'start_date':datetime(2025,12,18),
    'retries':1,
    'retry_delay':timedelta(minutes=2),
    'catch_up':False 
}

# -------------------------------------------------
# AUTHENTICATED SHEET READ
# -------------------------------------------------
# -------------------------------------------------
# AUTHENTICATED SHEET READ
# -------------------------------------------------
def read_google_sheet(sheet_id, worksheet_name):
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )

    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    # normalize column names
    df.columns = df.columns.str.strip().str.lower()

    return df

# -------------------------------------------------
# READ & FILTER DATA
# -------------------------------------------------
def read_and_filter_data():
    df1 = read_google_sheet(SHEET_ID, GIVERS_WORKSHEET)
    df2 = read_google_sheet(SHEET_ID, TAKERS_WORKSHEET)

    current_date = datetime.now().date()

    df1['date'] = pd.to_datetime(
        df1['date'],
        format="%m/%d/%Y",
        errors='coerce'
    ).dt.date

    df2['date'] = pd.to_datetime(
        df2['date'],
        format="%m/%d/%Y",
        errors='coerce'
    ).dt.date

    df1 = df1[df1['date'].notna()]
    df2 = df2[df2['date'].notna()]

    return (
        df1[df1['date'] == current_date],
        df2[df2['date'] == current_date]
    )


with DAG(
       dag_id='interviews_schedule_dag_v1',
       description='This is to schedule the interviews for the professionals',
       default_args=default_args,
       schedule_interval=None # cron job 
       
) as dag:
    
    #----------------------------
    #Tasks definitions section
    #----------------------------
    
    start_task = EmptyOperator(
            task_id='Start_Task',
            dag=dag           
    
    )
    
    read_gsheet = PythonOperator(
         task_id='reading_data_from_gsheet',
         python_callable=read_and_filter_data
         op_kwargs={},
         dag=dag
    
    )
    
    end_task = EmptyOperator(
            task_id='Start_Task',
            dag=dag           
    
    )
    
    start_task >> read_gsheet >> end_task 
    