# i need to connect to secret manger to get my credentials to coonect to google sheet and create a pandas dadataframe
import datetime
from google.cloud import secretmanager
import google_crc32c
import json
import pandas as pd
from datetime import datetime 
import gspread  
import smtplib
from google.cloud import bigquery
from google.oauth2.service_account import Credentials 

# -------------------------------------------------
# GOOGLE SHEET DETAILS
# -------------------------------------------------
SHEET_ID = "18LJ0dh7W8u698vPsB6Ew6xOgDONPynYcYE0SU6UzbiU"

GIVERS_WORKSHEET = "Candidate_Availability"
TAKERS_WORKSHEET = "Panel_Availability"

# -------------------------------------------------
# BIGQUERY CONFIG
# -------------------------------------------------
PROJECT_ID = "lvc-tc-mn-d"  # your GCP project
DATASET_ID = "interviews_stage"    # your BigQuery dataset
TABLE_ID = "scheduled_interviews"    # target table

# -------------------------------------------------
# EMAIL CONFIG
# -------------------------------------------------
# SENDER_EMAIL = "letsmailrahul27@gmail.com"
# SENDER_PASSWORD = "pkpd oqsi sfjj iwyo"

def get_values_in_secrets(project_id, secret_id, version_id="latest"):
    # Create the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build secret version name
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)

    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise ValueError("Data corruption detected")

    # Decode secret payload
    payload = response.payload.data.decode("UTF-8") 
    return payload

def get_service_account_email(project_id, secret_id, version_id="latest"):
    # Create the Secret Manager client
    client = secretmanager.SecretManagerServiceClient()

    # Build secret version name
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret
    response = client.access_secret_version(request={"name": name})

    # Verify payload checksum
    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)

    if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
        raise ValueError("Data corruption detected")

    # Decode secret payload
    payload = response.payload.data.decode("UTF-8")

    # Convert JSON string to dict
    sa_json = json.loads(payload)

    # I dont require email printing i want to connect to google sheet using this json   
    
    sa_email = sa_json.get("client_email")

    print("Service Account Email ID:", sa_email)

    return sa_json
# i need to create dataframes for two google sheets using this secret manager credentials
# give the code to connect to google sheet and create pandas dataframe using this secret manager credentials
def get_dataframe_from_google_sheet(project_id, secret_id, sheet_id, worksheet_name):
    # Get service account credentials from Secret Manager
    sa_json = get_service_account_email(project_id, secret_id)

    # Create Credentials object
    creds = Credentials.from_service_account_info(
        sa_json,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    # Authorize gspread client
    client = gspread.authorize(creds)

    # Open the Google Sheet by ID and select the worksheet
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    # Get all records and convert to DataFrame
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    return df
# adjust the code as per the following function call 
# -------------------------------------------------
# READ & FILTER DATA
# -------------------------------------------------
def read_and_filter_data(project_id, secret_id, SHEET_ID, GIVERS_WORKSHEET, TAKERS_WORKSHEET):
    df1 = get_dataframe_from_google_sheet(project_id, secret_id, SHEET_ID, GIVERS_WORKSHEET)
    df2 = get_dataframe_from_google_sheet(project_id, secret_id, SHEET_ID, TAKERS_WORKSHEET)

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

# -------------------------------------------------
# MERGE
# -------------------------------------------------
def mergefiltered_data(df1, df2):
    return pd.merge(
        df1,
        df2,
        on=['date', 'time'],
        how='inner',
        suffixes=('_giver', '_taker')
    )
 
# -------------------------------------------------
# EMAIL NOTIFICATION
# -------------------------------------------------
def send_email_notifications(merged_df):
    project_id = "175579495168" 
    SENDER_EMAIL = get_values_in_secrets(project_id, "SENDER_EMAIL")
    SENDER_PASSWORD = get_values_in_secrets(project_id, "SENDER_PASSWORD")
    if merged_df.empty:
        print("No matching records found.")
        return 

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(SENDER_EMAIL, SENDER_PASSWORD)

    try:
        for _, row in merged_df.iterrows():
            subject = "Matching Interview Slot Found"

            # Email body for Taker
            taker_body = f"""
Dear {row['name_taker']},

You have a matching interview slot on {row['date']} at {row['time']}.

GIVER:
Name  : {row['name_giver']}
Email : {row['email']}
Phone : {row['phonenumber']}

Your Details:
Name  : {row['name_taker']}
Email : {row['email_id']}
Phone : {row.get('phonenumber_taker', 'N/A')}

Technical Skillset : {row.get('technicalskillset', 'N/A')}
Remarks (if any)   : {row.get('remarks_giver', '')} / {row.get('remarks_taker', '')}
"""

            # Email body for Giver
            giver_body = f"""
Dear {row['name_giver']},

You have a matching interview slot on {row['date']} at {row['time']}.

TAKER:
Name  : {row['name_taker']}
Email : {row['email_id']}
Phone : {row.get('phonenumber_taker', 'N/A')}

Your Details:
Name  : {row['name_giver']}
Email : {row['email']}
Phone : {row['phonenumber']}

Technical Skillset : {row.get('technicalskillset', 'N/A')}
Remarks (if any)   : {row.get('remarks_giver', '')} / {row.get('remarks_taker', '')}
"""

            # Combine subject with body
            taker_message = f"Subject: {subject}\n\n{taker_body}"
            giver_message = f"Subject: {subject}\n\n{giver_body}"

            # Send emails
            server.sendmail(SENDER_EMAIL, row['email'], giver_message)
            server.sendmail(SENDER_EMAIL, row['email_id'], taker_message)

    finally:
        server.quit()

    print("Emails sent successfully.")



def insert_into_bigquery(df):
    if df.empty:
        print("No data to insert into BigQuery.")
        return

    client = bigquery.Client(project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Convert dataframe columns to match BigQuery table if needed
    # Example: ensure date column is datetime.date
    df_to_insert = df.copy()
    df_to_insert['date'] = pd.to_datetime(df_to_insert['date'])

    job = client.load_table_from_dataframe(df_to_insert, table_ref)
    job.result()  # wait for completion

    print(f"{len(df)} rows inserted into BigQuery table {table_ref} successfully.")

project_id = "175579495168"
secret_id = "interviews_secret_sa"
givers, takers = read_and_filter_data(project_id, secret_id, SHEET_ID, GIVERS_WORKSHEET, TAKERS_WORKSHEET)
merged_df = mergefiltered_data(givers, takers)
send_email_notifications(merged_df)
insert_into_bigquery(merged_df)