source_to_gcs_etl.py:
----------------------
    --> imports --> requirement.txt install (pip install)
    --> creadintials missing (there is no creadintials file)
    --> df = extract_data_from_google_sheets(SHEET_URL, SHEET_NAME, CREDS_JSON)
        json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
        
    --> I am raising a ticket to the infram team asking for json credintial file 
    
Subject: Request for JSON Service Account Credential File for GCP Access

Hi Team, 

I would like to request the GCP Service Account JSON credential file required for my development activities. This credential will be used to authenticate from my local machine/VS Code for accessing the required GCP resources (primarily GCS / BigQuery).

Request details:

Purpose: Development and testing

Required Access: GCS Object Admin (or the required role)

Format Needed: Service Account JSON key file

Please share the JSON credential file or the secure download link.
Let me know if you need any additional information from my end.

Thank you for your support.

Regards

PLN for LavusTech

    -->   File "C:\lct-dm-p\lct-mn-auto\myenv\lib\site-packages\gspread\utils.py", line 622, in extract_id_from_url
    raise NoValidUrlKeyFound
gspread.exceptions.NoValidUrlKeyFound
        <> We need ask the source team to enable api of the gdrive and gsheets 
        <> We need to give editor access to the email given in the service account json file 
    -->   FutureWarning: You are using a Python version (3.10.0) which Google will stop supporting in new releases of google.api_core once it reaches its end of life (2026-10-04). Please upgrade to the latest Python version, or at least Python 3.11, to continue receiving updates for google.api_core past that date.
    
    -->
    google.cloud.storage.exceptions.InvalidResponse: ('Request failed with status code', 403, 'Expected one of', <HTTPStatus.OK: 200>)

        During handling of the above exception, another exception occurred:
        
        Traceback (most recent call last):
        File "C:\lct-dm-p\lct-mn-auto\gcp-source\ETL_Pipeline\source_to_gcs_etl.py", line 60, in <module>
            load_data_to_gcs(df, BUCKET_NAME, DESTINATION_BLOB_NAME, CREDS_JSON)