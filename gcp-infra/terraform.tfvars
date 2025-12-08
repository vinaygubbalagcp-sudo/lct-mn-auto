project_id        = "lvc-tc-mn-d"
bucket_name       = "lvc-tc-mn-d-bckt"
storage_class     = "STANDARD"
gcs_location      = "US"

dataset_id        = "interviews_stage"
dataset_location  = "US"
table_name        = "interviews_candidates"
location          = "US"
table_schema = <<EOF
[
  {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
  {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "email", "type": "STRING", "mode": "NULLABLE"},
  {"name": "created_date", "type": "DATE", "mode": "NULLABLE"}
]
EOF


composer_env_name = "lct-mn-auto-env-d"
composer_region   = "us-central1"
composer_sa       = "gsheet-gcs-bq@lvc-tc-mn-d.iam.gserviceaccount.com"
composer_image    = "composer-3-airflow-2.10.5-build.19"
