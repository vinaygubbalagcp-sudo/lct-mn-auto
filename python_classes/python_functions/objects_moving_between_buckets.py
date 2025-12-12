
from google.cloud import storage 

source_bucket_name = 'lvc-tc-mn-d-bckt'
target_bucket_name = 'lct-dp-jobs-bucket-dev' 
blob_name = 'main_dag.py'
source_storage_client = storage.Client(project='lvc-tc-mn-d')
target_storage_client = storage.Client(project='alpine-guild-477901-v6')

source_bucket = source_storage_client.bucket(source_bucket_name)

target_bucket = target_storage_client.bucket(target_bucket_name)
# selecting the required method out of all the methods with a bucket 
# selecting the
# selecting the blob 
blob = source_bucket.blob(blob_name)
# copying the blob from source bucket to target bucket
blob_copy = source_bucket.copy_blob(
        blob, target_bucket, blob_name
    )
source_bucket.delete_blob(blob_name)