
##############################
# importing sections
##############################

 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, trim
from google.cloud import storage
import re 

spark = SparkSession.builder.master('yarn').appName('gcstobqloadjob').getOrCreate()

##############################
# We need to validate the file
##############################

project_id = 'lvc-tc-mn-d'
bucket_name = 'lvc-tc-mn-d-bckt' 
prefix_path = 'interviews_scheduled_folder/raw/'

storage_client = storage.Client(project=project_id)
bucket = storage_client.bucket(bucket_name)
blobs = bucket.list_blobs(prefix=prefix_path)


def validation(filename):
    pattern = r"^interviews_data_\d{8}_\d{6}\.csv$"
    return bool(re.match(pattern, filename))
    
def validations():
    print("Validations start here")
    df.select("Name","Email","`Company Name `", "`Contact Number`").show()
    # df.select is equal to sql - select * from table;
    
    ###################################
    # Triming spaces in all the columns 
    ###################################
    trimmed_df = df.select([trim(col(c)).alias(c) for c in df.columns])
    # list comprehension 
    trimmed_df.show()

    ######################################
    #RENAMING ONE COLUMN OR MORE COLUMNS
    #####################################
    # data analysts - bi team 

    new_df = (df.withColumnRenamed("Timestamp", "inserted_ts")
            .withColumnRenamed("Name", "interview_candidate_name")
            .withColumnRenamed("Email", "email"))
    new_df.printSchema()
    ######################################
    #RENAMING ONE COLUMN OR MORE COLUMNS
    #####################################
    # data analysts - bi team 
    # mapping is always done with dictionaries [stm is taken place using dictionaries]
    # dicitonary 
    # for loop 

    dict_for_renaming_columns = {
    "Timestamp" : "inserted_ts",
    "Name" : "interview_candidate_name",
    "Email" : "email",
    "Contact Number" : "contact_number",
    "Interview Date" : "interview_date",
    "Interview Time" : "interview_time",
    "Interview Duration" : "interview_duration",
    "Company Name" : "company_name",
    "Interview Details" : "interview_details",
    "Email Address" : "email_address"
    }

    for old_name, new_name in dict_for_renaming_columns.items():
        new_df = trimmed_df.withTypeInfo(old_name, new_name)

    new_df.printSchema()
    ######################################################
    # WRINTING IT TO BIGQUERY
    ######################################################
    new_df.write \
    .format("bigquery") \
    .option("table",'alpine-guild-477901-v6.first_dataset.interveiws') \
    .option("temporaryGcsBucket","gs://lvc-tc-mn-d-bckt/interviews_scheduled_folder/temp") \
    .mode("append") \
    .save()

# spark has few structured apis support. These structured apis are 1. dataset, 2. dataframe and 2. sql apis 
    
    
for blob in blobs: 
    print(blob.name)
    filename = blob.name.split('/')[-1] # blob has got other functionalities/methods which leverages the application of different attributes
    if validation(filename):
        print("This is the valid file name")
        print(bucket_name)
        file_path = "gs://{0}/{1}".format(bucket_name,blob.name)
        print(file_path)
        df = spark.read.format('csv').option("header",'true').option("inferScheam","true").load(file_path)
        validations()
    else:
        print("invalid filename")
        
