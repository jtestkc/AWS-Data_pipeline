import awswrangler as wr
import pandas as pd
import json
import os

# Temporary environment variables for testing/reference
# os.environ['s3_cleansed_layer'] = 's3://youtube-cleansed-layer-project'
# os.environ['glue_catalog_db_name'] = 'db_youtube_cleaned'
# os.environ['glue_catalog_table_name'] = 'cleaned_statistics_reference_data'
# os.environ['write_data_operation'] = 'append'

def lambda_handler(event, context):
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Read JSON file from S3
        df_raw = wr.s3.read_json(path=f"s3://{bucket}/{key}")

        # Extract items from JSON (YouTube category structure)
        df_step_1 = pd.json_normalize(df_raw['items'])

        # Write to S3 in Parquet format
        res = wr.s3.to_parquet(
            df=df_step_1,
            path=os.environ['s3_cleansed_layer'],
            dataset=True,
            database=os.environ['glue_catalog_db_name'],
            table=os.environ['glue_catalog_table_name'],
            mode=os.environ['write_data_operation']
        )

        return res
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}.'.format(key, bucket))
        raise e
