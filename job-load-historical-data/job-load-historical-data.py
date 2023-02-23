import sys
import pandas as pd
import boto3

s3_client = boto3.client('s3')

s3_bucket = 's3-challenge-globant-landing'

entities_dict = {
    'departments': {
        'schema': ['id', 'department']
    },
    'hired_employees': {
        'schema': ['id', 'name', 'datetime', 'department_id', 'job_id']
    },
    'jobs': {
        'schema': ['id', 'job']
    }
}

def read_csv_from_s3(entity, schema):
    response = s3_client.get_object(Bucket=s3_bucket, Key='{}/{} (1) (1).csv'.format(entity, entity))
    return pd.read_csv(response.get('Body'), names=schema)
    
#def format_df(df):  
    
    
for key in entities_dict:
    entity_df = read_csv_from_s3(key, entities_dict[key]['schema'])
#    entity_df = format_df(df)
    
