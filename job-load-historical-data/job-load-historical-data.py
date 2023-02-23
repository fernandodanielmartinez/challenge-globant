import sys
import boto3
import json
import pandas as pd
import pymysql
from sqlalchemy import create_engine

entities_dict = {
    'departments': {
        'colNames': ['id', 'department'],
        'colTypes': {'id': 'Int64', 'department': object}
    },
    'hired_employees': {
        'colNames': ['id', 'name', 'datetime', 'department_id', 'job_id'],
        'colTypes': {'id': 'Int64', 'name': object, 'datetime': object, 'department_id': 'Int64', 'job_id': 'Int64'}
    },
    'jobs': {
        'colNames': ['id', 'job'],
        'colTypes': {'id': 'Int64', 'job': object}
    }
}

def read_csv_from_s3(entity, colNames, colTypes):
    s3_client = boto3.client('s3')
    s3_bucket = 's3-challenge-globant-landing'
    response = s3_client.get_object(Bucket=s3_bucket, Key='{}/{} (1) (1).csv'.format(entity, entity))
    return pd.read_csv(response.get('Body'), names=colNames, dtype=colTypes)
    
def log_rows_with_nan_in_df(df):  
    df = df[df.isna().any(axis=1)]    
    if df.size > 0:
        print('The following records have NaN values: ')
        print(df)

def drop_nan_in_df(df):  
    return df.dropna()
   
def get_secret():
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name="us-east-1")
    get_secret_value_response = client.get_secret_value(SecretId="MySqlCredentials")
    return json.loads(get_secret_value_response['SecretString'])

def set_db_conn(secret):
    return create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(secret['user'], secret['passw'], secret['host'], secret['port'], secret['db']))

def write_to_sql(df, table_name, conn):
    df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

for key in entities_dict:
    entity_df = read_csv_from_s3(key, entities_dict[key]['colNames'], entities_dict[key]['colTypes'])
    log_rows_with_nan_in_df(entity_df)
    entity_df = drop_nan_in_df(entity_df)
    secret = get_secret()
    mydb = set_db_conn(secret)
    write_to_sql(entity_df, key.upper(), mydb)
