import sys
import boto3
import json
import logging
import pandas as pd
import pymysql

from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    try:
        s3_client = boto3.client('s3')
        s3_bucket = 's3-challenge-globant-landing'
        response = s3_client.get_object(Bucket=s3_bucket, Key='{}/{} (1) (1).csv'.format(entity, entity))
        return pd.read_csv(response.get('Body'), names=colNames, dtype=colTypes)
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not read from s3.")
        logger.error(e)
    
def log_rows_with_nan_in_df(df):  
    try:
        df = df[df.isna().any(axis=1)]    
        if df.size > 0:
            print('The following records have NaN values: ')
            print(df)
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not logs rows with NaNs.")
        logger.error(e)

def drop_nan_in_df(df):  
    try:
        return df.dropna()
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not format df.")
        logger.error(e)
   
def get_secret():
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name="us-east-1")
        get_secret_value_response = client.get_secret_value(SecretId="MySqlCredentials")
        return json.loads(get_secret_value_response['SecretString'])
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not get secret.")
        logger.error(e)

def set_db_conn(secret):
    try:
        return create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(secret['user'], secret['passw'], secret['host'], secret['port'], secret['db']))
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not create engine.")
        logger.error(e)

def write_to_sql(df, table_name, conn):
    try:
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not write to sql.")
        logger.error(e)

for key in entities_dict:
    entity_df = read_csv_from_s3(key, entities_dict[key]['colNames'], entities_dict[key]['colTypes'])
    log_rows_with_nan_in_df(entity_df)
    entity_df = drop_nan_in_df(entity_df)
    secret = get_secret()
    mydb = set_db_conn(secret)
    write_to_sql(entity_df, key.upper(), mydb)
