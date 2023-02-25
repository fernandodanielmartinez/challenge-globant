import sys
import boto3
import json
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

entities_dict = ['departments', 'hired_employees', 'jobs']

def get_secret():
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name="us-east-1")
        get_secret_value_response = client.get_secret_value(SecretId="MySqlCredentials")
        return json.loads(get_secret_value_response['SecretString'])
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not get secret.")
        logger.error(e)

def read_data_from_sql(entity, secret):
    try:
        return spark.read \
                    .format("jdbc") \
                    .option("driver", 'com.mysql.cj.jdbc.Driver') \
                    .option("url", 'jdbc:mysql://{}:{}/{}'.format(secret['host'], secret['port'], secret['db'])) \
                    .option("dbtable", entity.upper()) \
                    .option("user", secret['user']) \
                    .option("password", secret['passw']) \
                    .load()
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not get data from sql.")
        logger.error(e)
    
def write_to_s3(entity, df):
    s3_bucket = 's3-challenge-globant-backup'
    try:
        df.write \
          .format('avro') \
          .save('s3a://{}/{}/'.format(s3_bucket, entity))
    except Exceptions as e:
        logger.error("ERROR: Unexpected error: Could not write to s3.")
        logger.error(e)
    
secret = get_secret()

for key in entities_dict:
    job.init(args['JOB_NAME'], args)
    
    entity_df = read_data_from_sql(key, secret)
    write_to_s3(key, entity_df)

    job.commit()