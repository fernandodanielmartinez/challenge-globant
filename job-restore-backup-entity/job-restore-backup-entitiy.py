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

entity = 'departments'

def read_from_s3(entity):
    s3_bucket = 's3-challenge-globant-backup'
    try:
        return spark.read \
                    .format('avro') \
                    .load('s3a://{}/{}/'.format(s3_bucket, entity))
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not read from s3.")
        logger.error(e)
    
def get_secret():
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name="us-east-1")
        get_secret_value_response = client.get_secret_value(SecretId="MySqlCredentials")
        return json.loads(get_secret_value_response['SecretString'])
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not get secret.")
        logger.error(e)

def write_data_to_sql(entity, secret, df):
    try:
        df.write \
          .format("jdbc") \
          .option("driver", 'com.mysql.cj.jdbc.Driver') \
          .option("url", 'jdbc:mysql://{}:{}/{}'.format(secret['host'], secret['port'], secret['db'])) \
          .option("dbtable", entity.upper()) \
          .option("user", secret['user']) \
          .option("password", secret['passw']) \
          .mode("overwrite") \
          .save()
        logger.info("Backup on table {} restored".format(entity.upper()))
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not write data to sql.")
        logger.error(e)
                
    
job.init(args['JOB_NAME'], args)
    
entity_df = read_from_s3(entity)
secret = get_secret()
write_data_to_sql(entity, secret, entity_df)

job.commit()