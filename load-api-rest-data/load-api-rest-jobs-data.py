import sys
import boto3
import json
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.session.Session()

try:
    client = session.client(service_name='secretsmanager', region_name="us-east-1")
    get_secret_value_response = client.get_secret_value(SecretId="MySqlCredentials")
    secret = json.loads(get_secret_value_response['SecretString'])
except Exceptions as e:
    logger.error("ERROR: Unexpected error: Could not get secret.")
    logger.error(e)
    return {
        'statusCode': 500,
        'body': json.dumps("ERROR: Unexpected error: Could not get secret.")
    }

try:
    conn = pymysql.connect(host=secret['host'], user=secret['user'], passwd=secret['passw'], db=secret['db'], connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    return {
        'statusCode': 500,
        'body': json.dumps("ERROR: Unexpected error: Could not connect to MySQL instance.")
    }

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def lambda_handler(event, context):
    item_count = 0
    
    if len(event) < 1 or len(event) > 1000:
        logger.error("API Service can only work from 1 up to 1000 rows")
        return {
            'statusCode': 400,
            'body': json.dumps("API Service can only work from 1 up to 1000 rows")
        }     

    for row in event:
        if 'id' not in row or 'job' not in row:
            logger.error("Record was not inserted because one key is missing")
            continue

        id = row['id']
        job = row['job']
        
        if id == "" or job == "":
            logger.error("Record with id {} was not inserted because a null value was found".format(id))
            continue

        sql_string = f"insert into JOBS (id, job) values({id}, '{job}')"
    
        with conn.cursor() as cur:
            cur.execute(sql_string)
            conn.commit()
            item_count += cur.rowcount

    conn.commit()

    return "Added %d items to RDS MySQL table" %(item_count)
    