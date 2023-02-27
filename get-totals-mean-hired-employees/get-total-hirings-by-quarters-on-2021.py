import sys
import boto3
import json
import logging
import pymysql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.session.Session()

def lambda_handler(event, context):

    try:
        client = session.client(service_name='secretsmanager', region_name="us-east-1")
        get_secret_value_response = client.get_secret_value(SecretId="MySqlCredentials")
        secret = json.loads(get_secret_value_response['SecretString'])
    except Exception as e:
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

    sql_string = """
      SELECT d.department
           , j.job
           , SUM(CASE WHEN MONTH(he.`datetime`) BETWEEN '01' AND '03' THEN 1 ELSE 0 END) AS q1
           , SUM(CASE WHEN MONTH(he.`datetime`) BETWEEN '04' AND '06' THEN 1 ELSE 0 END) AS q2
           , SUM(CASE WHEN MONTH(he.`datetime`) BETWEEN '07' AND '09' THEN 1 ELSE 0 END) AS q3
           , SUM(CASE WHEN MONTH(he.`datetime`) BETWEEN '10' AND '12' THEN 1 ELSE 0 END) AS q4
        FROM HIRED_EMPLOYEES he 
        JOIN DEPARTMENTS d ON he.department_id = d.id
        JOIN JOBS j ON he.job_id = j.id    
       WHERE YEAR(he.`datetime`) = '2021'
    GROUP BY d.department
           , j.job     
    ORDER BY d.department
           , j.job 
    """
    
    with conn.cursor() as cur:
        cur.execute(sql_string)
        rows = cur.fetchall()
        conn.commit()

    return rows
    
