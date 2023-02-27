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
      SELECT he.department_id AS id
           , d.department 
           , COUNT(1) AS hired
        FROM HIRED_EMPLOYEES he 
        JOIN DEPARTMENTS d ON he.department_id = d.id   
       WHERE YEAR(he.`datetime`) = '2021'
    GROUP BY he.department_id
           , d.department   
      HAVING COUNT(1) > ( SELECT ROUND(AVG(hec.count), 0) avg
                            FROM ( SELECT COUNT(1) count
                                        , he.department_id 
             		                 FROM HIRED_EMPLOYEES he 
                                    WHERE YEAR(he.`datetime`) = '2021'
                                 GROUP BY he.department_id ) hec )
    ORDER BY COUNT(1) DESC 
    """
    
    with conn.cursor() as cur:
        cur.execute(sql_string)
        rows = cur.fetchall()
        conn.commit()

    return rows
    