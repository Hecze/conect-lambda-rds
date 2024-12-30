import boto3
import json
import os
from database_utils import SQLServerManager
model_id = "anthropic.claude-3-haiku-20240307-v1:0"

def lambda_handler(event, context):
    try:
        # Llamado a RDS
        RDS_HOST = "rds-chatbot.c1qqeeye47hk.us-east-1.rds.amazonaws.com"
        RDS_USER = "admin"
        RDS_PASSWORD = "adminadmin"
        RDS_DATABASE = "PRODUCCION"

        db_manager = SQLServerManager(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DATABASE
        )
        db_manager.connect()
        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_CATALOG = 'PRODUCCION'
        """
        resultados = db_manager.execute_query(query)
        db_manager.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Consulta ejecutada correctamente',
                'results': resultados 
            })
        }        

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error al conectarse con Amazon Bedrock',
                'error': str(e)
            })
        }
