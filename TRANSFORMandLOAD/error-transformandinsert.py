import json
import logging
import os
import psycopg2
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Database connection parameters
DB_HOST = '54.147.228.91'
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'postgresql'

def lambda_handler(event, context):
    logger.info("Error Data DB Inserter Lambda function started")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")

    try:
        # Parse the input event
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event

        logger.info(f"Parsed body: {json.dumps(body, indent=2)}")

        # Extract error data
        if 'error' in body:
            error_data = body['error']
            logger.info(f"Extracted error data: {json.dumps(error_data, indent=2)}")
        else:
            raise ValueError("Error data not found in the input")

        # Format error data for insertion
        formatted_data = format_error_data(error_data)
        logger.info(f"Formatted error data: {json.dumps(formatted_data, indent=2)}")

        # Insert data into PostgreSQL
        insert_result = insert_into_postgres(formatted_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Error data processed and inserted successfully',
                'insert_result': insert_result,
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'success'
            })
        }

        logger.info(f"Processed output: {json.dumps(output, indent=2)}")
        return output

    except Exception as e:
        logger.error(f"Error processing error data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'error'
            })
        }

def format_error_data(error_data):
    formatted_data = []
    for error in error_data['errors']:
        formatted_error = {
            'token': error_data['token'],
            'status': error_data['status'],
            'json_ver': error_data['json_ver'],
            'timestamp': error['timestamp'],
            'error_code': error['error_code'],
            'error_description': error['error_description']
        }
        formatted_data.append(formatted_error)
    return formatted_data

def insert_into_postgres(data):
    conn = None
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        # SQL query for insertion
        insert_query = """
        INSERT INTO error_data (
            token, status, json_ver, timestamp, error_code, error_description
        ) VALUES (
            %(token)s, %(status)s, %(json_ver)s, %(timestamp)s, %(error_code)s, %(error_description)s
        )
        """

        # Execute the insertion for each data point
        for item in data:
            cur.execute(insert_query, item)

        # Commit the transaction
        conn.commit()

        logger.info(f"Successfully inserted {len(data)} records into the database")
        return f"Inserted {len(data)} records"

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error inserting data into PostgreSQL: {error}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            cur.close()
            conn.close()

# For local testing
if __name__ == "__main__":
    # Sample input event
    sample_event = {
        "error": {
            "token": "FM4568",
            "status": "ok",
            "json_ver": "v1.4",
            "errors": [
                {
                    "timestamp": "2024-11-22T09:36:05.616000",
                    "error_code": 175,
                    "error_description": "Communication failure"
                }
            ]
        }
    }

    result = lambda_handler(sample_event, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")

