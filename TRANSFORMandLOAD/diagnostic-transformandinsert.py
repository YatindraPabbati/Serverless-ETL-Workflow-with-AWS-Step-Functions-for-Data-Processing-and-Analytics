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
    logger.info("Diagnostic Data DB Inserter Lambda function started")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")

    try:
        # Parse the input event
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event

        logger.info(f"Parsed body: {json.dumps(body, indent=2)}")

        # Extract diagnostic data
        if 'diagnostic' in body:
            diagnostic_data = body['diagnostic']
            logger.info(f"Extracted diagnostic data: {json.dumps(diagnostic_data, indent=2)}")
        else:
            raise ValueError("Diagnostic data not found in the input")

        # Format diagnostic data for insertion
        formatted_data = format_diagnostic_data(diagnostic_data)
        logger.info(f"Formatted diagnostic data: {json.dumps(formatted_data, indent=2)}")

        # Insert data into PostgreSQL
        insert_result = insert_into_postgres(formatted_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Diagnostic data processed and inserted successfully',
                'insert_result': insert_result,
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'success'
            })
        }

        logger.info(f"Processed output: {json.dumps(output, indent=2)}")
        return output

    except Exception as e:
        logger.error(f"Error processing diagnostic data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'error'
            })
        }

def format_diagnostic_data(diagnostic_data):
    formatted_data = {
        'token': diagnostic_data['token'],
        'status': diagnostic_data['status'],
        'json_ver': diagnostic_data['json_ver'],
        'timestamp': diagnostic_data['timestamp'],
        'diagnosParam': json.dumps(diagnostic_data['diagnosParam']),
        'commParam': json.dumps(diagnostic_data['commParam']),
        'storedDiagParams': json.dumps(diagnostic_data['storedDiagParams'])
    }
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
        INSERT INTO diagnostic_data (
            token, status, json_ver, timestamp, diagnos_param, comm_param, stored_diag_params
        ) VALUES (
            %(token)s, %(status)s, %(json_ver)s, %(timestamp)s, %(diagnosParam)s, %(commParam)s, %(storedDiagParams)s
        )
        """

        # Execute the insertion
        cur.execute(insert_query, data)

        # Commit the transaction
        conn.commit()

        logger.info("Successfully inserted diagnostic data into the database")
        return "Inserted 1 record"

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
        "diagnostic": {
            "token": "FM5519",
            "status": "ok",
            "json_ver": "v1.4",
            "timestamp": "2024-11-22T09:36:05.616000",
            "diagnosParam": {
                "RSSI": -67,
                "ttc": 4272,
                "simId": 1,
                "vBatNoLoad": 331,
                "vBatonLoad": 328,
                "vSuperCap": 305
            },
            "commParam": {
                "pppTime": 34,
                "ntpTime": 50,
                "serverCmdsTime": 3
            },
            "storedDiagParams": {
                "param1": {
                    "reason": "err-server-con",
                    "pppTime": 19,
                    "serverTime": 120,
                    "simId": 2,
                    "RSSI": -72,
                    "vBatNoLoad": 339,
                    "vBatonLoad": 339,
                    "vSuperCap": 343
                },
                "param2": {
                    "reason": "err-server-con",
                    "pppTime": 43,
                    "serverTime": 120,
                    "simId": 1,
                    "RSSI": -103,
                    "vBatNoLoad": 325,
                    "vBatonLoad": 344,
                    "vSuperCap": 311
                },
                "param3": {
                    "reason": "err-server-con",
                    "pppTime": 49,
                    "serverTime": 120,
                    "simId": 1,
                    "RSSI": -74,
                    "vBatNoLoad": 335,
                    "vBatonLoad": 348,
                    "vSuperCap": 325
                }
            }
        }
    }

    result = lambda_handler(sample_event, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")

