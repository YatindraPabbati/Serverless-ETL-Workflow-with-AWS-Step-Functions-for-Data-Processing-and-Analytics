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
    logger.info("Telemetry DB Inserter Lambda function started")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")

    try:
        # Parse the input event
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event

        logger.info(f"Parsed body: {json.dumps(body, indent=2)}")

        # Extract telemetry data
        if 'telemetry' in body:
            telemetry_data = body['telemetry']
            logger.info(f"Extracted telemetry data: {json.dumps(telemetry_data, indent=2)}")
        else:
            raise ValueError("Telemetry data not found in the input")

        # Format telemetry data for insertion
        formatted_data = format_telemetry_data(telemetry_data)
        logger.info(f"Formatted telemetry data: {json.dumps(formatted_data, indent=2)}")

        # Insert data into PostgreSQL
        insert_result = insert_into_postgres(formatted_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Telemetry data processed and inserted successfully',
                'insert_result': insert_result,
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'success'
            })
        }

        logger.info(f"Processed output: {json.dumps(output, indent=2)}")
        return output

    except Exception as e:
        logger.error(f"Error processing telemetry data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'error'
            })
        }

def format_telemetry_data(telemetry_data):
    formatted_data = []
    for param in telemetry_data['params']:
        formatted_param = {
            'token': telemetry_data['token'],
            'timestamp': param['timestamp'],
            'flow_rate': param['flow_rate'],
            'discharge': param['discharge'],
            'work_hours': param['work_hours'],
            'cumulative_reverse_discharge': param['cumulative_reverse_discharge'],
            'data_count': param['data_count'],
            'cycle_slips': param['cycle_slips'],
            'no_data_count': param['no_data_count'],
            'uss': param['uss']
        }
        formatted_data.append(formatted_param)
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
        INSERT INTO telemetry_data (
            token, timestamp, flow_rate, discharge, work_hours, 
            cumulative_reverse_discharge, data_count, cycle_slips, 
            no_data_count, uss
        ) VALUES (
            %(token)s, %(timestamp)s, %(flow_rate)s, %(discharge)s, 
            %(work_hours)s, %(cumulative_reverse_discharge)s, 
            %(data_count)s, %(cycle_slips)s, %(no_data_count)s, %(uss)s
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
        "telemetry": {
            "token": "FM1037",
            "status": "ok",
            "json_ver": "v1.2",
            "params": [
                {
                    "timestamp": "2024-11-22T05:30:51.768000",
                    "flow_rate": 5.347535907830867,
                    "discharge": 36301,
                    "work_hours": 21270,
                    "cumulative_reverse_discharge": 3,
                    "data_count": 216731,
                    "cycle_slips": 50691,
                    "no_data_count": 654,
                    "uss": 52083075
                },
                {
                    "timestamp": "2024-11-22T05:31:01.768000",
                    "flow_rate": 5.5,
                    "discharge": 36350,
                    "work_hours": 21271,
                    "cumulative_reverse_discharge": 4,
                    "data_count": 216780,
                    "cycle_slips": 50700,
                    "no_data_count": 655,
                    "uss": 52083100
                }
            ]
        }
    }

    result = lambda_handler(sample_event, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")

