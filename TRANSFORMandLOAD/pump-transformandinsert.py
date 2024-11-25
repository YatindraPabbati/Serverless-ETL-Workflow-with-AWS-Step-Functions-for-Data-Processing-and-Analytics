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
    logger.info("Pump Data DB Inserter Lambda function started")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")

    try:
        # Parse the input event
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event

        logger.info(f"Parsed body: {json.dumps(body, indent=2)}")

        # Extract pump data
        if 'pump' in body:
            pump_data = body['pump']
            logger.info(f"Extracted pump data: {json.dumps(pump_data, indent=2)}")
        else:
            raise ValueError("Pump data not found in the input")

        # Format pump data for insertion
        formatted_data = format_pump_data(pump_data)
        logger.info(f"Formatted pump data: {json.dumps(formatted_data, indent=2)}")

        # Insert data into PostgreSQL
        insert_result = insert_into_postgres(formatted_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Pump data processed and inserted successfully',
                'insert_result': insert_result,
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'success'
            })
        }

        logger.info(f"Processed output: {json.dumps(output, indent=2)}")
        return output

    except Exception as e:
        logger.error(f"Error processing pump data: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'error'
            })
        }

def format_pump_data(pump_data):
    formatted_data = []
    for param in pump_data['params']:
        formatted_param = {
            'token': pump_data['token'],
            'pump_start_time': param['pump_start_time'],
            'start_discharge': param['start_discharge'],
            'start_data': param['start_data'],
            'start_no_data': param['start_no_data'],
            'start_cycle_slips': param['start_cycle_slips'],
            'pump_stop_time': param['pump_stop_time'],
            'stop_discharge': param['stop_discharge'],
            'stop_data': param['stop_data'],
            'stop_no_data': param['stop_no_data'],
            'stop_cycle_slips': param['stop_cycle_slips'],
            'pump_duration_seconds': param['pump_duration_seconds'],
            'discharge_difference': param['discharge_difference'],
            'data_difference': param['data_difference'],
            'no_data_difference': param['no_data_difference'],
            'cycle_slips_difference': param['cycle_slips_difference']
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
        INSERT INTO pump_data (
            token, pump_start_time, start_discharge, start_data, start_no_data,
            start_cycle_slips, pump_stop_time, stop_discharge, stop_data,
            stop_no_data, stop_cycle_slips, pump_duration_seconds,
            discharge_difference, data_difference, no_data_difference,
            cycle_slips_difference
        ) VALUES (
            %(token)s, %(pump_start_time)s, %(start_discharge)s, %(start_data)s,
            %(start_no_data)s, %(start_cycle_slips)s, %(pump_stop_time)s,
            %(stop_discharge)s, %(stop_data)s, %(stop_no_data)s,
            %(stop_cycle_slips)s, %(pump_duration_seconds)s,
            %(discharge_difference)s, %(data_difference)s,
            %(no_data_difference)s, %(cycle_slips_difference)s
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
        "pump": {
            "token": "FM3278",
            "status": "ok",
            "json_ver": "v1.4",
            "params": [
                {
                    "pump_start_time": "2024-11-22T09:36:05.616000",
                    "start_discharge": 19769173,
                    "start_data": 5980,
                    "start_no_data": 1507,
                    "start_cycle_slips": 352,
                    "pump_stop_time": "2024-11-22T09:36:10.616000",
                    "stop_discharge": 19290454,
                    "stop_data": 24970,
                    "stop_no_data": 4903,
                    "stop_cycle_slips": 1278,
                    "pump_duration_seconds": 5.0,
                    "discharge_difference": -478719,
                    "data_difference": 18990,
                    "no_data_difference": 3396,
                    "cycle_slips_difference": 926
                }
            ]
        }
    }

    result = lambda_handler(sample_event, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")

