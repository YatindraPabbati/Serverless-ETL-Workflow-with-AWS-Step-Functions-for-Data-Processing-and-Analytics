import json
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Pump Data Extractor Lambda function started")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")

    try:
        # Parse the input event
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event

        logger.info(f"Parsed body: {json.dumps(body, indent=2)}")

        # Extract processed_data
        if 'processed_data' in body:
            processed_data = body['processed_data']
        else:
            processed_data = body

        # Extract pump data
        if 'pump' in processed_data:
            pump_data = processed_data['pump']
            logger.info(f"Extracted pump data: {json.dumps(pump_data, indent=2)}")
        else:
            raise ValueError("Pump data not found in the input")

        # Process pump data
        processed_pump = process_pump(pump_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'pump': processed_pump,
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

def process_pump(pump_data):
    logger.info("Processing pump data")

    processed_data = {
        'token': pump_data.get('token'),
        'status': pump_data.get('status'),
        'json_ver': pump_data.get('json-ver'),
        'params': []
    }

    if 'pumpParam' in pump_data and isinstance(pump_data['pumpParam'], list):
        for param in pump_data['pumpParam']:
            processed_param = {
                'pump_start_time': datetime.fromtimestamp(param['PumpStartTs'] / 1000).isoformat(),
                'start_discharge': param['Startdischarge'],
                'start_data': param['StartData'],
                'start_no_data': param['StartNoData'],
                'start_cycle_slips': param['StartCycleSlips'],
                'pump_stop_time': datetime.fromtimestamp(param['PumpStoptTs'] / 1000).isoformat(),
                'stop_discharge': param['Stopdischarge'],
                'stop_data': param['StopData'],
                'stop_no_data': param['StopNoData'],
                'stop_cycle_slips': param['StopCycleSlips'],
                'pump_duration_seconds': (param['PumpStoptTs'] - param['PumpStartTs']) / 1000,
                'discharge_difference': param['Stopdischarge'] - param['Startdischarge'],
                'data_difference': param['StopData'] - param['StartData'],
                'no_data_difference': param['StopNoData'] - param['StartNoData'],
                'cycle_slips_difference': param['StopCycleSlips'] - param['StartCycleSlips']
            }
            processed_data['params'].append(processed_param)
            logger.info(f"Processed pump parameter: {json.dumps(processed_param, indent=2)}")
    else:
        logger.warning("No pumpParam found in pump data or invalid format")

    return processed_data

# For local testing
if __name__ == "__main__":
    # Sample input event
    sample_event = {
        "statusCode": 200,
        "body": json.dumps({
            "processed_data": {
                "pump": {
                    "token": "FM3278",
                    "status": "ok",
                    "json-ver": "v1.4",
                    "pumpParam": [
                        {
                            "PumpStartTs": 1732253451768,
                            "Startdischarge": 19774947,
                            "StartData": 13653,
                            "StartNoData": 1671,
                            "StartCycleSlips": 166,
                            "PumpStoptTs": 1732253456768,
                            "Stopdischarge": 19474803,
                            "StopData": 34467,
                            "StopNoData": 2125,
                            "StopCycleSlips": 1481
                        }
                    ]
                }
            },
            "timestamp": "2023-05-22T12:34:56.789Z",
            "status": "success"
        })
    }

    result = lambda_handler(sample_event, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")

