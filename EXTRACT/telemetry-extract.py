import json
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Telemetry Extractor Lambda function started")
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

        # Extract telemetry data
        if 'telemetry' in processed_data:
            telemetry_data = processed_data['telemetry']
            logger.info(f"Extracted telemetry data: {json.dumps(telemetry_data, indent=2)}")
        else:
            raise ValueError("Telemetry data not found in the input")

        # Process telemetry data
        processed_telemetry = process_telemetry(telemetry_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'telemetry': processed_telemetry,
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

def process_telemetry(telemetry_data):
    logger.info("Processing telemetry data")

    processed_data = {
        'token': telemetry_data.get('token'),
        'status': telemetry_data.get('status'),
        'json_ver': telemetry_data.get('json-ver'),
        'params': []
    }

    if 'teleParam' in telemetry_data and isinstance(telemetry_data['teleParam'], list):
        for param in telemetry_data['teleParam']:
            processed_param = {
                'timestamp': datetime.fromtimestamp(param['ts'] / 1000).isoformat(),
                'flow_rate': param['flowRate'],
                'discharge': param['discharge'],
                'work_hours': param['workHour'],
                'cumulative_reverse_discharge': param['cummRevDisch'],
                'data_count': param['Data'],
                'cycle_slips': param['CycleSlips'],
                'no_data_count': param['NoData'],
                'uss': param['USS']
            }
            processed_data['params'].append(processed_param)
            logger.info(f"Processed telemetry parameter: {json.dumps(processed_param, indent=2)}")
    else:
        logger.warning("No teleParam found in telemetry data or invalid format")

    return processed_data

# For local testing
if __name__ == "__main__":
    # Sample input event
    sample_event = {
        "statusCode": 200,
        "body": json.dumps({
            "processed_data": {
                "telemetry": {
                    "token": "FM1037",
                    "status": "ok",
                    "json-ver": "v1.2",
                    "teleParam": [
                        {
                            "ts": 1732253451768,
                            "flowRate": 5.347535907830867,
                            "discharge": 36301,
                            "workHour": 21270,
                            "cummRevDisch": 3,
                            "Data": 216731,
                            "CycleSlips": 50691,
                            "NoData": 654,
                            "USS": 52083075
                        },
                        {
                            "ts": 1732253461768,
                            "flowRate": 5.5,
                            "discharge": 36350,
                            "workHour": 21271,
                            "cummRevDisch": 4,
                            "Data": 216780,
                            "CycleSlips": 50700,
                            "NoData": 655,
                            "USS": 52083100
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

