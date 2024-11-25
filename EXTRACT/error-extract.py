import json
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Error Data Extractor Lambda function started")
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

        # Extract error data
        if 'error' in processed_data:
            error_data = processed_data['error']
            logger.info(f"Extracted error data: {json.dumps(error_data, indent=2)}")
        else:
            raise ValueError("Error data not found in the input")

        # Process error data
        processed_error = process_error(error_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'error': processed_error,
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

def process_error(error_data):
    logger.info("Processing error data")

    processed_data = {
        'token': error_data.get('token'),
        'status': error_data.get('status'),
        'json_ver': error_data.get('json-ver'),
        'errors': []
    }

    if 'mspErrParam' in error_data and isinstance(error_data['mspErrParam'], list):
        for param in error_data['mspErrParam']:
            processed_error = {
                'timestamp': datetime.fromtimestamp(param['ts'] / 1000).isoformat(),
                'error_code': param['err-code'],
                'error_description': get_error_description(param['err-code'])
            }
            processed_data['errors'].append(processed_error)
            logger.info(f"Processed error parameter: {json.dumps(processed_error, indent=2)}")
    else:
        logger.warning("No mspErrParam found in error data or invalid format")

    return processed_data

def get_error_description(error_code):
    # This is a placeholder function. In a real-world scenario, you would have a more comprehensive
    # mapping of error codes to descriptions.
    error_descriptions = {
        127: "General system error",
        175: "Communication failure",
        # Add more error codes and descriptions as needed
    }
    return error_descriptions.get(error_code, "Unknown error")

# For local testing
if __name__ == "__main__":
    # Sample input event
    sample_event = {
        "statusCode": 200,
        "body": json.dumps({
            "processed_data": {
                "error": {
                    "token": "FM4568",
                    "status": "ok",
                    "json-ver": "v1.4",
                    "mspErrParam": [
                        {
                            "ts": 1732268165616,
                            "err-code": 175
                        }
                    ]
                }
            },
            "timestamp": "2024-11-22T10:38:16.584613",
            "status": "success"
        })
    }

    result = lambda_handler(sample_event, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")

