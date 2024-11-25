import json
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Diagnostic Data Extractor Lambda function started")
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

        # Extract diagnostic data
        if 'diagnostic' in processed_data:
            diagnostic_data = processed_data['diagnostic']
            logger.info(f"Extracted diagnostic data: {json.dumps(diagnostic_data, indent=2)}")
        else:
            raise ValueError("Diagnostic data not found in the input")

        # Process diagnostic data
        processed_diagnostic = process_diagnostic(diagnostic_data)

        # Prepare the output
        output = {
            'statusCode': 200,
            'body': json.dumps({
                'diagnostic': processed_diagnostic,
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

def process_diagnostic(diagnostic_data):
    logger.info("Processing diagnostic data")

    processed_data = {
        'token': diagnostic_data.get('token'),
        'status': diagnostic_data.get('status'),
        'json_ver': diagnostic_data.get('json-ver'),
        'timestamp': datetime.fromtimestamp(diagnostic_data['ts'] / 1000).isoformat(),
        'diagnosParam': process_diagnos_param(diagnostic_data.get('diagnosParam', {})),
        'commParam': process_comm_param(diagnostic_data.get('commParam', {})),
        'storedDiagParams': process_stored_diag_params(diagnostic_data.get('storedDiagParams', {}))
    }

    logger.info(f"Processed diagnostic data: {json.dumps(processed_data, indent=2)}")
    return processed_data

def process_diagnos_param(diagnos_param):
    logger.info("Processing diagnosParam")
    processed = {
        'RSSI': diagnos_param.get('RSSI'),
        'ttc': diagnos_param.get('ttc'),
        'simId': diagnos_param.get('simId'),
        'vBatNoLoad': diagnos_param.get('vBatNoLoad'),
        'vBatonLoad': diagnos_param.get('vBatonLoad'),
        'vSuperCap': diagnos_param.get('vSuperCap')
    }
    logger.info(f"Processed diagnosParam: {json.dumps(processed, indent=2)}")
    return processed

def process_comm_param(comm_param):
    logger.info("Processing commParam")
    processed = {
        'pppTime': comm_param.get('pppTime'),
        'ntpTime': comm_param.get('ntpTime'),
        'serverCmdsTime': comm_param.get('serverCmdsTime')
    }
    logger.info(f"Processed commParam: {json.dumps(processed, indent=2)}")
    return processed

def process_stored_diag_params(stored_diag_params):
    logger.info("Processing storedDiagParams")
    processed = {}
    for key, value in stored_diag_params.items():
        processed[key] = {
            'reason': value.get('reason'),
            'pppTime': value.get('pppTime'),
            'serverTime': value.get('serverTime'),
            'simId': value.get('simId'),
            'RSSI': value.get('RSSI'),
            'vBatNoLoad': value.get('vBatNoLoad'),
            'vBatonLoad': value.get('vBatonLoad'),
            'vSuperCap': value.get('vSuperCap')
        }
    logger.info(f"Processed storedDiagParams: {json.dumps(processed, indent=2)}")
    return processed

# For local testing
if __name__ == "__main__":
    # Sample input event
    sample_event = {
        "statusCode": 200,
        "body": json.dumps({
            "processed_data": {
                "diagnostic": {
                    "token": "FM5519",
                    "status": "ok",
                    "ts": 1732268165616,
                    "json-ver": "v1.4",
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
            },
            "timestamp": "2024-11-22T10:24:55.756373",
            "status": "success"
        })
    }

    result = lambda_handler(sample_event, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")

