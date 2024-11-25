import json
import logging
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Lambda function started")
    logger.info(f"Received event: {json.dumps(event, indent=2)}")

    # Check if the event is from SQS or direct JSON input
    if 'Records' in event and len(event['Records']) > 0:
        # SQS event
        try:
            message = json.loads(event['Records'][0]['body'])
        except Exception as e:
            logger.error(f"Error parsing SQS message: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps('Error parsing SQS message')
            }
    else:
        # Direct JSON input
        message = event

    logger.info(f"Processed message: {json.dumps(message, indent=2)}")

    # Process and validate each section
    processed_data = {}
    
    sections = ['telemetry', 'error', 'pump', 'diagnostic']
    for section in sections:
        if section in message:
            processed_data[section] = process_section(section, message[section])
        else:
            logger.warning(f"Section '{section}' not found in the message")
    
    # Check for anomalies
    anomalies = check_anomalies(processed_data)
    if anomalies:
        logger.warning(f"Anomalies detected: {anomalies}")
        processed_data['anomalies'] = anomalies
    
    # Prepare final output
    output = {
        'processed_data': processed_data,
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'success'
    }
    
    logger.info(f"Processed output: {json.dumps(output, indent=2)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(output)
    }

def process_section(section_name, section_data):
    logger.info(f"Processing {section_name} section")
    
    # Validate common fields
    required_fields = ['token', 'status', 'json-ver']
    for field in required_fields:
        if field not in section_data:
            logger.error(f"Missing required field '{field}' in {section_name} section")
            raise ValueError(f"Missing required field '{field}' in {section_name} section")
    
    # Process specific sections
    if section_name == 'telemetry':
        return process_telemetry(section_data)
    elif section_name == 'error':
        return process_error(section_data)
    elif section_name == 'pump':
        return process_pump(section_data)
    elif section_name == 'diagnostic':
        return process_diagnostic(section_data)
    else:
        logger.warning(f"Unknown section: {section_name}")
        return section_data

def process_telemetry(data):
    logger.info("Processing telemetry data")
    if 'teleParam' not in data or not isinstance(data['teleParam'], list):
        logger.error("Invalid telemetry data structure")
        raise ValueError("Invalid telemetry data structure")
    
    for param in data['teleParam']:
        required_fields = ['ts', 'flowRate', 'discharge', 'workHour', 'cummRevDisch', 'Data', 'CycleSlips', 'NoData', 'USS']
        for field in required_fields:
            if field not in param:
                logger.error(f"Missing required field '{field}' in telemetry data")
                raise ValueError(f"Missing required field '{field}' in telemetry data")
    
    return data

def process_error(data):
    logger.info("Processing error data")
    if 'mspErrParam' not in data or not isinstance(data['mspErrParam'], list):
        logger.error("Invalid error data structure")
        raise ValueError("Invalid error data structure")
    
    for param in data['mspErrParam']:
        required_fields = ['ts', 'err-code']
        for field in required_fields:
            if field not in param:
                logger.error(f"Missing required field '{field}' in error data")
                raise ValueError(f"Missing required field '{field}' in error data")
    
    return data

def process_pump(data):
    logger.info("Processing pump data")
    if 'pumpParam' not in data or not isinstance(data['pumpParam'], list):
        logger.error("Invalid pump data structure")
        raise ValueError("Invalid pump data structure")
    
    for param in data['pumpParam']:
        required_fields = ['PumpStartTs', 'Startdischarge', 'StartData', 'StartNoData', 'StartCycleSlips',
                           'PumpStoptTs', 'Stopdischarge', 'StopData', 'StopNoData', 'StopCycleSlips']
        for field in required_fields:
            if field not in param:
                logger.error(f"Missing required field '{field}' in pump data")
                raise ValueError(f"Missing required field '{field}' in pump data")
    
    return data

def process_diagnostic(data):
    logger.info("Processing diagnostic data")
    required_sections = ['diagnosParam', 'commParam', 'storedDiagParams']
    for section in required_sections:
        if section not in data:
            logger.error(f"Missing required section '{section}' in diagnostic data")
            raise ValueError(f"Missing required section '{section}' in diagnostic data")
    
    return data

def check_anomalies(data):
    logger.info("Checking for anomalies")
    anomalies = []
    
    # Check telemetry anomalies
    if 'telemetry' in data and 'teleParam' in data['telemetry']:
        for param in data['telemetry']['teleParam']:
            if param['flowRate'] < 0 or param['flowRate'] > 100:
                anomalies.append(f"Unusual flow rate: {param['flowRate']}")
            if param['discharge'] < 0:
                anomalies.append(f"Negative discharge: {param['discharge']}")
    
    # Check error anomalies
    if 'error' in data and 'mspErrParam' in data['error']:
        for param in data['error']['mspErrParam']:
            if param['err-code'] > 200:
                anomalies.append(f"High error code: {param['err-code']}")
    
    # Check pump anomalies
    if 'pump' in data and 'pumpParam' in data['pump']:
        for param in data['pump']['pumpParam']:
            if param['PumpStoptTs'] <= param['PumpStartTs']:
                anomalies.append("Pump stop time is before or equal to start time")
    
    # Check diagnostic anomalies
    if 'diagnostic' in data and 'diagnosParam' in data['diagnostic']:
        diag_param = data['diagnostic']['diagnosParam']
        if diag_param['RSSI'] > -30 or diag_param['RSSI'] < -120:
            anomalies.append(f"Unusual RSSI value: {diag_param['RSSI']}")
    
    return anomalies

# For local testing
if __name__ == "__main__":
    # Sample direct JSON input
    sample_input = {
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
                }
            ]
        },
        "error": {
            "token": "FM4568",
            "status": "ok",
            "json-ver": "v1.4",
            "mspErrParam": [
                {
                    "ts": 1732253451768,
                    "err-code": 127
                }
            ]
        },
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
        },
        "diagnostic": {
            "token": "FM5519",
            "status": "ok",
            "ts": 1732253451768,
            "json-ver": "v1.4",
            "diagnosParam": {
                "RSSI": -54,
                "ttc": 3579,
                "simId": 1,
                "vBatNoLoad": 349,
                "vBatonLoad": 326,
                "vSuperCap": 320
            },
            "commParam": {
                "pppTime": 44,
                "ntpTime": 30,
                "serverCmdsTime": 5
            },
            "storedDiagParams": {
                "param1": {
                    "reason": "err-server-con",
                    "pppTime": 40,
                    "serverTime": 120,
                    "simId": 2,
                    "RSSI": -59,
                    "vBatNoLoad": 336,
                    "vBatonLoad": 341,
                    "vSuperCap": 311
                },
                "param2": {
                    "reason": "err-server-con",
                    "pppTime": 45,
                    "serverTime": 120,
                    "simId": 1,
                    "RSSI": -79,
                    "vBatNoLoad": 323,
                    "vBatonLoad": 334,
                    "vSuperCap": 301
                },
                "param3": {
                    "reason": "err-server-con",
                    "pppTime": 49,
                    "serverTime": 120,
                    "simId": 2,
                    "RSSI": -108,
                    "vBatNoLoad": 329,
                    "vBatonLoad": 336,
                    "vSuperCap": 319
                }
            }
        }
    }
    
    result = lambda_handler(sample_input, None)
    print(f"Lambda function result: {json.dumps(result, indent=2)}")
