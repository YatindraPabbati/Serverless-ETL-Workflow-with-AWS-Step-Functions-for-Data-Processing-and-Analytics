import json
import random
import time
import logging
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS IoT configuration
AWS_IOT_ENDPOINT = ""
AWS_IOT_PORT = 443
AWS_IOT_TOPIC = ""
AWS_IOT_CLIENT_ID = ""
AWS_IOT_CERT_PATH = ""
AWS_IOT_KEY_PATH = ""
AWS_IOT_ROOT_CA_PATH = ""

def generate_tele_param(timestamp):
    return {
        "token": "FM1037",
        "status": "ok",
        "json-ver": "v1.2",
        "teleParam": [
            {
                "ts": timestamp,
                "flowRate": round(random.uniform(0, 10), 14),
                "discharge": random.randint(36000, 37000),
                "workHour": random.randint(21000, 23000),
                "cummRevDisch": random.randint(-10, 10),
                "Data": random.randint(210000, 230000),
                "CycleSlips": random.randint(50000, 53000),
                "NoData": random.randint(600, 700),
                "USS": random.randint(52000000, 53000000)
            }
        ]
    }

def generate_error_param(timestamp):
    return {
        "token": "FM4568",
        "status": "ok",
        "json-ver": "v1.4",
        "mspErrParam": [
            {
                "ts": timestamp,
                "err-code": random.choice([127, 175, 176])
            }
        ]
    }

def generate_pump_param(timestamp):
    start_ts = timestamp
    stop_ts = start_ts + random.randint(5000, 1800000)
    return {
        "token": "FM3278",
        "status": "ok",
        "json-ver": "v1.4",
        "pumpParam": [
            {
                "PumpStartTs": start_ts,
                "Startdischarge": random.randint(19000000, 20000000),
                "StartData": random.randint(5000, 15000),
                "StartNoData": random.randint(700, 2000),
                "StartCycleSlips": random.randint(30, 600),
                "PumpStoptTs": stop_ts,
                "Stopdischarge": random.randint(19000000, 20000000),
                "StopData": random.randint(10000, 30000),
                "StopNoData": random.randint(800, 5500),
                "StopCycleSlips": random.randint(30, 1700)
            }
        ]
    }

def generate_diagnostic_param(timestamp):
    return {
        "token": "FM5519",
        "status": "ok",
        "ts": timestamp,
        "json-ver": "v1.4",
        "diagnosParam": {
            "RSSI": random.randint(-100, -50),
            "ttc": random.randint(3000, 5000),
            "simId": random.randint(1, 2),
            "vBatNoLoad": random.randint(325, 335),
            "vBatonLoad": random.randint(325, 345),
            "vSuperCap": random.randint(300, 325)
        },
        "commParam": {
            "pppTime": random.randint(30, 50),
            "ntpTime": random.randint(30, 50),
            "serverCmdsTime": random.randint(1, 5)
        },
        "storedDiagParams": {
            f"param{i}": {
                "reason": "err-server-con",
                "pppTime": random.randint(0, 50),
                "serverTime": 120,
                "simId": random.randint(1, 2),
                "RSSI": random.randint(-110, -50),
                "vBatNoLoad": random.randint(325, 340),
                "vBatonLoad": random.randint(325, 350),
                "vSuperCap": random.randint(300, 350)
            } for i in range(1, 4)
        }
    }

def on_connection_interrupted(connection, error, **kwargs):
    logging.error(f"Connection interrupted. error: {error}")

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    logging.info(f"Connection resumed. return_code: {return_code} session_present: {session_present}")

def main():
    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=AWS_IOT_ENDPOINT,
        port=AWS_IOT_PORT,
        cert_filepath=AWS_IOT_CERT_PATH,
        pri_key_filepath=AWS_IOT_KEY_PATH,
        ca_filepath=AWS_IOT_ROOT_CA_PATH,
        client_bootstrap=client_bootstrap,
        client_id=AWS_IOT_CLIENT_ID,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed
    )

    connect_future = mqtt_connection.connect()
    connect_future.result()
    logging.info("Connected to AWS IoT")

    try:
        timestamp = int(time.time() * 1000)
        
        # Generate all data types
        combined_data = {
            "telemetry": generate_tele_param(timestamp),
            "error": generate_error_param(timestamp),
            "pump": generate_pump_param(timestamp),
            "diagnostic": generate_diagnostic_param(timestamp)
        }

        # Convert to JSON
        json_data = json.dumps(combined_data, indent=2)

        # Log and publish
        logging.info("Publishing combined data:")
        print(json_data)  # Print the JSON data to the console
        
        publish_future, packet_id = mqtt_connection.publish(
            topic=AWS_IOT_TOPIC,
            payload=json_data,
            qos=mqtt.QoS.AT_LEAST_ONCE
        )
        publish_future.result()
        logging.info(f"Message {packet_id} published successfully")

        # Wait for a moment to ensure the message is published
        time.sleep(2)

    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        logging.info("Disconnected from AWS IoT")

if __name__ == "__main__":
    main()
