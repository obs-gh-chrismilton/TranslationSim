import json
import random
import datetime
import logging
import uuid
import threading
import os
import time
import requests
from queue import PriorityQueue
from logging.handlers import RotatingFileHandler
import signal
import sys

# Load configuration
with open('trconfig.json', 'r') as config_file:
    config = json.load(config_file)

# Configuration values
desired_path = config['desired_path']
os.chdir(desired_path)  # Change working directory to desired path
http_endpoint = config['http_endpoint']
auth_token = config['auth_token']
log_file_name = config['log_file_name']
max_processing_times = config['max_processing_times']
service_specific_errors = config['service_specific_errors']

# Setup logger with rotation
logger = logging.getLogger('DocumentTranslationSimLogger')
handler = RotatingFileHandler(log_file_name, maxBytes=100*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Flag to indicate whether the script should stop processing
stop_processing = False

# List to keep track of running threads
running_threads = []

def signal_handler(sig, frame):
    global stop_processing
    stop_processing = True
    logger.info("Termination signal received. Waiting for ongoing jobs to complete before exiting.")
    for t in running_threads:
        t.join()
    logger.info("All jobs completed. Exiting now.")
    sys.exit(0)

# Register signal handler for SIGINT (Ctrl+C) and SIGTERM
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Document processing configurations extracted from the config file
workflow_sequence = ["Document Upload", "Language Detection", "Translation", "Quality Assurance", "Notification"]
document_queue = PriorityQueue()

def log_and_push_event(event):
    event["timestamp"] = datetime.datetime.now().isoformat()
    log_message = json.dumps(event)
    logger.info(log_message)
    headers = {'Authorization': f'Bearer {auth_token}', 'Content-Type': 'application/json'}
    try:
        response = requests.post(http_endpoint, headers=headers, data=log_message.encode('utf-8'))
        if response.status_code != 200:
            logger.error(f"Failed to push log to endpoint. Status Code: {response.status_code}. Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error pushing log to endpoint: {e}")

def update_service_health_states():
    while not stop_processing:
        for service, health_config in config['service_health'].items():
            current_state = health_config['state']
            transition_prob = random.random()
            # Transition probabilities from configuration
            if current_state == "Good" and transition_prob < config['service_health_transition_probabilities']['GoodToDegraded']:
                health_config['state'] = "Degraded"
            elif current_state == "Degraded":
                if transition_prob < config['service_health_transition_probabilities']['DegradedToCritical']:
                    health_config['state'] = "Critical"
                elif transition_prob < config['service_health_transition_probabilities']['DegradedToGood']:
                    health_config['state'] = "Good"
            elif current_state == "Critical" and transition_prob < config['service_health_transition_probabilities']['CriticalToDegraded']:
                health_config['state'] = "Degraded"
            
            logger.info(f"Service {service} health updated to {health_config['state']}")
        time.sleep(10)  # Check and potentially update service health every 10 seconds

def simulate_workflow(documentID, document_attributes):
    start_time = time.time()
    document_pages = document_attributes['length']
    error_occurred = False

    for service in workflow_sequence:
        if stop_processing:
            break

        health_state = config['service_health'][service]['state']
        degraded_impact = config['service_health'][service]['degraded_impact']
        critical_impact = config['service_health'][service]['critical_impact']

        processing_multiplier = 1
        if health_state == "Degraded":
            processing_multiplier = degraded_impact
        elif health_state == "Critical":
            processing_multiplier = critical_impact
            # Optionally increase the chance of error in critical state
            error_occurred = random.random() < 0.5

        processing_time = random.randint(1, max_processing_times[service]) + document_pages // 10
        processing_time = int(min(processing_time, max_processing_times[service]) * processing_multiplier)

        log_and_push_event({
            "documentID": documentID,
            "service": service,
            "action": "Entering",
            "processingTime": processing_time,
            "documentSize": document_pages,
            "serviceHealth": health_state
        })
        time.sleep(processing_time)

        if error_occurred:
            errorType = random.choice(service_specific_errors[service])
            log_and_push_event({
                "documentID": documentID,
                "service": service,
                "errorType": errorType,
                "action": "Failed",
                "documentSize": document_pages,
                "serviceHealth": health_state
            })
            break
        else:
            log_and_push_event({
                "documentID": documentID,
                "service": service,
                "action": "Completed",
                "processingTime": processing_time,
                "documentSize": document_pages,
                "serviceHealth": health_state
            })

    if not error_occurred:
        total_processing_time = time.time() - start_time
        log_and_push_event({
            "documentID": documentID,
            "action": "All Services Completed",
            "processingTime": int(total_processing_time),
            "documentSize": document_pages,
            "serviceHealth": health_state
        })

def simulate_document_workflow():
    while not stop_processing:
        try:
            priority, (documentID, document_attributes) = document_queue.get(timeout=1)
            worker_thread = threading.Thread(target=simulate_workflow, args=(documentID, document_attributes))
            worker_thread.start()
            running_threads.append(worker_thread)
        except:
            continue

def add_document_to_processing_queue():
    while not stop_processing:
        documentID = str(uuid.uuid4())
        document_pages = random.randint(1, 100)
        document_attributes = {'length': document_pages, 'priority': random.randint(1, 3)}
        document_queue.put((document_attributes['priority'], (documentID, document_attributes)))
        time.sleep(random.randint(1, 5))

def start_processing():
    producer_thread = threading.Thread(target=add_document_to_processing_queue)
    consumer_thread = threading.Thread(target=simulate_document_workflow)
    health_check_thread = threading.Thread(target=update_service_health_states)

    running_threads.extend([producer_thread, consumer_thread, health_check_thread])
    producer_thread.start()
    consumer_thread.start()
    health_check_thread.start()

    producer_thread.join()
    consumer_thread.join()
    health_check_thread.join()

if __name__ == "__main__":
    start_processing()
