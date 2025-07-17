import os
from flask import Flask, request, jsonify
from google.cloud import tasks_v2
import json
from dotenv import load_dotenv
import time

app = Flask(__name__)

load_dotenv(override=True)

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
QUEUE_ID = os.environ["GCP_QUEUE_ID"]
LOCATION = os.environ["GCP_LOCATION"]
WORKER_URL = os.environ["WORKER_URL"]
SERVICE_ACCOUNT = os.environ["TASK_SERVICE_ACCOUNT_EMAIL"]

@app.route("/", methods=["POST"])
def enqueue_gemini():
    event = request.get_json()
    bucket = event["bucket"]
    name = event["name"]
  

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, LOCATION, QUEUE_ID)

    task = {
        "http_request": {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": WORKER_URL,
            "headers": {"Content-Type": "application/json"},
            # "oidc_token": {"service_account_email": SERVICE_ACCOUNT},
            "body": json.dumps({"bucket": bucket, "name": name}).encode()
        }
    }

    client.create_task(parent=parent, task=task)
    return jsonify({"status": "enqueued", "file": name}), 200

@app.route("/worker", methods=["POST"])
def respond_to_request():
    print("Worker received a request")
    data = request.get_json()
    print(data)

    time.sleep(30)  # Simulate processing time

    return "Worker received the request", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
