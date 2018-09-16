import os
import pprint
import csv

import google.oauth2.credentials

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_SECRETS_FILE = "client_secret.json"
DATASET_TARGET_FILE = "steps_by_hour.csv"
START_TIME_UNIX = 1454313600000
END_TIME_UNIX = 1536969600000
BUCKET_DURATION_MS = 3600000
MAX_REQUEST_INTERVAL_MS = 5184000000

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE,
        "https://www.googleapis.com/auth/fitness.activity.read")
    credentials = flow.run_local_server()
    return build("fitness", "v1", credentials=credentials)

def request_aggregated_steps(service, start_time, end_time):
    body = {
        "aggregateBy": [{
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
        }],
        "bucketByTime": { "durationMillis": BUCKET_DURATION_MS },
        "startTimeMillis": start_time,
        "endTimeMillis": end_time,
    }
    request = service.users().dataset().aggregate(userId="me", body=body)
    buckets = request.execute()['bucket']
    return parse_bucketed_steps(buckets)

def parse_bucketed_steps(buckets):
    dataset = []
    for bucket in buckets:
        time = bucket["startTimeMillis"]
        if bucket["dataset"][0]["point"]:
            steps = bucket["dataset"][0]["point"][0]["value"][0]["intVal"]
        else:
            steps = 0
        dataset.append([time, steps])
    return dataset

def export_aggregated_steps():
    service = get_authenticated_service()
    full_dataset = []
    current_time = START_TIME_UNIX
    while current_time < END_TIME_UNIX:
        dataset = request_aggregated_steps(
            service    = service,
            start_time = current_time,
            end_time   = min(current_time + MAX_REQUEST_INTERVAL_MS, END_TIME_UNIX)
        )
        full_dataset += dataset
        current_time = current_time + MAX_REQUEST_INTERVAL_MS
        print_percent_progress(current_time)
    print(f"Writing to file {DATASET_TARGET_FILE}")
    write_dataset(full_dataset)

def write_dataset(dataset):
    with open(DATASET_TARGET_FILE, "w") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=",")
        csv_writer.writerow(["unix_epoch_ms", "steps"])
        for line in dataset:
            csv_writer.writerow(line)

def print_percent_progress(current_time):
    ratio = (current_time - START_TIME_UNIX) / (END_TIME_UNIX - START_TIME_UNIX)
    percent = min(ratio, 1) * 100
    print(f"{int(percent)}% downloaded")

if __name__ =="__main__":
    export_aggregated_steps()
