# TODO:
# * Export timezone aware date instead of unix epoch
# * Use object oriented pattern

import os
import pprint
import csv
import pickle
from datetime import datetime, timedelta

import google.oauth2.credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.pickle"
DATASET_TARGET_FILE = "steps_by_week.csv"

START_DATETIME = datetime(2018, 1, 1)
END_DATETIME = datetime.now()
BUCKET_TIMEDELTA = timedelta(days=7)
MAX_REQUEST_TIMEDELTA = timedelta(days=60)

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

def authenticate_service(secrets_file, token_file):
    if os.path.isfile(token_file):
        with open(token_file, "rb") as file:
            credentials = pickle.load(file)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(secrets_file,
            "https://www.googleapis.com/auth/fitness.activity.read")
        credentials = flow.run_local_server()
        with open(token_file, "wb") as file:
            pickle.dump(credentials, file)
    return build("fitness", "v1", credentials=credentials)

def request_aggregated_steps(service, start_timestamp, end_timestamp, bucket_interval, max_request_interval):
    full_dataset = []
    current_timestamp = start_timestamp
    while current_timestamp < end_timestamp:
        dataset = _request_aggregated_steps_single(
            service          = service,
            start_timestamp  = current_timestamp,
            end_timestamp    = min(current_timestamp + max_request_interval, int(end_timestamp)),
            bucket_interval  = bucket_interval,
        )
        full_dataset += dataset
        current_timestamp += max_request_interval
        _print_percent_progress(current_timestamp, start_timestamp, end_timestamp)
    full_dataset.pop()  # Exclude incomplete final datapoint
    return full_dataset

def export_dataset(dataset, target_file):
    with open(target_file, "w") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=",")
        csv_writer.writerow(["unix_epoch_ms", "steps"])
        for line in dataset:
            csv_writer.writerow(line)

def _request_aggregated_steps_single(service, start_timestamp, end_timestamp, bucket_interval):
    body = {
        "aggregateBy": [{
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
        }],
        "bucketByTime": { "durationMillis": bucket_interval },
        "startTimeMillis": start_timestamp,
        "endTimeMillis": end_timestamp,
    }
    request = service.users().dataset().aggregate(userId="me", body=body)
    buckets = request.execute()['bucket']
    return _parse_bucketed_steps(buckets)

def _parse_bucketed_steps(buckets):
    dataset = []
    for bucket in buckets:
        time = bucket["startTimeMillis"]
        if bucket["dataset"][0]["point"]:
            steps = bucket["dataset"][0]["point"][0]["value"][0]["intVal"]
        else:
            steps = 0
        dataset.append([time, steps])
    return dataset

def _print_percent_progress(current_timestamp, start_timestamp, end_timestamp):
    ratio = (current_timestamp - start_timestamp) / (end_timestamp - start_timestamp)
    percent = min(ratio, 1) * 100
    print(f"{int(percent)}% downloaded")

if __name__ =="__main__":
    start_timestamp = START_DATETIME.timestamp() * 1000
    end_timestamp = END_DATETIME.timestamp() * 1000
    bucket_interval = BUCKET_TIMEDELTA.total_seconds() * 1000
    max_request_interval = MAX_REQUEST_TIMEDELTA.total_seconds() * 1000

    service = authenticate_service(CLIENT_SECRETS_FILE, TOKEN_FILE)
    dataset = request_aggregated_steps(service,
                                       start_timestamp,
                                       end_timestamp,
                                       bucket_interval,
                                       max_request_interval)
    print(f"Writing to file {DATASET_TARGET_FILE}")
    export_dataset(dataset, DATASET_TARGET_FILE)
