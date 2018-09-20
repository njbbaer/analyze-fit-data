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
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.pickle"
DATASET_TARGET_FILE = "steps_by_week.csv"
START_TIME_UNIX = datetime(2018, 1, 1).timestamp() * 1000
END_TIME_UNIX = datetime.now().timestamp() * 1000
BUCKET_DURATION_MS = timedelta(days=7).total_seconds() * 1000
MAX_REQUEST_INTERVAL_MS = timedelta(days=60).total_seconds() * 1000

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

def request_aggregated_steps(service, start_time, end_time, bucket_size):
    body = {
        "aggregateBy": [{
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
        }],
        "bucketByTime": { "durationMillis": bucket_size },
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
    service = authenticate_service(CLIENT_SECRETS_FILE, TOKEN_FILE)
    full_dataset = []
    current_time = START_TIME_UNIX
    while current_time < END_TIME_UNIX:
        dataset = request_aggregated_steps(
            service     = service,
            start_time  = current_time,
            end_time    = min(current_time + MAX_REQUEST_INTERVAL_MS, int(END_TIME_UNIX)),
            bucket_size = BUCKET_DURATION_MS,
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
