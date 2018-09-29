# TODO:
# * Fix daylight savings offset

import os
import pprint
import csv
import pickle
import pytz
from datetime import datetime, timedelta
from collections import namedtuple

import google.oauth2.credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow

class GoogleFitSteps:
    def __init__(self, start_time, bucket_interval, max_request_interval):
        self.start_time = start_time
        self.bucket_interval = bucket_interval
        self.max_request_interval = max_request_interval
        self.dataset = []

    def authenticate(self, secrets_file, credentials_file):
        if os.path.isfile(credentials_file):
            with open(credentials_file, "rb") as file:
                credentials = pickle.load(file)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(secrets_file,
                "https://www.googleapis.com/auth/fitness.activity.read")
            credentials = flow.run_local_server()
            with open(credentials_file, "wb") as file:
                pickle.dump(credentials, file)
        self.service = build("fitness", "v1", credentials=credentials)

    def download(self):
        self._request_aggregated_steps(
            start_time = self.start_time,
            end_time = datetime.now())

    def export_dataset(self, target_file, date_format, timezone):
        with open(target_file, "w") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=",")
            csv_writer.writerow(["time", "steps"])
            for line in self.dataset:
                timestamp, steps = line
                dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone) # Convert timestamp to datetime
                date_string = datetime.strftime(dt, date_format) # Convert datetime to string
                csv_writer.writerow([date_string, steps])

    # Private

    def _request_aggregated_steps(self, start_time, end_time):
        current_time = start_time
        while current_time < end_time:
            request_dataset = self._request_aggregated_steps_single(
                request_start_time = current_time,
                request_end_time   = min(current_time + self.max_request_interval, end_time))
            self.dataset += request_dataset
            current_time += self.max_request_interval
        self.dataset.pop() # Exclude incomplete final datapoint

    def _request_aggregated_steps_single(self, request_start_time, request_end_time):
        body = {
            "aggregateBy": [{
                "dataTypeName": "com.google.step_count.delta",
                "dataSourceId": "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps"
            }],
            "bucketByTime": { "durationMillis": self.bucket_interval.total_seconds() * 1000 },
            "startTimeMillis": int(request_start_time.timestamp()) * 1000,
            "endTimeMillis": int(request_end_time.timestamp()) * 1000,
        }
        request = self.service.users().dataset().aggregate(userId="me", body=body)
        buckets = request.execute()['bucket']
        dataset = self._parse_bucketed_steps(buckets)
        return dataset

    def _parse_bucketed_steps(self, buckets):
        dataset = []
        for bucket in buckets:
            time = int(bucket["startTimeMillis"])
            if bucket["dataset"][0]["point"]:
                steps = int(bucket["dataset"][0]["point"][0]["value"][0]["intVal"])
            else:
                steps = 0
            dataset.append([time, steps])
        return dataset

if __name__ == "__main__":
    steps = GoogleFitSteps(
        start_time           = datetime(2018, 1, 1),
        bucket_interval      = timedelta(days=1),
        max_request_interval = timedelta(days=60))

    steps.authenticate(
        secrets_file     = "client_secret.json",
        credentials_file = "token.pickle")

    steps.download()

    steps.export_dataset(
        target_file = "steps_by_day.csv",
        date_format = "%m/%d/%Y",
        timezone    = pytz.timezone("America/Los_Angeles"))
