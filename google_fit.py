# TODO:
# * Fix daylight savings offset
# * Rename repository

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

from app import db

class BucketSteps(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    datetime = db.Column(db.DateTime, nullable=False, unique=True)
    steps = db.Column(db.Integer, nullable=False)

class GoogleFitSteps:
    def __init__(self, start_time, bucket_interval, max_request_interval):
        self.start_time = start_time
        self.bucket_interval = bucket_interval
        self.max_request_interval = max_request_interval
        self.timezone = pytz.timezone("America/Los_Angeles")
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
        self._request_steps(
            start_time = self.start_time,
            end_time = datetime.now())

    # Private

    def _request_steps(self, start_time, end_time):
        current_time = start_time
        while current_time < end_time:
            request_dataset = self._single_request_steps(
                request_start_time = current_time,
                request_end_time   = min(current_time + self.max_request_interval, end_time))
            current_time += self.max_request_interval
        db.session.commit()

    def _single_request_steps(self, request_start_time, request_end_time):
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
        self._parse_bucket_steps(buckets)

    def _parse_bucket_steps(self, buckets):
        for bucket in buckets:
            time_millis = int(bucket["startTimeMillis"])
            if bucket["dataset"][0]["point"]:
                steps = int(bucket["dataset"][0]["point"][0]["value"][0]["intVal"])
            else:
                steps = 0
            dt = datetime.fromtimestamp(time_millis / 1000, tz=self.timezone)
            bucket = BucketSteps(datetime=dt, steps=steps)
            db.session.add(bucket)
