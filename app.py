from flask import Flask
from google_fit import GoogleFitSteps
from datetime import datetime, timedelta

app = Flask(__name__)

steps = GoogleFitSteps(
    start_time           = datetime(2018, 1, 1),
    bucket_interval      = timedelta(days=1),
    max_request_interval = timedelta(days=90))
steps.authenticate(
    secrets_file     = "client_secret.json",
    credentials_file = "token.pickle")
steps.update()

@app.route('/')
def steps_chart():
    return 'Google Fit Steps'
