from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///sqlite.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

from google_fit import GoogleFitSteps

db.create_all()

steps = GoogleFitSteps(
    start_time           = datetime(2018, 1, 1),
    bucket_interval      = timedelta(days=1),
    max_request_interval = timedelta(days=90))
steps.authenticate(
    secrets_file     = "client_secret.json",
    credentials_file = "token.pickle")
steps.download()

@app.route('/')
def steps_chart():
    return 'Google Fit Steps'
