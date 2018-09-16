import csv
import datetime
import pytz

DATASET_SOURCE_FILE = "steps_by_hour.csv"
TIMEZONE = "America/Los_Angeles"

timezone = pytz.timezone(TIMEZONE)

def empty_hour_buckets():
    hour_buckets = {}
    for hour in range(24):
        hour_buckets[hour] = {"sum": 0, "count": 0}
    return hour_buckets

def read_dataset():
    with open(DATASET_SOURCE_FILE, "r") as csv_file:
        return list(csv.DictReader(csv_file, delimiter=','))

def print_results(hour_buckets):
    for hour, bucket in hour_buckets.items():
        print(hour, int(bucket["sum"] / bucket["count"]))

if __name__ =="__main__":
    hour_buckets = empty_hour_buckets()
    dataset = read_dataset()
    for row in dataset:
        unix_epoch_sec = int(row["unix_epoch_ms"]) / 1000
        dt = datetime.datetime.fromtimestamp(unix_epoch_sec, timezone)
        if not dt.weekday(): continue
        bucket = hour_buckets[dt.hour]
        bucket["sum"] += int(row["steps"])
        bucket["count"] += 1
    print_results(hour_buckets)
