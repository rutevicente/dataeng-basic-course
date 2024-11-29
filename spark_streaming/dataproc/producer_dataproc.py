import requests
from pyspark.sql.types import *
import json
import datetime
import os
import time

def ingest_from_api(url: str, landing_path: str):
    print("fetching data...")
    response = requests.get(url)
    print(url)
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    if response.status_code == 200:
        data = response.json()
        with open(f"{landing_path}_{int(timestamp)}.json", "w+") as f:
            json.dump(data, f)
            print("writing done...")

def producer_vehicles(loop: int, interval_time: int, landing_path: str):
    for i in range(loop):
        print(f"producing running...{i}")
        ingest_from_api(f"https://api.carrismetropolitana.pt/vehicles", landing_path)
        time.sleep(interval_time)

if __name__ == '__main__':
    print("Starting producer...")
    bucket_name="edit-data-eng-dev"
    table="vehicles"

    if (os.getenv('COLAB_JUPYTER_IP')):
        landing_path=f"/content/landing/{table}/{table}"
    else:
        landing_path=f"gs://{bucket_name}/datalake/landing/{table}/{table}"
        
    print(f"landing_path: {landing_path}")

    producer_vehicles(10, 30, landing_path)