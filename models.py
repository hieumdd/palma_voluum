import os
import json
import csv
from datetime import datetime, timedelta

import requests
from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
HOUR_FORMAT = "%Y-%m-%dT%H"

BASE_URL = "https://api.voluum.com"

BQ_CLIENT = bigquery.Client()
DATASET = "Palma"

TEMPLATE_LOADER = jinja2.FileSystemLoader("./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


def get_headers():
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    payload = {"accessId": os.getenv("ACCESS_ID"), "accessKey": os.getenv("ACCESS_KEY")}
    url = f"{BASE_URL}/auth/access/session"
    with requests.post(url, data=json.dumps(payload), headers=headers) as r:
        res = r.json()
    return {"cwauth-token": res["token"]}


class ReportConversions:
    table = "ReportConversions"

    def __init__(self, start=None, end=None):
        self.start, self.end = self.get_time_range(start, end)
        self.column, self.schema = self.get_config()
        self.headers = get_headers()

    def get_config(self):
        with open(f"configs/ReportConversions.json", "r") as f:
            config = json.load(f)
        return config["column"], config["schema"]

    def get_time_range(self, _start, _end):
        if _start and _end:
            start = datetime.strptime(_start, DATE_FORMAT).strftime(HOUR_FORMAT)
            end = datetime.strptime(_end, DATE_FORMAT).strftime(HOUR_FORMAT)
        else:
            end = NOW.strftime(HOUR_FORMAT)
            start = (NOW - timedelta(days=7)).strftime(HOUR_FORMAT)
        return start, end

    def get(self):
        url = f"{BASE_URL}/report/conversions"
        params = {
            "from": self.start,
            "to": self.end,
            "tz": "America/Los_Angeles",
            "column": self.column,
        }
        with requests.post(url, params=params, headers=self.headers) as r:
            res = r.content
        decoded_content = res.decode("utf-8")
        csv_lines = decoded_content.splitlines()
        cr = csv.DictReader(csv_lines[1:], fieldnames=tuple(self.column))
        return [row for row in cr]

    def transform(self, rows):
        rows = [{**row, "_batched_at": NOW.strftime(TIMESTAMP_FORMAT)} for row in rows]
        return rows

    def load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

    def update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET, table=self.table, p_key=",".join(self.column)
        )
        BQ_CLIENT.query(rendered_query)

    def run(self):
        rows = self.get()
        responses = {
            "table": self.table,
            "start": self.start,
            "end": self.end,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses["output_rows"] = loads.output_rows
        return responses
