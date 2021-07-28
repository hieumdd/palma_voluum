import os
import json
import csv
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

import requests
from google.cloud import bigquery
import jinja2
import pytz


NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"
T_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"
S_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
HOUR_FORMAT = "%Y-%m-%dT%H"
TZ = "America/Los_Angeles"

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


class Voluum(ABC):
    def __init__(self, start, end):
        self.start, self.end = self.get_time_range(start, end)
        self.keys, self.column, self.fields, self.schema = self.get_config()
        self.headers = get_headers()

    @staticmethod
    def factory(mode, start, end):
        args = (start, end)
        if mode == "report_conversions":
            return ReportConversions(*args)
        elif mode == "report":
            return Report(*args)
        else:
            raise NotImplementedError(mode)

    def get_config(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["column"], config["fields"], config["schema"]

    def get_time_range(self, _start, _end):
        if _start and _end:
            start = datetime.strptime(_start, DATE_FORMAT).strftime(HOUR_FORMAT)
            end = datetime.strptime(_end, DATE_FORMAT).strftime(HOUR_FORMAT)
        else:
            end = NOW.strftime(HOUR_FORMAT)
            start = (NOW - timedelta(days=7)).strftime(HOUR_FORMAT)
        return start, end

    def get(self):
        url = f"{BASE_URL}/{self._get_endpoint()}"
        params = {
            "from": self.start,
            "to": self.end,
            "tz": TZ,
            "column": self.column,
        }
        with requests.post(url, params=params, headers=self.headers) as r:
            r.raise_for_status()
            res = r.content
        decoded_content = res.decode("utf-8")
        csv_lines = decoded_content.splitlines()
        with open("test.csv", "w") as f:
            for line in csv_lines:
                f.write(line)
                f.write("\n")
        cr = csv.DictReader(csv_lines[1:], fieldnames=tuple(self.column), delimiter=",")
        rows = [row for row in cr]
        return rows

    @abstractmethod
    def _get_endpoint(self):
        pass

    @abstractmethod
    def _get_params(self, params):
        pass

    def transform(self, rows):
        for row in rows:
            for i in self.fields.get("timestamp"):
                if row.get(i):
                    dt = datetime.strptime(row[i], S_TIMESTAMP_FORMAT)
                    loc_dt = pytz.timezone(TZ).localize(dt)
                    row[i] = loc_dt.isoformat(timespec="seconds")
        rows = [
            {**row, "_batched_at": NOW.strftime(T_TIMESTAMP_FORMAT)} for row in rows
        ]
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
            dataset=DATASET, table=self.table, p_key=",".join(self.keys.get("p_key"))
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


class ReportConversions(Voluum):
    table = "ReportConversions"

    def __init__(self, start, end):
        super().__init__(start, end)

    def _get_endpoint(self):
        return "report/conversions"

    def _get_params(self, params):
        return params


class Report(Voluum):
    table = "Report"

    def __init__(self, start, end):
        super().__init__(start, end)

    def _get_endpoint(self):
        return "report"

    def _get_params(self, params):
        return {**params, "conversionTimeMode": "VISIT", "groupBy": "campaign"}
