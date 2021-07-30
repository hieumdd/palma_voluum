import os
import json
import time
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
    payload = {
        "email": os.getenv("EMAIL"),
        "password": os.getenv("VPWD"),
    }
    url = f"{BASE_URL}/auth/session"
    with requests.post(url, data=json.dumps(payload), headers=headers) as r:
        res = r.json()
    return {"cwauth-token": res["token"]}


class Voluum(ABC):
    @staticmethod
    def factory(mode, start, end):
        args = (start, end)
        if mode == "report_conversions":
            return ReportConversions(*args)
        elif mode == "report":
            return Report(*args)
        else:
            raise NotImplementedError(mode)

    def __init__(self, start, end):
        self.start, self.end = self.get_time_range(start, end)
        self.keys, self.column, self.fields, self.schema = self.get_config()
        self.headers = get_headers()

    def get_config(self):
        with open(f"configs/{self.table}.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["column"], config["fields"], config["schema"]

    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def transform(self, rows):
        pass

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
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.keys.get("p_key")),
            incre_key=self.keys.get("incre_key"),
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

    def get_time_range(self, _start, _end):
        if _start and _end:
            start = datetime.strptime(_start, DATE_FORMAT)
            end = datetime.strptime(_end, DATE_FORMAT)
        else:
            end = NOW
            template = TEMPLATE_ENV.get_template("read_max_incremental.sql.j2")
            rendered_query = template.render(
                dataset=DATASET, table=self.table, incre_key="postbackTimestamp"
            )
            rows = BQ_CLIENT.query(rendered_query).result()
            row = [dict(row) for row in rows][0]
            start = row["incre"]
        return start, end

    def get_params(self, params):
        return params

    def get(self):
        url = f"{BASE_URL}/report/conversions"
        limit = 10000
        params = {
            "from": self.start.strftime("%Y-%m-%dT%H"),
            "to": self.end.strftime("%Y-%m-%dT%H"),
            "tz": TZ,
            "column": self.column,
            "limit": limit,
            "offset": 0,
        }
        rows = []
        with requests.Session() as sessions:
            while True:
                with sessions.get(url, params=params, headers=self.headers) as r:
                    r.raise_for_status()
                    res = r.json()
                rows.extend(res["rows"])
                print(len(rows))
                if len(rows) < res["totalRows"]:
                    params["offset"] += limit
                else:
                    break
        return rows

    def transform(self, rows):
        for row in rows:
            for i in self.fields.get("timestamp"):
                if row.get(i):
                    dt = datetime.strptime(row[i], "%Y-%m-%d %I:%M:%S %p")
                    loc_dt = pytz.timezone(TZ).localize(dt)
                    row[i] = loc_dt.isoformat(timespec="seconds")
        rows = [
            {**row, "_batched_at": NOW.strftime(T_TIMESTAMP_FORMAT)} for row in rows
        ]
        return rows


class Report(Voluum):
    table = "Report"

    def __init__(self, start, end):
        super().__init__(start, end)

    def get_time_range(self, _start, _end):
        if _start and _end:
            start = datetime.strptime(_start, DATE_FORMAT)
            end = datetime.strptime(_end, DATE_FORMAT)
        else:
            end = NOW
            start = NOW - timedelta(days=28)
        return start, end

    def get(self):
        url = f"{BASE_URL}/report"
        date_ranges = []
        _start = self.start
        while _start <= self.end:
            date_ranges.append(_start)
            _start += timedelta(days=1)
        rows = []
        limit = 10000
        with requests.Session() as sessions:
            for date in date_ranges:
                params = {
                    "include": "ALL",
                    "from": date.replace(hour=0, minute=0, second=0).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "to": (
                        date.replace(hour=0, minute=0, second=0) + timedelta(days=1)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "tz": TZ,
                    "column": self.column,
                    "conversionTimeMode": "VISIT",
                    "groupBy": "campaign",
                    "limit": limit,
                    "offset": 0,
                }
                _rows = []
                while True:
                    with sessions.get(url, params=params, headers=self.headers) as r:
                        r.raise_for_status()
                        res = r.json()
                    _rows = res["rows"]
                    if len(_rows) < res["totalRows"]:
                        params["offset"] += limit
                        time.sleep(1)
                    else:
                        break
                rows.extend(
                    [
                        {
                            **_row,
                            "date_start": pytz.timezone(TZ).localize(date).isoformat(),
                            "date_end": (
                                pytz.timezone(TZ).localize(date) + timedelta(days=1)
                            ).isoformat(),
                            "_batched_at": NOW.isoformat(),
                        }
                        for _row in _rows
                    ]
                )
        return rows

    def transform(self, rows):
        return rows
