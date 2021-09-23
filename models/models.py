import os
import json
from datetime import datetime
import importlib
from abc import ABC, abstractmethod

import requests
from google.cloud import bigquery
import jinja2


NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"
TZ = "America/Los_Angeles"

BASE_URL = "https://api.voluum.com"

BQ_CLIENT = bigquery.Client()
DATASET = "Palma"

TEMPLATE_LOADER = jinja2.FileSystemLoader("./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


def get_headers(session):
    with session.post(
        f"{BASE_URL}/auth/session",
        data=json.dumps(
            {
                "email": os.getenv("EMAIL"),
                "password": os.getenv("VPWD"),
            }
        ),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    ) as r:
        res = r.json()
    return {
        "cwauth-token": res["token"],
    }


class Voluum(ABC):
    @staticmethod
    def factory(table, start, end):
        try:
            module = importlib.import_module(f"models.{table}")
            model = getattr(module, table)
            return model(start, end)
        except (ImportError, AttributeError, IndexError):
            raise ValueError(table)

    @property
    @abstractmethod
    def keys(self):
        pass

    @property
    @abstractmethod
    def schema(self):
        pass

    @abstractmethod
    def _get(self, session, headers):
        pass

    @abstractmethod
    def _transform(self, rows):
        pass

    def _load(self, rows):
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{DATASET}._stage_{self.table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=self.schema,
                ),
            )
            .result()
            .output_rows
        )
        self._update()
        return output_rows

    def _update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.keys.get("p_key")),
            incre_key=self.keys.get("incre_key"),
        )
        BQ_CLIENT.query(rendered_query).result()

    def run(self):
        with requests.Session() as session:
            rows = self._get(session, get_headers(session))
        response = {
            "table": self.table,
        }
        if getattr(self, "start", None) and getattr(self, "end", None):
            response = {
                **response,
                "start": self.start,
                "end": self.end,
            }
        if len(rows) > 0:
            rows = self._transform(rows)
            response = {
                **response,
                "num_processed": len(rows),
                "output_rows": self._load(rows),
            }
        return response
