import os
import json
from datetime import datetime
import asyncio
from abc import ABC, abstractmethod

import requests
import aiohttp

BASE_URL = "https://api.voluum.com"


def get_headers():
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    payload = {"accessId": os.getenv("ACCESS_ID"), "accessKey": os.getenv("ACCESS_KEY")}
    url = f"{BASE_URL}/auth/access/session"
    with requests.post(url, data=json.dumps(payload), headers=headers) as r:
        res = r.json()
    return {"cwauth-token": res["token"]}


class ReportConversions:
    def __init__(self, start=None, end=None):
        self.start, self.end = self.get_time_range(start, end)
        self.columns = self.get_config()
        self.headers = get_headers()

    def get_config(self):
        with open(f"configs/ReportConversions.json", 'r') as f:
            config = json.load(f)
        return config['columns']

    def get_time_range(self, start, end):
        return datetime(2021, 7, 20).strftime("%Y-%m-%dT%H:%M:%SZ"), datetime(
            2021, 7, 27
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    def get_params(self):
        limit = 1000
        return {
            "from": self.start,
            "to": self.end,
            "tz": "America/Los_Angeles",
            "columns": self.columns,
            "limit": limit,
        }, limit

    def get(self):
        limit = 1000
        params = {
            "from": self.start,
            "to": self.end,
            "tz": "America/Los_Angeles",
            "columns": self.columns,
            "limit": limit,
            "offset": 0,
        }
        url = f"{BASE_URL}/report/conversions"
        rows = asyncio.run(self._get(url))
        return rows

    async def _get(self, url):
        connector = aiohttp.TCPConnector(limit=3)
        timeout = aiohttp.ClientTimeout(total=530)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as sessions:
            total_rows = await self._get_initial_rows(sessions, url)
            params, limit = self.get_params()
            offsets = [i for i in range(0, total_rows, limit)]
            tasks = [asyncio.create_task(self._get_offset(sessions, url, params, i)) for i in offsets]
            rows = await asyncio.gather(*tasks)
        return rows

    async def _get_initial_rows(self, sessions, url):
        params, _ = self.get_params()
        params['limit'] = 10
        async with sessions.get(url, params=params, headers=self.headers) as r:
            res = await r.json()
        return res['totalRows']

    async def _get_offset(self, sessions, url, params, offset):
        params['offset'] = offset
        async with sessions.get(url, params=params, headers=self.headers) as r:
            res = await r.json()
        return res['rows']
        
    def transform(self, rows):
        return rows

    def load(self, rows):
        with open('test.json', 'w') as f:
            json.dump(rows, f)

    def run(self):
        rows = self.get()
        rows = self.transform(rows)
        self.load(rows)


def main():
    job = ReportConversions()
    job.get()


main()
