import time
from datetime import datetime, timedelta

import pytz

from models.models import Voluum, BASE_URL, NOW, DATE_FORMAT, TZ


class Report(Voluum):
    keys = {
        "p_key": ["date_start", "campaignId"],
        "incre_key": "_batched_at",
    }
    schema = [
        {"name": "campaignId", "description": "Campaign ID", "type": "STRING"},
        {"name": "campaignName", "description": "Campaign", "type": "STRING"},
        {"name": "clicks", "description": "Clicks", "type": "INTEGER"},
        {"name": "conversions", "description": "Conversions", "type": "INTEGER"},
        {"name": "cost", "description": "Cost", "type": "FLOAT"},
        {"name": "customConversions1", "description": "LEAD", "type": "INTEGER"},
        {"name": "customConversions5", "description": "VIEW", "type": "INTEGER"},
        {"name": "deleted", "description": "Deleted", "type": "BOOLEAN"},
        {"name": "hour", "description": "Hour", "type": "INTEGER"},
        {"name": "impressions", "description": "Impressions", "type": "INTEGER"},
        {"name": "profit", "description": "Profit", "type": "FLOAT"},
        {"name": "revenue", "description": "Revenue", "type": "FLOAT"},
        {"name": "uniqueVisits", "description": "Unique visits", "type": "INTEGER"},
        {"name": "visits", "description": "Visits", "type": "INTEGER"},
        {"name": "date_start", "type": "TIMESTAMP"},
        {"name": "date_end", "type": "TIMESTAMP"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]

    def __init__(self, start, end):
        super().__init__()
        self.start, self.end = self._get_time_range(start, end)

    def _get_time_range(self, _start, _end):
        if _start and _end:
            start = datetime.strptime(_start, DATE_FORMAT)
            end = datetime.strptime(_end, DATE_FORMAT)
        else:
            end = NOW
            start = NOW - timedelta(days=28)
        return start, end

    def _get(self, session, headers):
        date_ranges = [
            self.start + timedelta(i)
            for i in range(int((self.end - self.start).days) + 1)
        ]
        date_ranges = [date.replace(hour=0, minute=0, second=0) for date in date_ranges]
        return [self._get_one(session, headers, date) for date in date_ranges]

    def _get_one(self, session, headers, date, offset=0):
        limit = 10000
        with session.get(
            f"{BASE_URL}/report",
            params={
                "include": "ALL",
                "from": date.isoformat(timespec="seconds") + "Z",
                "to": (date + timedelta(days=1)).isoformat(timespec="seconds") + "Z",
                "tz": TZ,
                "column": [
                    "day",
                    "campaignName",
                    "campaignId",
                    "impressions",
                    "visits",
                    "uniqueVisits",
                    "clicks",
                    "conversions",
                    "customConversions1",
                    "customConversions5",
                    "revenue",
                    "cost",
                    "profit",
                    "deleted",
                ],
                "conversionTimeMode": "VISIT",
                "groupBy": "campaign",
                "limit": limit,
                "offset": offset,
            },
            headers=headers,
        ) as r:
            r.raise_for_status()
            res = r.json()
        rows = res["rows"]
        rows = [
            {
                **row,
                "date_start": pytz.timezone(TZ)
                .localize(date)
                .isoformat(timespec="seconds"),
                "date_end": (
                    pytz.timezone(TZ).localize(date) + timedelta(days=1)
                ).isoformat(timespec="seconds"),
            }
            for row in rows
        ]
        time.sleep(1)
        return rows + self._get_one(session, headers, date, offset + limit) if rows else []

    def _transform(self, rows):
        rows = [i for j in rows for i in j]
        return [
            {
                **row,
                "_batched_at": NOW.isoformat(),
            }
            for row in rows
        ]
