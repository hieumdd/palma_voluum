from datetime import datetime

import pytz

from models.models import Voluum, BQ_CLIENT, DATASET, BASE_URL, NOW, DATE_FORMAT, TZ


class ReportConversions(Voluum):
    table = "ReportConversions"
    keys = {
        "p_key": [
            "postbackTimestamp",
            "visitTimestamp",
            "clickId",
            "conversionType",
            "offerName",
            "offerId",
            "countryName",
            "countryCode",
            "trafficSourceName",
            "trafficSourceId",
            "transactionId",
            "ip",
            "campaignName",
            "campaignId",
            "creativeId",
            "customVariable1",
            "customVariable2",
            "customVariable3",
            "customVariable4",
            "customVariable5",
            "customVariable6",
            "customVariable7",
        ],
        "incre_key": "_batched_at",
    }
    schema = [
        {"name": "clickId", "type": "STRING"},
        {"name": "conversionType", "type": "STRING"},
        {"name": "countryCode", "type": "STRING"},
        {"name": "customVariable1", "type": "STRING"},
        {"name": "customVariable2", "type": "STRING"},
        {"name": "customVariable3", "type": "STRING"},
        {"name": "customVariable4", "type": "STRING"},
        {"name": "customVariable5", "type": "STRING"},
        {"name": "customVariable6", "type": "STRING"},
        {"name": "customVariable7", "type": "STRING"},
        {"name": "transactionId", "type": "STRING"},
        {"name": "ip", "type": "STRING"},
        {"name": "offerId", "type": "STRING"},
        {"name": "offerName", "type": "STRING"},
        {"name": "postbackTimestamp", "type": "TIMESTAMP"},
        {"name": "trafficSourceId", "type": "STRING"},
        {"name": "trafficSourceName", "type": "STRING"},
        {"name": "visitTimestamp", "type": "TIMESTAMP"},
        {"name": "campaignName", "type": "STRING"},
        {"name": "campaignId", "type": "STRING"},
        {"name": "creativeId", "type": "STRING"},
        {"name": "countryName", "type": "STRING"},
        {"name": "deviceName", "type": "STRING"},
        {"name": "os", "type": "STRING"},
        {"name": "osVersion", "type": "STRING"},
        {"name": "browser", "type": "STRING"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]

    def __init__(self, start, end):
        self.start, self.end = self._get_time_range(start, end)

    def _get_time_range(self, _start, _end):
        if _start and _end:
            start = datetime.strptime(_start, DATE_FORMAT)
            end = datetime.strptime(_end, DATE_FORMAT)
        else:
            end = NOW
            query = f"""
            SELECT MAX({self.keys['incre_key']}) AS incre
            FROM `{DATASET}`.`{self.table}`
            """
            rows = BQ_CLIENT.query(query).result()
            row = [dict(row) for row in rows][0]
            start = row["incre"].astimezone(pytz.timezone(TZ))
        return start, end

    def _get(self, session, headers, offset=0):
        limit = 10000
        with session.get(
            f"{BASE_URL}/report/conversions",
            params={
                "from": self.start.strftime("%Y-%m-%dT%H"),
                "to": self.end.strftime("%Y-%m-%dT%H"),
                "tz": TZ,
                "column": [
                    "postbackTimestamp",
                    "visitTimestamp",
                    "clickId",
                    "conversionType",
                    "offerName",
                    "offerId",
                    "countryCode",
                    "trafficSourceName",
                    "trafficSourceId",
                    "transactionId",
                    "ip",
                    "campaignName",
                    "campaignId",
                    "creativeId",
                    "customVariable1",
                    "customVariable2",
                    "customVariable3",
                    "customVariable4",
                    "customVariable5",
                    "customVariable6",
                    "customVariable7",
                    "countryName",
                    "deviceName",
                    "os",
                    "osVersion",
                    "browser",
                ],
                "limit": limit,
                "offset": offset,
            },
            headers=headers,
        ) as r:
            r.raise_for_status()
            res = r.json()
        rows = res["rows"]
        if rows:
            return rows + self._get(session, headers, offset + limit)
        else:
            return []

    def _transform(self, rows):
        def transform_dt(x):
            dt = datetime.strptime(x, "%Y-%m-%d %I:%M:%S %p")
            return pytz.timezone(TZ).localize(dt).isoformat(timespec="seconds")

        return [
            {
                "postbackTimestamp": transform_dt(row["postbackTimestamp"]),
                "visitTimestamp": transform_dt(row["visitTimestamp"]),
                "clickId": row["clickId"],
                "conversionType": row["conversionType"],
                "offerName": row["offerName"],
                "offerId": row["offerId"],
                "countryCode": row["countryCode"],
                "trafficSourceName": row["trafficSourceName"],
                "trafficSourceId": row["trafficSourceId"],
                "transactionId": row["transactionId"],
                "ip": row["ip"],
                "campaignName": row["campaignName"],
                "campaignId": row["campaignId"],
                "creativeId": row["creativeId"],
                "customVariable1": row["customVariable1"],
                "customVariable2": row["customVariable2"],
                "customVariable3": row["customVariable3"],
                "customVariable4": row["customVariable4"],
                "customVariable5": row["customVariable5"],
                "customVariable6": row["customVariable6"],
                "customVariable7": row["customVariable7"],
                "countryName": row["countryName"],
                "deviceName": row["deviceName"],
                "os": row["os"],
                "osVersion": row["osVersion"],
                "browser": row["browser"],
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in rows
        ]
