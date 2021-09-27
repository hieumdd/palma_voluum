from models.models import Voluum, BASE_URL


class Offer(Voluum):
    table = "Offer"
    keys = {
        "p_key": ["id"],
        "incre_key": "updatedTime",
    }
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "namePostfix", "type": "STRING"},
        {"name": "createdTime", "type": "TIMESTAMP"},
        {"name": "updatedTime", "type": "TIMESTAMP"},
        {"name": "deleted", "type": "BOOLEAN"},
        {"name": "url", "type": "STRING"},
        {"name": "currencyCode", "type": "STRING"},
    ]

    def _get(self, session, headers):
        with session.get(
            url=f"{BASE_URL}/offer",
            params={
                "includeDeleted": True,
                "fields": [
                    "createdTime",
                    "currencyCode",
                    "deleted",
                    "id",
                    "name",
                    "namePostfix",
                    "updatedTime",
                    "url",
                ],
            },
            headers=headers,
        ) as r:
            r.raise_for_status()
            res = r.json()
        return res["offers"]

    def _transform(self, rows):
        return [
            {
                "createdTime": row["createdTime"],
                "currencyCode": row["currencyCode"],
                "deleted": row["deleted"],
                "id": row["id"],
                "name": row["name"],
                "namePostfix": row["namePostfix"],
                "updatedTime": row["updatedTime"],
                "url": row["url"],
            }
            for row in rows
        ]
