import json
import base64

from models import ReportConversions


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    job = ReportConversions(data.get("start"), data.get("end"))
    results = job.run()
    responses = {"pipelines": "Taboola", "results": results}

    print(responses)
    return responses
