import json
import base64

from models import Voluum
from broadcast import broadcast


def main(request):
    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        results = broadcast(data)
    elif "table" in data:
        job = Voluum.factory(
            data["table"],
            data.get("start"),
            data.get("end"),
        )
        results = job.run()
    else:
        raise NotImplementedError(data)

    responses = {
        "pipelines": "Voluum",
        "results": results,
    }
    print(responses)
    return responses
