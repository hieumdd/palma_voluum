from models.models import Voluum


def main(request):
    data = request.get_json()
    print(data)

    if "table" in data:
        response = Voluum.factory(
            data["table"],
            data.get("start"),
            data.get("end"),
        ).run()
    else:
        raise ValueError(data)

    print(response)
    return response
