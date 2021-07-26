import json
import base64
from unittest.mock import Mock

from main import main

def encode_data(data):
    """Encode data to publish to PubSub

    Args:
        data (dict): Data

    Returns:
        dict: Data encoded
    """

    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    return {"message": {"data": data_encoded}}

def assertion(res):
    """Assert test

    Args:
        res (list): Job Results
    """

    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["output_rows"] > 0
        assert res["num_processed"] == res["output_rows"]

def process(data):
    """Main Test Process

    Args:
        data (dict): Data
    """
        
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)
