from unittest.mock import Mock

import pytest
from main import main


@pytest.mark.parametrize(
    "table",
    [
        "ReportConversions",
        "Report",
        "Offer",
    ],
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        ("2021-09-01", "2021-09-02"),
    ],
    ids=("auto", "manual"),
)
def test_units(table, start, end):
    if table == "Offer" and start:
        pytest.skip()
    data = {
        "table": table,
        "start": start,
        "end": end,
    }
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["output_rows"] > 0
        assert res["num_processed"] == res["output_rows"]
