from .utils import process


def test_report_conversions():
    data = {
        "table": "report_conversions",
    }
    process(data)


def test_report():
    data = {
        "table": "report",
    }
    process(data)
