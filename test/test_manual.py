from .utils import process

START = '2021-07-27'
END = '2021-07-28'


def test_report_conversions():
    data = {
        "table": "report_conversions",
        "start": START,
        "end": END,
    }
    process(data)


def test_report():
    data = {
        "table": "report",
        "start": START,
        "end": END,
    }
    process(data)
