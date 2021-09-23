from .utils import process

START = '2021-08-04'
END = '2021-08-06'


def test_report_conversions():
    data = {
        "table": "ReportConversions",
        "start": START,
        "end": END,
    }
    process(data)


def test_report():
    data = {
        "table": "Report",
        "start": START,
        "end": END,
    }
    process(data)
