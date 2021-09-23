from .utils import process


def test_report_conversions():
    data = {
        "table": "ReportConversions",
    }
    process(data)


def test_report():
    data = {
        "table": "Report",
    }
    process(data)

def test_offer():
    data = {
        "table": "Offer",
    }
    process(data)
