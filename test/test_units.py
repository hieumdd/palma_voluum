from .utils import process

START = '2021-07-01'
END = '2021-07-10'


def test_manual():
    data = {
        "start": START,
        "end": END,
    }
    process(data)


def test_auto():
    data = {}
    process(data)
