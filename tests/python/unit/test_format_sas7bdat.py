from sas_format_extractor import SAS7BDATFormatExtractor
import os
from io import BytesIO


def get_file_path(filename):
    files_directory = os.path.join(os.getcwd(), "tests/python/resources")
    file_path = os.path.join(files_directory, filename)
    return file_path


def get_file_schema(filename):
    file_path = get_file_path(filename)
    with open(file_path, "rb") as f:
        stream = BytesIO(f.read())
        sas_format_extractor = SAS7BDATFormatExtractor(stream)
        schema = sas_format_extractor.read_schema()
    return schema


def get_file_rows(filename):
    file_path = get_file_path(filename)
    with open(file_path, "rb") as f:
        stream = BytesIO(f.read())
        sas_format_extractor = SAS7BDATFormatExtractor(stream)
        rows = []
        row = sas_format_extractor.read_row()
        while row is not None:
            rows.append(row)
            row = sas_format_extractor.read_row()
    return rows


def list_equal(l_1, l_2):
    if len(l_1) != len(l_2):
        return False
    for i in range(len(l_1)):
        if l_1[i] != l_2[i]:
            return False
    return True


def test_airline_file_schema():
    filename = "airline.sas7bdat"
    schema = get_file_schema(filename)
    expected_schema = [
        {'name': u'YEAR', 'type': 'STRING'},
        {'name': u'Y', 'type': 'STRING'},
        {'name': u'W', 'type': 'STRING'},
        {'name': u'R', 'type': 'STRING'},
        {'name': u'L', 'type': 'STRING'},
        {'name': u'K', 'type': 'STRING'}
    ]
    assert list_equal(schema, expected_schema) == True


def test_airline_file_rows():
    filename = "airline.sas7bdat"
    rows = get_file_rows(filename)
    assert len(rows) == 32
    expected_row_13 = {
        u'K': '1.7359999418258667',
        u'L': '3.121999979019165',
        u'R': '0.30790001153945923',
        u'W': '0.46000000834465027',
        u'Y': '5.465000152587891',
        u'YEAR': '1961.0'
    }
    assert rows[13] == expected_row_13


def test_cars_file_schema():
    filename = "cars.sas7bdat"
    schema = get_file_schema(filename)
    expected_schema = [
        {'name': u'MPG', 'type': 'STRING'},
        {'name': u'CYL', 'type': 'STRING'},
        {'name': u'ENG', 'type': 'STRING'},
        {'name': u'WGT', 'type': 'STRING'}
    ]
    assert list_equal(schema, expected_schema) == True


def test_cars_file_rows():
    filename = "cars.sas7bdat"
    rows = get_file_rows(filename)
    assert len(rows) == 392
    expected_row_234 = {
        u'MPG': '25.5',
        u'WGT': '2755.0',
        u'ENG': '140.0',
        u'CYL': '4.0'
    }
    assert rows[234] == expected_row_234


def test_datetime_file_schema():
    filename = "datetime.sas7bdat"
    schema = get_file_schema(filename)
    expected_schema = [
        {'name': u'Date1', 'type': 'STRING'},
        {'name': u'Date2', 'type': 'STRING'},
        {'name': u'DateTime', 'type': 'STRING'},
        {'name': u'DateTimeHi', 'type': 'STRING'},
        {'name': u'Taiw', 'type': 'STRING'}
    ]
    assert list_equal(schema, expected_schema) == True


def test_datetime_file_rows():
    filename = "datetime.sas7bdat"
    rows = get_file_rows(filename)
    assert len(rows) == 4
    expected_row_3 = {
        u'Date1': '2262-04-11',
        u'Taiw': '110404.0',
        u'Date2': '2262-04-11',
        u'DateTimeHi': '2262-04-11 23:47:16.854774',
        u'DateTime': '2262-04-11 23:47:16'
    }
    assert rows[3] == expected_row_3


def test_highdate_file_schema():
    filename = "highdate.sas7bdat"
    schema = get_file_schema(filename)
    expected_schema = [
        {'name': u'highDate_num', 'type': 'STRING'},
        {'name': u'highDate_date', 'type': 'STRING'}
    ]
    assert list_equal(schema, expected_schema) == True


def test_highdate_file_rows():
    filename = "highdate.sas7bdat"
    rows = get_file_rows(filename)
    assert len(rows) == 1
    expected_row = {u'highDate_num': '2936547.0', u'highDate_date': '9999-12-31'}
    assert rows[0] == expected_row




