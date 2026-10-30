"""Exercise the Pandas reader without requiring a running DSS instance."""
import importlib.util
import io
import json
import struct
import sys
import tempfile
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[3]
RESOURCES = ROOT / "tests/python/resources"


@pytest.fixture
def formatter_module(monkeypatch):
    customformat = ModuleType("dataiku.customformat")

    class Formatter:
        def __init__(self, config, plugin_config):
            self.config = config
            self.plugin_config = plugin_config

    class FormatExtractor:
        def __init__(self, stream):
            self.stream = stream

    customformat.Formatter = Formatter
    customformat.FormatExtractor = FormatExtractor
    utils = ModuleType("dataiku.base.utils")
    utils.TmpFolder = lambda dirname: tempfile.TemporaryDirectory()
    for name, module in {
        "dataiku": ModuleType("dataiku"),
        "dataiku.customformat": customformat,
        "dataiku.base": ModuleType("dataiku.base"),
        "dataiku.base.utils": utils,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("sas_pandas_format", ROOT / "python-formats/sas/format.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extract(module, filename, config, schema=None):
    with (RESOURCES / filename).open("rb") as stream:
        extractor = module.SASFormatter(config, {}).get_format_extractor(stream, schema)
        detected_schema = extractor.read_schema()
        rows = list(iter(extractor.read_row, None))
        extractor.iterator.close()
    return detected_schema, rows


@pytest.mark.parametrize("filename", ["cars.sas7bdat", "airline.sas7bdat", "datetime.sas7bdat", "highdate.sas7bdat"])
@pytest.mark.parametrize("option", [{}, {"use_original_schema": False}])
@pytest.mark.parametrize("schema", [None, {"columns": [{"name": "MPG", "type": "string"}]}])
def test_existing_sas7bdat_behavior(formatter_module, filename, option, schema):
    detected, rows = extract(formatter_module, filename, dict(option, chunksize=2), schema)
    # Pandas stores ctype as bytes: the existing implementation reports strings.
    frame = pd.read_sas(RESOURCES / filename, encoding="latin_1")
    assert detected == [{"name": name, "type": "STRING"} for name in frame.columns]
    assert rows == frame.to_dict("records")
    assert isinstance(rows[0], OrderedDict) == (schema is None)


@pytest.mark.parametrize("filename,count", [("cars.sas7bdat", 392), ("airline.sas7bdat", 32)])
@pytest.mark.parametrize("dump", [False, True])
@pytest.mark.parametrize("schema", [None, {"columns": [{"name": "MPG", "type": "double"}]}])
def test_original_numeric_schema(formatter_module, filename, count, dump, schema):
    detected, rows = extract(formatter_module, filename, {
        "use_original_schema": True, "chunksize": 2, "dump_to_file": dump,
    }, schema)
    frame = pd.read_sas(RESOURCES / filename, encoding="latin_1")
    assert detected == [{"name": name, "type": "DOUBLE"} for name in frame.columns]
    assert len(rows) == count
    assert rows == frame.to_dict("records")
    json.dumps(rows, allow_nan=False)


@pytest.mark.parametrize("filename", ["datetime.sas7bdat", "highdate.sas7bdat"])
@pytest.mark.parametrize("dump", [False, True])
@pytest.mark.parametrize("schema", [None, {"columns": [{"name": "Date1", "type": "date"}]}])
def test_original_dates(formatter_module, filename, dump, schema):
    detected, rows = extract(formatter_module, filename, {
        "use_original_schema": True, "chunksize": 1, "dump_to_file": dump,
    }, schema)
    frame = pd.read_sas(RESOURCES / filename, encoding="latin_1")
    expected_types = (["DOUBLE", "DATEONLY"] if filename == "highdate.sas7bdat"
                      else ["DATEONLY", "DATEONLY", "DATETIMENOTZ", "DATETIMENOTZ", "DATEONLY"])
    assert [column["type"] for column in detected] == expected_types
    expected = []
    for row in frame.to_dict("records"):
        expected.append({
            name: (value.date().isoformat() if column_type == "DATEONLY" else value.isoformat())
            if isinstance(value, datetime) else value
            for (name, value), column_type in zip(row.items(), expected_types)
        })
    assert rows == expected
    if filename == "highdate.sas7bdat":
        assert rows[0]["highDate_date"].startswith("9999-")
    json.dumps(rows, allow_nan=False)


@pytest.mark.parametrize("dump", [False, True])
@pytest.mark.parametrize("schema", [None, {"columns": [{"name": "NUMBER", "type": "double"}]}])
def test_original_xport_schema_and_values(formatter_module, dump, schema):
    detected, rows = extract(formatter_module, "mixed.xpt", {
        "sas_format": "xport", "use_original_schema": True, "chunksize": 1, "dump_to_file": dump,
    }, schema)
    assert detected == [
        {"name": "NUMBER", "type": "DOUBLE"},
        {"name": "TEXT", "type": "STRING"},
        {"name": "DATE", "type": "DATEONLY"},
        {"name": "DATETIME", "type": "DATETIMENOTZ"},
    ]
    assert rows == [
        {"NUMBER": 1.5, "TEXT": "caf\u00e9", "DATE": "1960-01-01", "DATETIME": "1960-01-01T00:00:00"},
        {"NUMBER": -2.0, "TEXT": "second", "DATE": "2020-01-01", "DATETIME": "1960-01-01T00:00:01.500000"},
        {"NUMBER": None, "TEXT": "missing", "DATE": None, "DATETIME": None},
    ]
    json.dumps(rows, allow_nan=False)


@pytest.mark.parametrize("dump", [False, True])
@pytest.mark.parametrize("schema", [None, {"columns": [{"name": "DATE", "type": "date"}]}])
@pytest.mark.parametrize("sas_format", ["sas7bdat", "xport"])
def test_original_out_of_range_dates(formatter_module, tmp_path, dump, schema, sas_format):
    # Simulate a datetime value accidentally stored with a date (days) format.
    if sas_format == "sas7bdat":
        filename = "highdate.sas7bdat"
        original = struct.pack("<d", 2936547.0)
        invalid = struct.pack("<d", 2026117800.0)
        date_column = "highDate_date"
    else:
        filename = "mixed.xpt"
        # XPORT stores numbers as IBM hexadecimal floats: 21915 -> 2026117800.
        original = bytes.fromhex("44559b0000000000")
        invalid = bytes.fromhex("4878c41aa8000000")
        date_column = "DATE"

    contents = (RESOURCES / filename).read_bytes()
    assert original in contents
    path = tmp_path / filename
    path.write_bytes(contents.replace(original, invalid))

    detected, rows = extract(formatter_module, path, {
        "sas_format": sas_format, "use_original_schema": True,
        "chunksize": 1, "dump_to_file": dump,
    }, schema)
    assert {"name": date_column, "type": "DATEONLY"} in detected
    assert rows[0 if sas_format == "sas7bdat" else 1][date_column] is None
    if sas_format == "xport":
        assert len(rows) == 3
        assert rows[0]["DATE"] == "1960-01-01"
        assert rows[1]["DATETIME"] == "1960-01-01T00:00:01.500000"
        assert rows[2]["TEXT"] == "missing"
    else:
        assert rows[0]["highDate_num"] == 2026117800.0
    json.dumps(rows, allow_nan=False)


@pytest.mark.parametrize("option", [{}, {"use_original_schema": False}])
@pytest.mark.parametrize("dump", [False, True])
def test_existing_xport_behavior(formatter_module, option, dump):
    detected, rows = extract(formatter_module, "mixed.xpt", dict(option, sas_format="xport", chunksize=1, dump_to_file=dump))
    assert detected == [{"name": name, "type": "STRING"} for name in ["NUMBER", "TEXT", "DATE", "DATETIME"]]
    frame = pd.read_sas(RESOURCES / "mixed.xpt", format="xport", encoding="latin_1")
    pd.testing.assert_frame_equal(pd.DataFrame(rows), frame)


@pytest.mark.parametrize("sas_format,numeric", [("sas7bdat", "d"), ("sas7bdat", b"d"), ("xport", 1), ("xport", "numeric")])
@pytest.mark.parametrize("empty", [False, True])
def test_schema_comes_from_metadata_not_sample(formatter_module, monkeypatch, sas_format, numeric, empty):
    frame = pd.DataFrame({"num": [float("nan")], "text": [None], "date": [pd.NaT], "datetime": [pd.NaT]})
    if sas_format == "sas7bdat":
        columns = [
            SimpleNamespace(name=b"num", ctype=numeric, format=""),
            SimpleNamespace(name="text", ctype=b"s", format="DATE"),
            SimpleNamespace(name="date", ctype=numeric, format=b"DATE"),
            SimpleNamespace(name="datetime", ctype=numeric, format=b"DATETIME"),
        ]
        fields = []
    else:
        columns = list(frame.columns)
        fields = [
            {"name": b"num", "ntype": numeric},
            {"name": "text", "ntype": "char", "nform": "DATE"},
            SimpleNamespace(name="date", ntype=numeric, nform=b"DATE"),
            SimpleNamespace(name="datetime", ntype=numeric, nform=b"DATETIME"),
        ]

    class Reader:
        def __init__(self):
            self.columns = columns
            self.fields = fields
            self.chunks = iter([] if empty else [frame])

        def __next__(self):
            return next(self.chunks)

    monkeypatch.setattr(formatter_module.pd, "read_sas", lambda *args, **kwargs: Reader())
    extractor = formatter_module.SASFormatExtractor(io.BytesIO(), None, {
        "sas_format": sas_format, "use_original_schema": True,
    })
    assert extractor.read_schema() == [
        {"name": "num", "type": "DOUBLE"},
        {"name": "text", "type": "STRING"},
        {"name": "date", "type": "DATEONLY"},
        {"name": "datetime", "type": "DATETIMENOTZ"},
    ]
    assert list(iter(extractor.read_row, None)) == ([] if empty else [{"num": None, "text": None, "date": None, "datetime": None}])


def test_option_is_disabled_by_default():
    descriptor = json.loads((ROOT / "python-formats/sas/format.json").read_text())
    option = next(param for param in descriptor["params"] if param["name"] == "use_original_schema")
    assert option["type"] == "BOOLEAN"
    assert option["defaultValue"] is False
