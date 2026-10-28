"""Generate matching SAS examples using ReadStat's C writer API.

Run with /private/tmp/sas-format-tests-py311/bin/python.
Only generated artifacts are written into dist/sas-samples.
"""
import ctypes as C
import datetime as dt
import json
import math
import re
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.io.sas.sas7bdat import SAS7BDATReader
from pandas.io.sas.sas_xport import XportReader


OUTPUT = Path('/Users/mayeul.rousselet@dataiku.com/dev/plugins/dss-plugin-sas-format/dist/sas-samples')
LIBRARY = Path('/private/tmp/sas-samples-build/libreadstat.dylib')
SAS_EPOCH = dt.datetime(1960, 1, 1)

# Names, labels and string widths respect XPORT V5 limits.
SCHEMA = [
    ('ID', 'Observation identifier', 0, 'BEST12.'),
    ('TEXT', 'Text, accents, punctuation, empty', 64, ''),
    ('LONGTXT', 'Long character values', 200, ''),
    ('CODE', 'String codes preserving leading zeros', 12, ''),
    ('INTEGER', 'Integer-valued SAS numeric', 0, 'BEST12.'),
    ('BIGINT', 'Exact integers up to 2**53 minus 1', 0, 'BEST32.'),
    ('DOUBLE', 'Double precision numeric', 0, 'BEST32.'),
    ('SCIENTIF', 'Small and large scientific values', 0, 'E24.16'),
    ('BOOLEAN', 'Boolean convention: 0 false, 1 true', 0, '1.'),
    ('DATEVAL', 'Date: days since 1960-01-01', 0, 'DATE9.'),
    ('DATEISO', 'Same date displayed in ISO format', 0, 'E8601DA10.'),
    ('DATETIME', 'Datetime: seconds since 1960-01-01', 0, 'DATETIME26.6'),
    ('TIMEVAL', 'Time of day: seconds since midnight', 0, 'TIME15.6'),
    ('MONEY', 'Numeric with a currency format', 0, 'DOLLAR16.2'),
    ('PERCENT', 'Numeric with a percent format', 0, 'PERCENT12.2'),
    ('MISSNUM', 'Entirely missing numeric column', 0, 'BEST12.'),
    ('MISSTXT', 'Entirely blank character column', 16, ''),
    ('SPECIAL', 'All 28 SAS numeric missing codes', 0, 'BEST12.'),
    ('CASE', 'Description of the observation', 80, ''),
]


def make_rows():
    texts = ['Bonjour', 'café, été, Noël', 'naïve façade', '0', 'True', '',
             '   ', None, 'NULL', 'NaN', 'virgule, point; "guillemets"',
             'ligne 1\nligne 2', 'tab\ttexte', 'texte avec espaces']
    dates = [dt.datetime(1960, 1, 1), dt.datetime(1959, 12, 31),
             dt.datetime(1970, 1, 1), dt.datetime(2000, 2, 29),
             dt.datetime(2024, 2, 29), None, dt.datetime(1900, 1, 1),
             dt.datetime(2099, 12, 31)]
    integers = [0, 1, -1, 42, -42, None, 2**31 - 1, -(2**31)]
    bigints = [0, 2**53 - 1, -(2**53 - 1), 2**40, -(2**40), None]
    doubles = [0.0, 1.5, -2.75, math.pi, 0.1, None, float('nan'), 123456.789]
    scientific = [1e-20, -1e-20, 1e20, -1e20, 1e-10, None, 1e10, 0.0]
    tagged = ['_', '.'] + list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    rows = []
    for i in range(32):
        date = dates[i % len(dates)]
        dateval = (date - SAS_EPOCH).days if date is not None else None
        stamp = (date + dt.timedelta(hours=13, minutes=45, seconds=12,
                                     microseconds=125000)) if date is not None else None
        special = ('tag', tagged[i]) if i < len(tagged) and tagged[i] != '.' else None
        row = [i + 1, texts[i % len(texts)],
               ('Texte long accentué : ' + 'Abc0123456789-' * 12) if i % 4 else None,
               ['000001', '001020', 'A-0007', None][i % 4],
               integers[i % len(integers)], bigints[i % len(bigints)],
               doubles[i % len(doubles)], scientific[i % len(scientific)],
               [1, 0, None, 1][i % 4], dateval, dateval,
               (stamp - SAS_EPOCH).total_seconds() if stamp is not None else None,
               [0.0, 1.125, 45296.5, 86399.999, None][i % 5],
               [1234.56, -99.95, 0.0, None][i % 4],
               [0.0, 0.125, 1.0, -0.05, None][i % 5],
               None, None, special,
               f'SAS missing .{tagged[i]}' if i < len(tagged) and tagged[i] != '.'
               else ('SAS missing .' if i == 1 else 'Ordinary numeric value')]
        if i >= len(tagged):
            row[17] = [0.0, 42.0, None, None][i - len(tagged)]
        if i == 30:
            row = [31] + [None] * (len(SCHEMA) - 1)
        if i == 31:
            row[6] = float('nan')
            row[18] = 'Python NaN becomes SAS system missing .'
        rows.append(row)
    return rows


def bind(lib, name, result, *args):
    func = getattr(lib, name)
    func.restype = result
    func.argtypes = list(args)
    return func


def write_file(path, rows, xport=False):
    lib = C.CDLL(str(LIBRARY))
    ptr = C.c_void_p
    init = bind(lib, 'readstat_writer_init', ptr)
    free = bind(lib, 'readstat_writer_free', None, ptr)
    errmsg = bind(lib, 'readstat_error_message', C.c_char_p, C.c_int)
    callback_type = C.CFUNCTYPE(C.c_ssize_t, ptr, C.c_size_t, ptr)
    set_writer = bind(lib, 'readstat_set_data_writer', C.c_int, ptr, callback_type)
    add_var = bind(lib, 'readstat_add_variable', ptr, ptr, C.c_char_p, C.c_int, C.c_size_t)
    set_label = bind(lib, 'readstat_variable_set_label', None, ptr, C.c_char_p)
    set_format = bind(lib, 'readstat_variable_set_format', None, ptr, C.c_char_p)
    set_version = bind(lib, 'readstat_writer_set_file_format_version', C.c_int, ptr, C.c_uint8)
    set_table = bind(lib, 'readstat_writer_set_table_name', C.c_int, ptr, C.c_char_p)
    set_file_label = bind(lib, 'readstat_writer_set_file_label', C.c_int, ptr, C.c_char_p)
    begin = bind(lib, 'readstat_begin_writing_xport' if xport else 'readstat_begin_writing_sas7bdat', C.c_int, ptr, ptr, C.c_long)
    begin_row = bind(lib, 'readstat_begin_row', C.c_int, ptr)
    end_row = bind(lib, 'readstat_end_row', C.c_int, ptr)
    end = bind(lib, 'readstat_end_writing', C.c_int, ptr)
    missing = bind(lib, 'readstat_insert_missing_value', C.c_int, ptr, ptr)
    tagged = bind(lib, 'readstat_insert_tagged_missing_value', C.c_int, ptr, ptr, C.c_char)
    number = bind(lib, 'readstat_insert_double_value', C.c_int, ptr, ptr, C.c_double)
    string = bind(lib, 'readstat_insert_string_value', C.c_int, ptr, ptr, C.c_char_p)

    def check(code):
        if code:
            raise RuntimeError(errmsg(code).decode())

    writer = init()
    try:
        with path.open('wb') as output:
            @callback_type
            def write_bytes(data, length, context):
                return output.write(C.string_at(data, length))

            check(set_writer(writer, write_bytes))
            check(set_table(writer, b'ALLTYPES'))
            check(set_file_label(writer, b'Synthetic data: types and missing values'))
            if xport:
                check(set_version(writer, 5))
            variables = []
            for name, label, width, fmt in SCHEMA:
                var = add_var(writer, name.encode(), 0 if width else 5, width or 8)
                if not var:
                    raise RuntimeError(f'Unable to create {name}')
                variables.append(var)
                set_label(var, label.encode())
                if fmt:
                    # ReadStat expects no terminal dot when storing a format.
                    set_format(var, fmt.rstrip('.').encode())
            check(begin(writer, None, len(rows)))
            for row in rows:
                check(begin_row(writer))
                for spec, var, value in zip(SCHEMA, variables, row):
                    if value is None or (isinstance(value, float) and math.isnan(value)):
                        check(missing(writer, var))
                    elif isinstance(value, tuple):
                        check(tagged(writer, var, value[1].encode()))
                    elif spec[2]:
                        check(string(writer, var, value.encode('utf-8')))
                    else:
                        check(number(writer, var, float(value)))
                check(end_row(writer))
            check(end(writer))
    finally:
        free(writer)


def verify(rows):
    # Pandas is independent from the ReadStat writer.
    with SAS7BDATReader(OUTPUT / 'all_types.sas7bdat',
                                       encoding='utf-8', convert_dates=False, blank_missing=False) as reader:
        sas = reader.read()
        formats = dict(zip(reader.column_names, reader.column_formats))
    with XportReader(OUTPUT / 'all_types.xport', encoding='utf-8') as reader:
        xpt = reader.read()
    pd.testing.assert_frame_equal(sas, xpt, check_exact=False, rtol=1e-14, atol=1e-14)
    assert sas.shape == (32, len(SCHEMA))
    for i, row in enumerate(rows):
        for j, (value, spec) in enumerate(zip(row, SCHEMA)):
            got = sas.iloc[i, j]
            if spec[2]:
                assert got == (value or '').rstrip(), (i, spec[0], got, value)
            elif value is None or isinstance(value, tuple) or math.isnan(value):
                assert pd.isna(got), (i, spec[0], got)
            else:
                assert np.isclose(got, value, rtol=1e-14, atol=0), (i, spec[0], got, value)
    for name, _, width, fmt in SCHEMA:
        if fmt:
            actual = formats[name]
            if isinstance(actual, bytes):
                actual = actual.decode()
            expected_name = re.sub(r'\d+(?:\.\d+)?\.?$', '', fmt)
            assert actual == expected_name, (name, actual, expected_name)
    dates = pd.read_sas(OUTPUT / 'all_types.sas7bdat', encoding='utf-8')
    assert dates.loc[0, 'DATEVAL'] == pd.Timestamp('1960-01-01')
    assert dates.loc[1, 'DATEVAL'] == pd.Timestamp('1959-12-31')
    assert dates.loc[3, 'DATEVAL'] == pd.Timestamp('2000-02-29')
    assert dates.loc[0, 'DATETIME'] == pd.Timestamp('1960-01-01 13:45:12.125')
    assert pd.isna(dates.loc[5, 'DATETIME'])
    sas.to_csv(OUTPUT / 'preview.csv', index=False)
    result = {'rows': len(rows), 'columns': len(SCHEMA),
              'verification': 'Pandas reads both files; numeric and text values match the source and each other; SAS7BDAT date conversion checked.',
              'sas7bdat_formats': {k: v.decode() if isinstance(v, bytes) else v for k, v in formats.items()},
              'numeric_missing_counts': {name: int(sas[name].isna().sum()) for name, _, width, _ in SCHEMA if not width}}
    (OUTPUT / 'validation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    for path in (OUTPUT / 'all_types.xport', OUTPUT / 'all_types.sas7bdat'):
        print(path, path.stat().st_size, 'bytes')


if __name__ == '__main__':
    OUTPUT.mkdir(parents=True, exist_ok=True)
    data = make_rows()
    write_file(OUTPUT / 'all_types.xport', data, xport=True)
    write_file(OUTPUT / 'all_types.sas7bdat', data)
    verify(data)
