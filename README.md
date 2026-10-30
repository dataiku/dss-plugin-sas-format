## Plugin information

This plugin provides a custom format reader to import the contents of a .sas7bdat or .xport data file into your DSS project.

This implementation will work with almost any SAS file but may be slower than the default one provided with DSS. It is provided by the Pandas library shipped with DSS, and may change when future Pandas versions are released.

## Original SAS schema

In **SAS using Pandas**, enable **Use original SAS schema** to extract column types from the file metadata for SAS7BDAT and XPORT files:

- SAS numeric columns become DSS `double` columns, even when all values are missing. SAS does not distinguish integer and floating-point storage types.
- SAS character columns become DSS `string` columns.
- Numeric columns with date formats recognized by Pandas become DSS `dateonly` columns. SAS dates count days from 1960-01-01.
- Numeric columns with datetime formats recognized by Pandas become DSS `datetimenotz` columns. SAS datetimes count seconds from 1960-01-01. Missing values remain empty.

SAS dates carry no timezone. They are imported without adding a timezone or shifting the recorded date or time, using DSS-compatible ISO 8601 values (`YYYY-MM-DD` for dates and `YYYY-MM-DDTHH:MM:SS` with optional fractional seconds for datetimes).

The option is disabled by default, including for existing datasets whose configuration does not contain it. With it disabled, schema detection and row values keep their previous behavior. The **SAS using sas7bdat** reader is unchanged.

For an existing dataset, enable the option and use DSS's schema detection again to update its stored schema. SAS display formats, value labels, and formats not recognized by Pandas are not reproduced.

## License

This library is distributed under the Apache License version 2.0
