from sas7bdat import SAS7BDAT
from dataiku.customformat import Formatter, FormatExtractor


class SASFormatter(Formatter):
    def __init__(self, config, plugin_config):
        Formatter.__init__(self, config, plugin_config)

    def get_output_formatter(self, stream, schema):
        raise NotImplementedError

    def get_format_extractor(self, stream, schema=None):
        return SASFormatExtractor(stream, schema, self.config)


class SASFormatExtractor(FormatExtractor):
    """
    Reads a stream in a format to a stream of rows
    """
    def __init__(self, stream, schema, config):
        """
        Initialize the extractor
        :param stream: the stream to read the formatted data from
        """
        FormatExtractor.__init__(self, stream)
        with SAS7BDAT(path="", fh=self.stream, skip_header=False) as sas_handler:
            self.iterator = sas_handler.readlines()
            self.columns = next(self.iterator)

            # example to retrieve some special types
            # self.sas_columns = sas_handler.columns

    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        # example to retrieve some special types
        # return [{"name": column.name, "type": "STRING" if column.format != "DATE" else "DATE"} for column in self.sas_columns]
        return [{"name": column_name, "type": "STRING"} for column_name in self.columns]

    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        line = next(self.iterator, None)
        if line is not None:
            row = {}
            for i, value in enumerate(line):
                row[self.columns[i]] = str(value)
            return row
        return None
