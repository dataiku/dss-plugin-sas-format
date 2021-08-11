from dataiku.customformat import Formatter, FormatExtractor
from sas_format_extractor import SAS7BDATFormatExtractor


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
        self.sas_format_extractor = SAS7BDATFormatExtractor(self.stream)

    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        return self.sas_format_extractor.read_schema()

    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        return self.sas_format_extractor.read_row()
