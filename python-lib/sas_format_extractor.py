from sas7bdat import SAS7BDAT

class SAS7BDATFormatExtractor:
    """
    Reads a stream in a format to a stream of rows
    """
    def __init__(self, stream):
        """
        Initialize the extractor
        :param stream: the stream to read the formatted data from
        """
        self.stream = stream
        self.iterator = iter(SAS7BDAT(path="", fh=self.stream, skip_header=False))
        self.columns = next(self.iterator)
    
    def read_schema(self):
        """
        Get the schema of the data in the stream, if the schema can be known upfront.
        """
        return [{"name": column_name, "type": "STRING"} for column_name in self.columns]

    def read_row(self):
        """
        Read one row from the formatted stream
        :returns: a dict of the data (name, value), or None if reading is finished
        """
        if self.stream.closed:
            self.iterator.close()
            return None
        line = next(self.iterator, None)
        if line is not None:
            row = {}
            for i, value in enumerate(line):
                if value is None:
                    row[self.columns[i]] = None
                if str(value) == "9999-12-29":
                    row[self.columns[i]] = "9999-12-31"
                else:
                    row[self.columns[i]] = str(value)
            return row
        self.iterator.close()
        return None
