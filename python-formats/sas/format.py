import os
import time
from collections import OrderedDict
import io
import pandas as pd

from dataiku.customformat import Formatter, FormatExtractor
from dataiku.base.utils import TmpFolder

class SASFormatter(Formatter):
    def __init__(self, config, plugin_config):
        Formatter.__init__(self, config, plugin_config)

    def get_output_formatter(self, stream, schema):
        raise NotImplementedError

    def get_format_extractor(self, stream, schema=None):
        return SASFormatExtractor(stream, schema, self.config)

 
class ForwardSeekStream2(io.RawIOBase):
    """A forward-seekable stream that properly keeps track of what it has already read"""
    def __init__(self, stream):
        self.stream = stream
        self.size_read = 0

    def read(self, size=-1):
        #print("FSS: Read sz=%s" % size)
        if size is None:
            size = -1
        if size < 0:
            return self.readall()
        b = bytearray(size.__index__())

        already_read = 0
        while True:
            #print("FSS: Calling readinto with already_read=%s" % already_read)

            if already_read == 0:
                n = self.readinto(b)
            else:
                    n = self.readinto(b[already_read:])
            #print("FSS: Now n=%s" % n)
            if n is None:
                return None

            if n <= 0:
                del b[already_read:]
                #print("FSS: Returning a buffer of length %s" % len(b))
                return bytes(b)

            already_read += n
            #print("FSS: now already_read=%s" % already_read)

            if already_read > size:
                raise Exception("Has read too much")
            elif already_read == size:
                return bytes(b)
    
    def readinto(self, b):
        #print("FSS: readinto: %s" % len(b))
        res = self.stream.readinto(b)
        #print("FSS: readinto did read: %s" % res)
        self.size_read += res
        return res

    def seek(self, seek, whence=0):
        #print("FSS: seek: %s %s" % (seek, whence))
        to_read = seek if whence == 1 else seek - self.size_read
        #print("FSS: seek: to_read=%s" % to_read)

        if to_read < 0:
            raise IOError("Only forward seeking is supported")
        elif to_read > 0:
            self.read(to_read)

class SASFormatExtractor(FormatExtractor):
    def __init__(self, stream, schema, config):
        FormatExtractor.__init__(self, stream)

        chunksize = int(config.get("chunksize", "10000"))
        sas_format = config.get("sas_format", "sas7bdat")
        encoding = config.get("encoding", "latin_1")
        dump_to_file = config.get("dump_to_file", False)

        self.sas_format = sas_format
        self.hasSchema = schema != None
        self.encoding = encoding

        if sas_format.lower() == 'xport' and not dump_to_file:
            buffer = io.BytesIO()
            for data in iter((lambda: stream.read(500000)), b''):
                buffer.write(data)
            buffer.seek(0)
            
            self.iterator = pd.read_sas(buffer,
                                        format=sas_format,
                                        iterator=True,
                                        encoding=encoding,
                                        chunksize=chunksize)
        elif dump_to_file:
            dirname, _ = os.path.split(os.path.abspath(__file__))
            with TmpFolder(dirname) as tmp_folder_path:
                extension = 'xpt' if sas_format.lower() == 'xport' else 'sas7bdat'
                fullpath = os.path.join(tmp_folder_path, 'dumped-%s.%s' % (time.time(), extension))
                with open(fullpath, 'wb') as of:
                    for data in iter((lambda: stream.read(500000)), b''):
                        of.write(data)
                # has to be within the with statements before the file is deleted
                self.iterator = pd.read_sas(fullpath,
                                            format=sas_format,
                                            iterator=True,
                                            encoding=encoding,
                                            chunksize=chunksize)
        else:
            read_from = ForwardSeekStream2(stream)
            
            self.iterator = pd.read_sas(read_from,
                                        format=sas_format,
                                        iterator=True,
                                        encoding=encoding,
                                        chunksize=chunksize)

        self.get_chunk()

    def get_chunk(self):
        # Fix for previewing when using DSS < 4.1.X
        if self.hasSchema:
            self.chunk = next(self.iterator).to_dict('records')
        else:
            self.chunk = [OrderedDict(row) for i, row in next(self.iterator).iterrows()]

        self.chunk_nb = 0

    def read_schema(self):
        # For XPORT format, we need to extract the schema from the fields
        if self.sas_format.lower() == 'xport':
            # checking if the iterator has fields or columns
            if hasattr(self.iterator, 'fields') and self.iterator.fields:
                schema = []
                for field in self.iterator.fields:
                    if isinstance(field, dict):
                        field_name = field.get('name', 'unknown')
                        field_type = "DOUBLE" if field.get('ntype') == 1 else "STRING"
                    else:
                        field_name = getattr(field, 'name', 'unknown')
                        field_type = "DOUBLE" if getattr(field, 'ntype', 2) == 1 else "STRING"

                    if isinstance(field_name, bytes):
                        field_name = field_name.decode(self.encoding)
                    
                    schema.append({"name": field_name, "type": field_type})
                return schema
            else:
                if hasattr(self.iterator, 'columns'):
                    schema = []
                    for col_name in self.iterator.columns:
                        if isinstance(col_name, bytes):
                            col_name = col_name.decode(self.encoding)
                        schema.append({"name": col_name, "type": "STRING"})
                    return schema
        else:
            # For SAS7BDAT
            # checking if the iterator has columns
            if hasattr(self.iterator, 'columns') and len(self.iterator.columns) > 0:
                if hasattr(self.iterator.columns[0], 'name'):
                    return [{"name": c.name, "type": "DOUBLE" if c.ctype == 'd' else "STRING"} for c in self.iterator.columns]
        
        return []

    def read_row(self):
        try:
            if self.chunk_nb >= len(self.chunk):
                self.get_chunk()

            self.chunk_nb += 1
            return self.chunk[self.chunk_nb - 1]

        except StopIteration:
            return None
