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

        self.hasSchema = schema != None

        # necessary to handle the case where the stream is not seekable => force dump_to_file
        if sas_format.lower() == 'xport' and not dump_to_file:
            print("Warning: XPORT format detected, forcing dump_to_file mode for better compatibility")
            dump_to_file = True

        if dump_to_file:
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
        if hasattr(self.iterator, 'fields'):  # XPORT format
            return [{"name": f.name, "type": "DOUBLE" if f.ntype == 'numeric' else "STRING"} for f in self.iterator.fields]
        else:  # SAS7BDAT format
            return [{"name": c.name, "type": "DOUBLE" if c.ctype == 'd' else "STRING"} for c in self.iterator.columns]

    def read_row(self):
        try:
            if self.chunk_nb >= len(self.chunk):
                self.get_chunk()

            self.chunk_nb += 1
            return self.chunk[self.chunk_nb - 1]

        except StopIteration:
            return None
