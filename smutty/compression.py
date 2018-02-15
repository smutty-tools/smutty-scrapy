import lzma


class LzmaCompression:

    @staticmethod
    def default_settings():
        return {
            "format": lzma.FORMAT_XZ,
            "check": lzma.CHECK_SHA256,
            "preset": lzma.PRESET_DEFAULT,
        }

    def __init__(self, outside_file_obj, file_mode):
        self._outside_file_obj = outside_file_obj
        self._file_mode = file_mode
        self._inside_fileobj = None

    def __enter__(self):
        self._inside_fileobj = lzma.LZMAFile(self._outside_file_obj, mode=self._file_mode, **self.default_settings())
        return self._inside_fileobj

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._inside_fileobj is not None:
            self._inside_fileobj.close()
