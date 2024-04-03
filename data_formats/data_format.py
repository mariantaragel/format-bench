from .file_utils import FileUtils

class DataFormat:

    filename: str
    pathname: str
    compression: any

    def __init__(self, compression) -> None:
        self.compression = compression
        self.pathname = None

    def __repr__(self) -> str:
        return self.format_name

    def save(self):
        pass

    def parallel_save(self):
        pass

    def read(self):
        pass

    def parallel_read(self):
        pass

    def remove(self):
        FileUtils.remove_file(self.filename)

    def remove_all_files(self):
        FileUtils.remove_all_files(self.pathname, self.filename)

    def size(self):
        return FileUtils.size_of_file(self.filename)
    
    def size_all_files(self):
        return FileUtils.size_all_files(self.pathname)
