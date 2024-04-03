import os

class ImageStorage:

    def __init__(self) -> None:
        pass

    def __repr__(self) -> str:
        return self.format_name

    def save(self):
        pass

    def read(self):
        pass

    def size(self):
        return os.path.getsize(self.filename)

    def remove(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)