class ImageDataset:

    images: list
    labels: list
    name: str

    def __init__(self, name, images, labels) -> None:
        self.name = name
        self.images = images
        self.labels = labels

    def __repr__(self) -> str:
        return self.name