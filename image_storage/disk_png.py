from .image_storage import ImageStorage
from PIL import Image
import numpy as np
import glob
import csv
import os

class PngImage(ImageStorage):

    filename = "tmp/test.csv"
    format_name = "PNG"
    pathname = "tmp/*.png"

    def save(self, images, labels):
        ids = []
        for id, img_arr in enumerate(images):
            Image.fromarray(img_arr).save(f"tmp/{id}.png")
            ids.append(id)

        with open(self.filename, "w", newline="") as csvfile:
            csv_witer = csv.writer(csvfile, quotechar=None, quoting=csv.QUOTE_NONE)
            csv_witer.writerow(["id", "label"])
            csv_witer.writerows(list(zip(ids, labels)))

    def read(self):
        with open(self.filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")

            labels, images = [], []
            for line_number, line in enumerate(csv_reader):
                if line_number == 0:
                    continue
                else:
                    id = line[0]
                    label = line[1]
                    labels.append(label)

                    img = Image.open(f"tmp/{id}.png")
                    img_arr = np.asarray(img)
                    images.append(img_arr)
        
        return images, labels
    
    def size(self):
        total_size = super().size()

        files = glob.glob(self.pathname)
        for filename in files:
            total_size += os.path.getsize(filename)

        return total_size
    
    def remove(self):
        super().remove()

        files = glob.glob(self.pathname)
        for filename in files:
            if os.path.exists(filename):
                os.remove(filename)