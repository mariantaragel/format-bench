from .image_dataset import ImageDataset
from .gen_dtype import GenDtype
from PIL import Image
import pandas as pd
import numpy as np
import pickle
import random
import glob
import math
import io

class DataSet:

    @staticmethod
    def gen_data_set(
            entries: int,
            int_cols: int,
            float_cols: int,
            bool_cols: int,
            str_fixed_cols: int,
            str_var_cols: int,
            str_word_cols: int
        ) -> pd.DataFrame:
        gd = GenDtype()
        
        num_of_cols = int_cols + float_cols + bool_cols + str_fixed_cols + str_var_cols + str_word_cols
        cols = [f"col{i}" for i in range(num_of_cols)]
        
        data = []
        for i in range(int_cols):
            data.append(gd.gen_int_col(0, 100, entries))
        for i in range(float_cols):
            data.append(gd.gen_float_col(0, 100, entries))
        for i in range(bool_cols):
            data.append(gd.gen_bool_col(entries))
        for i in range(str_fixed_cols):
            data.append(gd.gen_fixed_str_col(10, entries))
        for i in range(str_var_cols):
            data.append(gd.gen_var_str_col(0, 10, entries))
        for i in range(str_word_cols):
            data.append(gd.gen_str_words(entries))

        random.shuffle(data)
        data = (dict(zip(cols, data)))

        return pd.DataFrame(data)
    
    @staticmethod
    def load_cifar_10(entries: int) -> ImageDataset:
        pathname = "./data/cifar-10/*_batch*"
        labels, images = [], []
        number_of_batches = math.ceil(entries / 10000)

        for batch_file in glob.glob(pathname)[:number_of_batches]:

            with open(batch_file, "rb") as pickle_file:
                batch = pickle.load(pickle_file, encoding="bytes")
                data_batch = batch[b"data"]
                label_batch = batch[b"labels"]
                labels += label_batch

                for img_list in data_batch:
                    img_array = np.array(img_list)
                    rgb = []

                    for i in range(3):
                        rgb.append(img_array[i * 1024 : 1024 * (i + 1)].reshape(32, 32))

                    img_raw = np.dstack(rgb)
                    images.append(img_raw)

        return ImageDataset("Cifar-10", images[:entries], labels[:entries])
    
    @staticmethod
    def load_imagenet_100(entries: int) -> ImageDataset:
        df = pd.read_parquet("./data/imagenet-100/ImageNet0-5.parquet")
        images_bytes = df["image"][:entries]
        labels = df["label"][:entries].to_list()
        images = []

        for img in images_bytes:
            image = Image.open(io.BytesIO(img["bytes"]))
            arr = np.asarray(image)
            images.append(arr)

        return ImageDataset("Imagetnet-100", images, labels)