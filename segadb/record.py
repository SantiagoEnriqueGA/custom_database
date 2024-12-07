from .index import Index

import math
from PIL import Image
import io

class Record:
    def __init__(self, record_id, data):
        """
        Initializes a new instance of the class.
        Args:
            record_id (int): The unique identifier for the record.
            data (dict): The data associated with the record.
        """
        self.id = record_id
        self.data = data
        self._index = Index()
        
    @property
    def index(self):
        """
        Returns the index of the record. 
        @property decorator is used to make the method behave like an attribute. (Read-only)
        """
        return self._index

    def add_to_index(self, key):
        self._index.add(key, self)

    def remove_from_index(self, key):
        self._index.remove(key, self)
        
class VectorRecord(Record):
    def __init__(self, record_id, vector):
        """
        Initializes a new instance of the VectorRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            vector (list): The vector data associated with the record.
        """
        super().__init__(record_id, vector)

    @property
    def vector(self):
        return self.data["vector"]

    def magnitude(self):
        """
        Calculates the magnitude of the vector.
        Returns:
            float: The magnitude of the vector.
        """       
        return math.sqrt(sum(float(x)**2 for x in self.vector))

    def normalize(self):
        """
        Normalizes the vector.
        Returns:
            list: The normalized vector.
        """
        mag = self.magnitude()
        if mag == 0:
            return [0] * len(self.vector)
        return [x / mag for x in self.vector]

    def dot_product(self, other_vector):
        """
        Calculates the dot product with another vector.
        Args:
            other_vector (list): The other vector to calculate the dot product with.
        Returns:
            float: The dot product of the two vectors.
        """
        return sum(x * y for x, y in zip(self.vector, other_vector))
    
        
class TimeSeriesRecord(Record):
    def __init__(self, record_id, time_series):
        super().__init__(record_id, time_series)

    @property
    def time_series(self):
        return self.data["time_series"]

    def moving_average(self, window_size):
        """
        Calculates the moving average of the time series.
        Args:
            window_size (int): The window size for the moving average.
        Returns:
            list: The moving average of the time series.
        """
        return [sum(self.time_series[i:i+window_size]) / window_size for i in range(len(self.time_series) - window_size + 1)]


class ImageRecord(Record):
    def __init__(self, record_id, image_path):
        """
        Initializes a new instance of the ImageRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            image_path (str): The file path to the image.
        """
        image_path = image_path['image_data']
        
        with open(image_path, "rb") as image_file:
            image_data = image_file.read()
        super().__init__(record_id, {"image_data": image_data, "image_path": image_path})

    @property
    def image_data(self):
        return self.data["image_data"]
    
    @property
    def image_path(self):
        return self.data["image_path"]
    
    @property
    def image_size(self):
        """
        Returns the size of the image in bytes.

        """
        return len(self.image_data)

    def get_image(self):
        """
        Converts the image data to a PIL Image object.
        Returns:
            Image: The PIL Image object.
        """
        return Image.open(io.BytesIO(self.image_data))

    def resize(self, percentage):
        """
        Resizes the image by a given percentage.
        Args:
            percentage (float): The percentage to increase or decrease the size of the image.
        Returns:
            Image: The resized PIL Image object.
        """
        image = self.get_image()
        width, height = image.size
        new_width = int(width * percentage)
        new_height = int(height * percentage)
        resized_image = image.resize((new_width, new_height))
        return resized_image
        
class TextRecord(Record):
    def __init__(self, record_id, text):
        """
        Initializes a new instance of the TextRecord class.
        Args:
            record_id (int): The unique identifier for the record.
            text (str): The text data associated with the record.
        """
        super().__init__(record_id, text)

    @property
    def text(self):
        return self.data["text"]

    def word_count(self):
        """
        Counts the number of words in the text.
        Returns:
            int: The number of words in the text.
        """
        return len(self.text.split())

    def to_uppercase(self):
        """
        Converts the text to uppercase.
        Returns:
            str: The text in uppercase.
        """
        return self.text.upper()
    
    def to_lowercase(self):
        """
        Converts the text to lowercase.
        Returns:
            str: The text in lowercase.
        """
        return self.text.lower()