
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.record import Record, VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord
from test_utils import suppress_print

class TestRecord(unittest.TestCase):
    """
    Unit tests for the Record class.
    Methods:
    - test_record_creation: Tests the creation of a record with an ID and data.
    - test_vector_record: Tests the VectorRecord class and its methods.
    - test_time_series_record: Tests the TimeSeriesRecord class and its methods.
    - test_image_record: Tests the ImageRecord class and its methods.
    - test_text_record: Tests the TextRecord class and its
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Record Class")

    def test_record_creation(self):
        record = Record(1, {"name": "John Doe"})
        self.assertEqual(record.id, 1)
        self.assertEqual(record.data["name"], "John Doe")

    def test_vector_record(self):
        import math
        
        vector_record = VectorRecord(1, {"vector": [1, 2, 3]})
        self.assertEqual(vector_record.magnitude(), math.sqrt(14))
        self.assertEqual(vector_record.normalize(), [1/math.sqrt(14), 2/math.sqrt(14), 3/math.sqrt(14)])
        self.assertEqual(vector_record.dot_product([4, 5, 6]), 32)

    def test_time_series_record(self):
        time_series_record = TimeSeriesRecord(1, {"time_series": [1, 2, 3, 4, 5]})
        self.assertEqual(time_series_record.moving_average(3), [2.0, 3.0, 4.0])

    def test_image_record(self):
        import tempfile
        from PIL import Image

        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_image:
            image = Image.new("RGB", (100, 100), color="red")
            image.save(temp_image, format="PNG")
            temp_image_path = temp_image.name

        image_record = ImageRecord(1, {"image_data": temp_image_path})
        self.assertEqual(image_record.image_size, os.path.getsize(temp_image_path))
        resized_image = image_record.resize(0.5)
        self.assertEqual(resized_image.size, (50, 50))
        os.remove(temp_image_path)

    def test_text_record(self):
        text_record = TextRecord(1, {"text": "Hello World"})
        self.assertEqual(text_record.word_count(), 2)
        self.assertEqual(text_record.to_uppercase(), "HELLO WORLD")
        self.assertEqual(text_record.to_lowercase(), "hello world")

if __name__ == '__main__':
    unittest.main()