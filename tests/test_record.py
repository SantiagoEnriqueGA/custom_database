import unittest
from unittest.mock import Mock
import sys
import os
import math
import tempfile
from PIL import Image

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.record import Record, VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord, EncryptedRecord
from segadb.crypto import CustomFernet
from tests.utils import suppress_print

class TestRecord(unittest.TestCase):
    """
    Unit tests for the Record class and its subclasses.
    - setUpClass: Sets up the test class.
    - test_record_creation: Tests the creation of a Record instance.
    - test_add_to_index: Tests adding an item to the Record index.
    - test_remove_from_index: Tests removing an item from the Record index.
    - test_vector_record: Tests the creation of a VectorRecord instance.
    - test_vector_record_methods: Tests methods of the VectorRecord class.
    - test_time_series_record: Tests the creation of a TimeSeriesRecord instance.
    - test_time_series_record_methods: Tests methods of the TimeSeriesRecord class.
    - test_image_record: Tests the creation of an ImageRecord instance.
    - test_image_record_methods: Tests methods of the ImageRecord class.
    - test_text_record: Tests the creation of a TextRecord instance.
    - test_text_record_methods: Tests methods of the TextRecord class.
    - test_encrypted_record: Tests the creation and decryption of an EncryptedRecord instance.
    - test_encrypted_record_no_key: Tests decryption of an EncryptedRecord instance with an incorrect key.
    """
    @classmethod
    def setUpClass(cls):
        print("\nTesting Record Class", end="", flush=True)

    def test_record_creation(self):
        record = Record(1, {"name": "John Doe"})
        self.assertEqual(record.id, 1)
        self.assertEqual(record.data["name"], "John Doe")
        
    # REMOVED test_add_to_index
    # def test_add_to_index(self):
    #     record = Record(1, {"name": "John Doe"})
    #     record.add_to_index(100)
    #     self.assertIn(100, record.index.to_dict())

    # REMOVED test_remove_from_index
    # def test_remove_from_index(self):
    #     record = Record(100, {"name": "John Doe"})
    #     record.add_to_index(100)
    #     record.remove_from_index(100)
    #     self.assertNotIn(100, record.index.to_dict()) 
    
    def test_vector_record(self):        
        vector_record = VectorRecord(1, {"vector": [1, 2, 3]})
        self.assertEqual(vector_record.vector, [1, 2, 3])
    
    def test_vector_record_methods(self):
        vector_record = VectorRecord(1, {"vector": [1, 2, 3]})
        self.assertEqual(vector_record.magnitude(), math.sqrt(14))
        self.assertEqual(vector_record.normalize(), [1/math.sqrt(14), 2/math.sqrt(14), 3/math.sqrt(14)])
        self.assertEqual(vector_record.dot_product([4, 5, 6]), 32)

    def test_time_series_record(self):
        time_series_record = TimeSeriesRecord(1, {"time_series": [1, 2, 3, 4, 5]})
        self.assertEqual(time_series_record.time_series, [1, 2, 3, 4, 5])
        
    def test_time_series_record_methods(self):
        time_series_record = TimeSeriesRecord(1, {"time_series": [1, 2, 3, 4, 5]})
        self.assertEqual(time_series_record.moving_average(3), [2.0, 3.0, 4.0])

    def test_image_record(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_image:
            image = Image.new("RGB", (100, 100), color="red")
            image.save(temp_image, format="PNG")
            temp_image_path = temp_image.name

        image_record = ImageRecord(1, {"image_data": temp_image_path})
        self.assertTrue(image_record.image_data, temp_image_path)
    
    def test_image_record_methods(self):
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
        self.assertEqual(text_record.text, "Hello World")
        
    def test_text_record_methods(self):
        text_record = TextRecord(1, {"text": "Hello World"})
        self.assertEqual(text_record.word_count(), 2)
        self.assertEqual(text_record.to_uppercase(), "HELLO WORLD")
        self.assertEqual(text_record.to_lowercase(), "hello world")
        
    def test_encrypted_record(self):
        key = CustomFernet.generate_key()
        encrypted_record = EncryptedRecord(1, {"data": "Hello World", "key": key})
        self.assertEqual(encrypted_record.decrypt(key), "Hello World")
        
    def test_encrypted_record_no_key(self):
        """Test decrypting with an incorrect/invalid key raises ValueError."""
        original_data = "my secret data"
        key = CustomFernet.generate_key() # Generate a valid key for encryption
        encrypted_record = EncryptedRecord(1, {"data": original_data, "key": key})

        # Assert that calling decrypt with an invalid key raises ValueError
        with self.assertRaisesRegex(ValueError, "Decryption failed for record 1"):
            encrypted_record.decrypt("wrong_key") # Pass an invalid key

        # Optional: Verify decryption still works with the correct key
        decrypted_data = encrypted_record.decrypt(key)
        self.assertEqual(decrypted_data, original_data)
    
if __name__ == '__main__':
    unittest.main()