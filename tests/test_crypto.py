
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.crypto import CustomFernet

class TestCustomFernet(unittest.TestCase):
    """
    Unit tests for the CustomFernet class.
    Methods:
    - test_generate_key: Tests the key generation.
    - test_encrypt: Tests the encryption of data.
    - test_decrypt: Tests the decryption of data.
    """
    def setUp(self):
        self.key = CustomFernet.generate_key()
        self.fernet = CustomFernet(self.key)
        self.data = "This is a test."

    def test_generate_key(self):
        key = CustomFernet.generate_key()
        self.assertIsInstance(key, str)
        self.assertEqual(len(key), 44)  # 32 bytes encoded in base64 is 44 characters

    def test_encrypt(self):
        encrypted_data = self.fernet.encrypt(self.data)
        self.assertIsInstance(encrypted_data, str)
        self.assertNotEqual(encrypted_data, self.data)
        
    def test_decrypt(self):
        encrypted_data = self.fernet.encrypt(self.data)
        decrypted_data = self.fernet.decrypt(encrypted_data)
        self.assertEqual(decrypted_data, self.data)

if __name__ == '__main__':
    unittest.main()
