
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
    - setUpClass: Sets up the test class.
    - setUp: Sets up the test case.
    - test_init: Tests the initialization of the CustomFernet class.
    - test_init_no_key: Tests the initialization of the CustomFernet class without a key.
    - test_init_invalid_key: Tests the initialization of the CustomFernet class with an invalid key.
    - test_generate_key: Tests the key generation.
    - test_encrypt: Tests the encryption of data.
    - test_encrypt_no_data: Tests encryption of data without any data.
    - test_decrypt: Tests the decryption of data.
    - test_decrypt_no_key: Tests decryption of data with an incorrect key.
    - test_decrypt_invalid_key: Tests decryption of data with an invalid key.
    """
    @classmethod
    def setUpClass(cls):
        print("\nTesting CustomFernet Class", end="", flush=True)
    
    def setUp(self):
        self.key = CustomFernet.generate_key()
        self.fernet = CustomFernet(self.key)
        self.data = "This is a test."

    def test_init(self):
        self.assertIsInstance(self.fernet, CustomFernet)
            
    def test_init_no_key(self):
        with self.assertRaises(TypeError):
            CustomFernet()

    def test_init_invalid_key(self):
        with self.assertRaises(ValueError):
            CustomFernet("Invalid key")

    def test_generate_key(self):
        key = CustomFernet.generate_key()
        self.assertIsInstance(key, str)
        self.assertEqual(len(key), 44)  # 32 bytes encoded in base64 is 44 characters

    def test_encrypt(self):
        encrypted_data = self.fernet.encrypt(self.data)
        self.assertIsInstance(encrypted_data, str)
        self.assertNotEqual(encrypted_data, self.data)
    
    def test_encrypt_no_data(self):
        with self.assertRaises(TypeError):
            self.fernet.encrypt()
    
    def test_decrypt(self):
        encrypted_data = self.fernet.encrypt(self.data)
        decrypted_data = self.fernet.decrypt(encrypted_data)
        self.assertEqual(decrypted_data, self.data)
        
    def test_decrypt_no_key(self):
        encrypted_data = self.fernet.encrypt(self.data)
        fernet = CustomFernet(CustomFernet.generate_key())
        with self.assertRaises(ValueError):
            fernet.decrypt(encrypted_data)
        
    def test_decrypt_invalid_key(self):
        with self.assertRaises(ValueError):
            self.fernet.decrypt("Invalid key")

if __name__ == '__main__':
    unittest.main()
