
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database
from segadb.storage import Storage

class TestStorage(unittest.TestCase):
    """
    Unit tests for the Storage class.
    Methods:
    - setUp: Initializes a new instance of the Database class and a filename before each test method is run.
    - tearDown: Deletes the test file after each test method is run.
    - test_save: Tests saving the database to a file.
    - test_save_encrypted: Tests saving the database to a file with encryption.
    - test_load: Tests loading the database from a file.
    - test_save_encrypted_decrypt: Tests saving the database to a file with encryption and then loading it.
    - test_delete: Tests deleting the file after saving the
    - test_db_to_dict: Tests converting a database to a dictionary.
    - test_generate_key: Tests generating a key for encryption.
    - test_encrypt_decrypt: Tests encrypting and decrypting data.
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Storage Class")

    def setUp(self):
        self.db = Database("TestDB")
        self.db.create_table("Users", ["id", "name", "email"])
        self.filename = "test_db.segadb"

    def tearDown(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def test_save(self):
        Storage.save(self.db, self.filename)
        self.assertTrue(os.path.exists(self.filename))
        
    def test_save_encrypted(self):
        key = Storage.generate_key()
        Storage.save(self.db, self.filename, key)
        self.assertTrue(os.path.exists(self.filename))
    
    def test_save_encrypted_decrypt(self):
        key = Storage.generate_key()
        Storage.save(self.db, self.filename, key)
        loaded_db = Storage.load(self.filename, key)
        self.assertEqual(loaded_db.name, "TestDB")
        self.assertEqual(loaded_db.tables.keys(), self.db.tables.keys())
        self.assertEqual(loaded_db.get_table("Users").columns, self.db.get_table("Users").columns)
        self.assertEqual(len(loaded_db.get_table("Users").records), len(self.db.get_table("Users").records))
        self.assertEqual(loaded_db.get_table("Users").next_id, self.db.get_table("Users").next_id)
        self.assertEqual(loaded_db.get_table("Users").constraints, self.db.get_table("Users").constraints)

    def test_load(self):
        Storage.save(self.db, self.filename)
        loaded_db = Storage.load(self.filename)
        self.assertEqual(loaded_db.name, "TestDB")
        self.assertEqual(loaded_db.tables.keys(), self.db.tables.keys())
        self.assertEqual(loaded_db.get_table("Users").columns, self.db.get_table("Users").columns)
        self.assertEqual(len(loaded_db.get_table("Users").records), len(self.db.get_table("Users").records))
        self.assertEqual(loaded_db.get_table("Users").next_id, self.db.get_table("Users").next_id)
        self.assertEqual(loaded_db.get_table("Users").constraints, self.db.get_table("Users").constraints)

    def test_delete(self):
        Storage.save(self.db, self.filename)
        Storage.delete(self.filename)
        self.assertFalse(os.path.exists(self.filename))

    def test_db_to_dict(self):
        db_dict = Storage.db_to_dict(self.db)
        self.assertEqual(db_dict["name"], "TestDB")
        self.assertIn("Users", db_dict["tables"])
        self.assertEqual(db_dict["tables"]["Users"]["columns"], ["id", "name", "email"])

    def test_generate_key(self):
        key = Storage.generate_key()
        self.assertIsInstance(key, bytes)
        self.assertEqual(len(key), 44)  # Fernet keys are 44 bytes long

    def test_encrypt_decrypt(self):
        key = Storage.generate_key()
        data = "This is a test."
        encrypted_data = Storage.encrypt(data, key)
        decrypted_data = Storage.decrypt(encrypted_data, key)
        self.assertEqual(decrypted_data, data)

if __name__ == '__main__':
    unittest.main()