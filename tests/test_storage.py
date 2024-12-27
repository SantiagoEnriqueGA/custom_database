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
    - test_compress_decompress: Tests compressing and decompressing data.
    - test_backup: Tests creating a backup of the database.
    - test_list_backups: Tests listing the backups of the database.
    - test_restore: Tests restoring the database from a backup.
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Storage Class")

    def setUp(self):
        self.db = Database("TestDB")
        self.db.create_table("Users", ["id", "name", "email"])
        self.filename = "test_db.segadb"
        
        # Ensure tests_backups, tests_backups_list, and tests_backups_restore directories are empty
        if os.path.exists("tests_backups"):
            for file in os.listdir("tests_backups"):
                os.remove(os.path.join("tests_backups", file))
        if os.path.exists("tests_backups_list"):
            for file in os.listdir("tests_backups_list"):
                os.remove(os.path.join("tests_backups_list", file))
        if os.path.exists("tests_backups_restore"):
            for file in os.listdir("tests_backups_restore"):
                os.remove(os.path.join("tests_backups_restore", file))        

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
        self.assertIsInstance(key, str)
        self.assertEqual(len(key), 44)  # Fernet keys are 44 bytes long

    def test_encrypt_decrypt(self):
        key = Storage.generate_key()
        data = "This is a test."
        encrypted_data = Storage.encrypt(data, key)
        decrypted_data = Storage.decrypt(encrypted_data, key)
        self.assertEqual(decrypted_data, data)
        
    def test_compress_decompress(self):
        table = self.db.get_table("Users")
        table.insert({"user_id": 1, "name": "Alice", "email": "alice@abc.com"})
        table.insert({"user_id": 2, "name": "Bob", "email": "bob@abc.com"})
        table.insert({"user_id": 3, "name": "Charlie", "email": "charlie@abc.com"})
        table.insert({"user_id": 4, "name": "David", "email": "david@abc.com"})
        Storage.save(self.db, self.filename, compress=True)
        loaded_db = Storage.load(self.filename, compression=True)
        self.assertEqual(loaded_db.name, "TestDB")
        self.assertEqual(loaded_db.tables.keys(), self.db.tables.keys())
        self.assertEqual(loaded_db.get_table("Users").columns, self.db.get_table("Users").columns)
        self.assertEqual(len(loaded_db.get_table("Users").records), len(self.db.get_table("Users").records))
        self.assertEqual(loaded_db.get_table("Users").constraints, self.db.get_table("Users").constraints)    

    def test_backup(self):
        Storage.backup(self.db, dir="tests_backups")
        self.assertTrue(os.path.exists("tests_backups/TestDB_backup_0.segadb"))
        os.remove(sys.path[0] + "/tests_backups/TestDB_backup_0.segadb")
    
    def test_list_backups(self):
        Storage.backup(self.db, dir="tests_backups_list")
        Storage.backup(self.db, dir="tests_backups_list")
        Storage.backup(self.db, dir="tests_backups_list")
        backups = Storage.list_backups("tests_backups_list", print_output=False)
        self.assertEqual(len(backups), 3)
        os.remove(sys.path[0] + "/tests_backups_list/TestDB_backup_0.segadb")
        os.remove(sys.path[0] + "/tests_backups_list/TestDB_backup_1.segadb")
        os.remove(sys.path[0] + "/tests_backups_list/TestDB_backup_2.segadb")
    
    def test_restore(self):
        Storage.backup(self.db, dir="tests_backups_restore")
        Storage.restore(self.db, dir="tests_backups_restore")
        self.assertTrue(os.path.exists("tests_backups_restore/TestDB_backup_0.segadb"))
        os.remove(sys.path[0] + "/tests_backups_restore/TestDB_backup_0.segadb")

if __name__ == '__main__':
    unittest.main()