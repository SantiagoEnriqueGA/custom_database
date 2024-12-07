
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
    - test_load: Tests loading the database from a file.
    - test_delete: Tests deleting the file after saving the
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Storage Class")

    def setUp(self):
        self.db = Database("TestDB")
        self.db.create_table("Users", ["id", "name", "email"])
        self.filename = "test_db.json"

    def tearDown(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def test_save(self):
        Storage.save(self.db, self.filename)
        self.assertTrue(os.path.exists(self.filename))

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

if __name__ == '__main__':
    unittest.main()