
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database
from segadb.transaction import Transaction
from tests.utils import suppress_print

class TestTransaction(unittest.TestCase):
    """
    Unit tests for the Transaction class.
    Methods:
    - setUp: Initializes a new instance of the Database and Transaction classes before each test method is run.
    - test_begin: Tests the begin method of the Transaction class.
    - test_commit: Tests the commit method of the Transaction class.
    - test_rollback: Tests the rollback method of the Transaction class.
    """
    @classmethod
    def setUpClass(cls):
        print("\nTesting Transaction Class", end="", flush=True)

    def setUp(self):
        self.db = Database("TestDB")
        self.db.create_table("Users", ["id", "name", "email"])
        self.transaction = Transaction(self.db)

    def test_begin(self):
        self.transaction.begin()
        self.assertIsNotNone(self.db.shadow_copy)

    def test_commit(self):
        self.transaction.begin()
        self.db.get_table("Users").insert({"name": "John Doe", "email": "john@example.com"})
        self.transaction.add_operation(lambda: self.db.get_table("Users").insert({"name": "Jane Doe", "email": "jane@example.com"}))
        self.transaction.commit()
        self.assertIsNone(self.db.shadow_copy)
        self.assertEqual(len(self.db.get_table("Users").records), 2)

    def test_rollback(self):
        self.db.get_table("Users").insert({"name": "John Doe", "email": "john@example.com"})
        self.transaction.begin()
        self.transaction.add_operation(lambda: self.db.get_table("Users").insert({"name": "Jane Doe2", "email": "jane@example.com"}))
        self.transaction.rollback()
        self.assertIsNone(self.db.shadow_copy)
        self.assertEqual(len(self.db.get_table("Users").records), 1)

if __name__ == '__main__':
    unittest.main()