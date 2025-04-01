
import unittest
from unittest.mock import Mock
from unittest.mock import MagicMock
import logging
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.databasePartial import PartialDatabase
from segadb.database import Database
from segadb.storage import Storage
from tests.utils import suppress_print

class TestDatabasePartial(unittest.TestCase):
    """
    Unit tests for the DatabasePartial class.
    Methods:
    - setUp: Initializes a new instance of the PartialDatabase class before each test method is run.
    - test_get_table: Tests the get_table method of the PartialDatabase class.
    - test_active_tables: Tests the active_tables method of the PartialDatabase class.
    - test_dormant_tables: Tests the dormant_tables method of the PartialDatabase class.
    - test_deactivate_table: Tests the deactivate_table method of the PartialDatabase class.
    - test_print_db: Tests the print_db method of the PartialDatabase class.
    """
    @classmethod
    def setUpClass(cls):
        print("\nTesting DatabasePartial Class", end="", flush=True)

    def setUp(self):
        db = Database.load_sample_database(n_users=100, n_orders=200)
        Storage.save(db, "example_storage/database_partial.segadb")
        self.db = PartialDatabase("Partial Database", "example_storage/database_partial.segadb")
        self.db._load_table_from_storage = MagicMock()
        self.db._check_permission = MagicMock()

    def test_get_table(self):
        self.db._load_table_from_storage.return_value = Mock()
        table = self.db.get_table("test_table", session_token="dummy_token")
        self.db._check_permission.assert_called_with("dummy_token", "read_table")
        self.db._load_table_from_storage.assert_called_with("test_table")
        self.assertIn("test_table", self.db.loaded_tables)

    def test_active_tables(self):
        self.db.loaded_tables = {"table1": Mock(), "table2": Mock()}
        active_tables = self.db.active_tables()
        self.assertEqual(active_tables, ["table1", "table2"])

    def test_dormant_tables(self):
        self.db.loaded_tables = {"table1": Mock()}
        with open("example_storage/database_partial.segadb", 'w') as f:
            f.write('{"tables": ["table1", "table2", "table3"]}')
        dormant_tables = self.db.dormant_tables()
        self.assertEqual(dormant_tables, ["table2", "table3"])
        
    def test_deactivate_table(self):
        users_table = self.db.get_table("orders")
        self.db.deactivate_table("orders")
        self.assertNotIn("orders", self.db.loaded_tables)       

    def test_print_db(self):
        with suppress_print():
            self.db.print_db()
        self.assertTrue(True)  # If no exception is raised, the test passes

if __name__ == '__main__':
    unittest.main()