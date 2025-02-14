
import unittest
from unittest.mock import MagicMock, Mock, ANY
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.table import Table
from segadb.database import Database
from test_utils import suppress_print

# TODO; Extend Table tests

class TestTable(unittest.TestCase):
    """
    Unit tests for the Table class.
    Methods:
    - setUp: Initializes a Table instance with the name "Users" and columns ["id", "name", "email"].
    - test_insert: Tests that a record can be inserted into the table and verifies the inserted data.
    - test_delete: Tests that a record can be deleted from the table and verifies the deletion.
    - test_update: Tests that a record can be updated in the table and verifies the updated data.
    - test_select: Tests that records can be selected from the table based on a condition and verifies the selected data.
    - test_try_insert: Tests that a record can be inserted into the table with constraints and verifies the inserted data.
    - test_print_table: Tests that the table can be printed with a limit and pretty format.
    - test_add_constraint1: Tests that a constraint can be added to a column and verifies the constraint.
    - test_add_constraint2: Tests that a UNIQUE constraint can be added to a column and verifies the constraint.
    - test_logging: Tests that table operations are logged when table logging is enabled.
    - test_logging_override: Tests that table operations are logged when table logging is disabled and logging override is enabled
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Table Class")

    def setUp(self):
        self.table = Table("Users", ["id", "name", "email"])

    def test_insert(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.assertEqual(len(self.table.records), 1)
        self.assertEqual(self.table.records[0].data["name"], "John Doe")

    def test_delete(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.delete(1)
        self.assertEqual(len(self.table.records), 0)

    def test_update(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.update(1, {"name": "Jane Doe", "email": "jane@example.com"})
        self.assertEqual(self.table.records[0].data["name"], "Jane Doe")

    def test_select(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        results = self.table.select(lambda record: record.data["name"] == "Jane Doe")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].data["name"], "Jane Doe")

    def test_try_insert(self):
        with suppress_print():
            self.table.add_constraint("email", lambda x: "@" in x)
            self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)
            self.table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})
            self.assertEqual(len(self.table.records), 1)  # Should not insert due to constraint violation

    def test_print_table(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        with suppress_print():
            self.table.print_table(limit=1)
            self.table.print_table(pretty=True)
        
    def test_add_constraint1(self):
        with suppress_print():    
            self.table.add_constraint("email", lambda x: "@" in x)
            self.assertIn("email", self.table.constraints)
            self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)
            self.table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})
            self.assertEqual(len(self.table.records), 1)  # Should not insert due to constraint violation
    
    def test_add_constraint2(self):
        with suppress_print():    
            self.table.add_constraint("email", 'UNIQUE')
            self.assertIn("email", self.table.constraints)
            self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)
            self.table.try_insert({"name": "Jane Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)  # Should not insert due to UNIQUE constraint violation
            
    def test_logging(self):
        db = Database("TestDB_table", db_logging=True, table_logging=True)
        db.logger = MagicMock()
        
        db.create_table("users", ["username", "password_hash", "roles"])
        db.add_constraint("users", "username", lambda x: len(x) > 3)
        db.get_table("users").insert({"username": "john", "password_hash": "1234", "roles": ["admin"]})
        
        # Ensure Method Calls are logged
        db.logger.info.assert_any_call("Method Call: create_table | Args: {'table_name': 'users', 'columns': ['username', 'password_hash', 'roles']}")
        
        # Ensure Table Operations are logged
        db.logger.info.assert_any_call('Table Log: users | Method Complete: add_constraint | Status: Success')
        db.logger.info.assert_any_call("Table Log: users | Method Call: insert | Args: {'data': {'username': 'john', 'password_hash': '1234', 'roles': ['admin']}}")
        db.logger.info.assert_any_call('Table Log: users | Method Complete: insert | Status: Success')

    def test_logging_override(self):
        db = Database("TestDB_table", db_logging=True, table_logging=False)
        db.logger = MagicMock()
        
        db.create_table("users", ["username", "password_hash", "roles"], logging_override=True)
        db.add_constraint("users", "username", lambda x: len(x) > 3)
        db.get_table("users").insert({"username": "john", "password_hash": "1234", "roles": ["admin"]})
            
        # Ensure Method Calls are logged
        db.logger.info.assert_any_call("Method Call: create_table | Args: {'table_name': 'users', 'columns': ['username', 'password_hash', 'roles'], 'logging_override': True}")
        
        # Ensure Table Operations are logged
        db.logger.info.assert_any_call('Table Log: users | Method Complete: add_constraint | Status: Success')
        db.logger.info.assert_any_call("Table Log: users | Method Call: insert | Args: {'data': {'username': 'john', 'password_hash': '1234', 'roles': ['admin']}}")
        db.logger.info.assert_any_call('Table Log: users | Method Complete: insert | Status: Success')
        

if __name__ == '__main__':
    unittest.main()