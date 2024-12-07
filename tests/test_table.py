
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.table import Table
from test_utils import suppress_print

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

if __name__ == '__main__':
    unittest.main()