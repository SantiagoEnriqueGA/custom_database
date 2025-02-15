
import unittest
from unittest.mock import MagicMock, Mock, ANY
import sys
import os

from joblib import Logger

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb import table
from segadb.table import Table
from segadb.database import Database
from test_utils import suppress_print

class TestTable(unittest.TestCase):
    """
    Unit tests for the Table class.
    Methods:
    # Initialization and Configuration
        - test_init: Test the initialization of the Table class.
        - test_init_logging: Test the initialization of the Table class with logging.
        - test_add_constraintLambda: Test adding a lambda constraint to the Table.
        - test_add_constraintUnique: Test adding a UNIQUE constraint to the Table.
        - test_add_constraintForeignKey: Test adding a FOREIGN_KEY constraint to the Table.
        - test_add_constraintForeignKeyFail: Test adding a FOREIGN_KEY constraint to the Table with missing column.
    # CRUD Operations
        - test_insert: Test the insert method of the Table class.
        - test_insert_fail: Test the insert method of the Table class with a typo in the column name.
        - test_try_insert: Test the try_insert method of the Table class.
        - test_try_insert_fail: Test the try_insert method of the Table class with a constraint violation.
        - test_bulk_insert: Test the bulk_insert method of the Table class.
        - test_delete: Test the delete method of the Table class.
        - test_delete_miss: Test the delete method of the Table class with a missing record.
        - test_update: Test the update method of the Table class.
        - test_update_miss: Test the update method of the Table class with a missing record.
        - test_select: Test the select method of the Table class.
        - test_select_miss: Test the select method of the Table class with no results.
    # Table Operations
        - test_join: Test the join method of the Table class.
        - test_join_miss: Test the join method of the Table class with no results.
        - test_aggregate_MIN: Test the aggregate method of the Table class with MIN.
        - test_aggregate_MAX: Test the aggregate method of the Table class with MAX.
        - test_aggregate_COUNT: Test the aggregate method of the Table class with COUNT.
        - test_aggregate_COUNT_DISTINCT: Test the aggregate method of the Table class with COUNT_DISTINCT.
        - test_aggregate_SUM: Test the aggregate method of the Table class with SUM.
        - test_aggregate_AVG: Test the aggregate method of the Table class with AVG.
        - test_aggregate_fail: Test the aggregate method of the Table class with an invalid operation.
        - test_filter: Test the filter method of the Table class.
        - test_sort: Test the sort method of the Table class.
    # Utility Methods
        - test_print_table: Test the print_table method of the Table class.
        - test_print_table_lim: Test the print_table method of the Table class with a limit.
        - test_print_table_pretty: Test the print_table method of the Table class with pretty printing.
    # Logging
        - test_logging: Test the logging of the Table class.
        - test_logging_override: Test the logging of the Table class with logging override
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Table Class")

    def setUp(self):
        self.table = Table("Users", ["id", "name", "email"])

    # Initialization and Configuration
    # ---------------------------------------------------------------------------------------------
    def test_init(self):
        self.assertEqual(self.table.name, "Users")
        self.assertEqual(self.table.columns, ["id", "name", "email"])
        self.assertEqual(self.table.records, [])
        
    def test_init_logging(self):
        table = Table("Users", ["id", "name", "email"], logger=Logger)
        self.assertEqual(table.name, "Users")
        self.assertEqual(table.columns, ["id", "name", "email"])
        self.assertEqual(table.records, [])
        self.assertEqual(table.logger, Logger)
    
    def test_add_constraintLambda(self):
        with suppress_print():    
            self.table.add_constraint("email", lambda x: "@" in x)
            self.assertIn("email", self.table.constraints)
            self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)
            self.table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})
            self.assertEqual(len(self.table.records), 1)  # Should not insert due to constraint violation
    
    def test_add_constraintUnique(self):
        with suppress_print():    
            self.table.add_constraint("email", 'UNIQUE')
            self.assertIn("email", self.table.constraints)
            self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)
            self.table.try_insert({"name": "Jane Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)  # Should not insert due to UNIQUE constraint violation
            
    def test_add_constraintForeignKey(self):
        with suppress_print():    
            table2 = Table("Authed Users", ["id", "user_id", "name"])
            table2.insert({"user_id": 1, "name": "John Doe"})
            
            self.table.add_constraint("name", 'FOREIGN_KEY', table2, "name")
            self.assertIn("id", self.table.constraints)
            self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)
            self.table.try_insert({"name": "Jane Doe", "email": "jane@example.com"})
            self.assertEqual(len(self.table.records), 1)  # Should not insert due to FOREIGN_KEY constraint violation
            
    def test_add_constraintForeignKeyFail(self):
        with self.assertRaises(ValueError):
            self.table.add_constraint("email", 'FOREIGN_KEY')

    def test_add_constraintInvalidFunction(self):
        with self.assertRaises(ValueError):
            # Invalid constraint function ie. not a callable
            self.table.add_constraint("email", 123)

        with self.assertRaises(ValueError):
            # Invalid constraint function ie. function with wrong number of parameters
            self.table.add_constraint("email", lambda x, y: x > y)                    

            
    # CRUD Operations
    # ---------------------------------------------------------------------------------------------
    def test_insert(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.assertEqual(len(self.table.records), 1)
        self.assertEqual(self.table.records[0].data["name"], "John Doe")
    
    def test_insert_fail(self):
        with self.assertRaises(ValueError):
            # Should fail due typo in column name
            self.table.insert({"name": "John Doe", "emails": "john@example.com"})

    def test_try_insert(self):
        self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
        self.assertEqual(len(self.table.records), 1)
        self.assertEqual(self.table.records[0].data["name"], "John Doe")        
        
    def test_try_insert_fail(self):
        with suppress_print():
            self.table.add_constraint("email", lambda x: "@" in x)
            self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
            self.assertEqual(len(self.table.records), 1)
            self.table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})
            self.assertEqual(len(self.table.records), 1)  # Should not insert due to constraint violation
    
    def test_bulk_insert(self):
        self.table.bulk_insert([{"name": "John Doe", "email": "john@example.com"},
                                {"name": "Jane Doe", "email": "john@example.com"},
                                {"name": "Jane Doe", "email": "john@example.com"},
                                {"name": "Jane Doe", "email": "john@example.com"},
                                {"name": "Jane Doe", "email": "john@example.com"}])
        self.assertEqual(len(self.table.records), 5)
        self.assertEqual(self.table.records[0].data["name"], "John Doe")          
        
    def test_delete(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.delete(1)
        self.assertEqual(len(self.table.records), 0)
        
    def test_delete_miss(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.delete(10)
        # ensure no changes
        self.assertEqual(len(self.table.records), 1)

    def test_update(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.update(1, {"name": "Jane Doe", "email": "jane@example.com"})
        self.assertEqual(self.table.records[0].data["name"], "Jane Doe")
        
    def test_update_miss(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.update(10, {"name": "Jane Doe", "email": "john@example.com"})
        # ensure no changes
        self.assertEqual(self.table.records[0].data["name"], "John Doe")

    def test_select(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        results = self.table.select(lambda record: record.data["name"] == "Jane Doe")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].data["name"], "Jane Doe")
        
    def test_select_miss(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        results = self.table.select(lambda record: record.data["name"] == "Bob Doe")
        self.assertEqual(len(results), 0)
            
    # Table Operations
    # ---------------------------------------------------------------------------------------------
    def test_join(self):
        table2 = Table("Authed Users", ["id", "user_id", "name"])
        table2.insert({"user_id": 1, "name": "John Doe"})
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "john@example.com"})
        results_tbl = self.table.join(table2, "name", "name")
        self.assertEqual(len(results_tbl.records), 1)
        
    def test_join_miss(self):
        table2 = Table("Authed Users", ["id", "user_id", "name"])
        table2.insert({"user_id": 1, "name": "John Doe"})
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "john@example.com"})
        results_tbl = self.table.join(table2, "name", "email")
        self.assertEqual(len(results_tbl.records), 0)
        
    def test_aggregate_MIN(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.aggregate("id", "name", "MIN")
        self.assertEqual(len(results_tbl.records), 1)

    def test_aggregate_MAX(self):        
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.aggregate("id", "name", "MAX")
        self.assertEqual(len(results_tbl.records), 1)
        
    def test_aggregate_COUNT(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.aggregate("name", "name", "COUNT")
        self.assertEqual(len(results_tbl.records), 2)
    
    def test_aggregate_COUNT_DISTINCT(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.aggregate("name", "name", "COUNT_DISTINCT")
        self.assertEqual(len(results_tbl.records), 2)
        
    def test_aggregate_SUM(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.aggregate("id", "id", "SUM")
        self.assertEqual(len(results_tbl.records), 1)
    
    def test_aggregate_AVG(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.aggregate("id", "id", "AVG")
        self.assertEqual(len(results_tbl.records), 1)
        
    def test_aggregate_fail(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        with self.assertRaises(ValueError):
            self.table.aggregate("id", "id", "INVALID")
    
    def test_filter(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.filter(lambda record: "1234" in record.data["name"])
        self.assertEqual(len(results_tbl.records), 1)
        
    def test_sort(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe 1234", "email": "john@example.com"})
        results_tbl = self.table.sort("name")
        self.assertEqual(results_tbl.records[0].data["name"], "Jane Doe 1234")
    
    # Utility Methods
    # ---------------------------------------------------------------------------------------------  
    def test_print_table(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        with suppress_print():
            self.table.print_table()
    
    def test_print_table_lim(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        with suppress_print():
            self.table.print_table(limit=1)

    def test_print_table_pretty(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        with suppress_print():
            self.table.print_table(pretty=True)
    
    
    # Logging
    # ---------------------------------------------------------------------------------------------  
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