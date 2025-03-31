
import unittest
from unittest.mock import MagicMock, Mock, ANY
import sys
import os

from joblib import Logger

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.table import Table
from segadb.database import Database
from tests.utils import suppress_print

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
    # CRUD Operations - Parallel
        - test_parallel_insert: Test basic parallel insertion functionality.
        - test_parallel_insert_custom_workers: Test parallel insert with custom configurations.
        - test_parallel_insert_custom_chunk_size: Test parallel insert with custom chunk size.
        - test_parallel_insert_empty: Test parallel insert with empty record list.
        - test_parallel_insert_with_ids: Test parallel insert with pre-existing IDs in data.
        - test_parallel_insert_large_dataset: Test parallel insert with a large number of records.
        - test_parallel_insert_constraint_violation: Test parallel insert with constraint violations.
        - test_parallel_try_insert: Test parallel try_insert with valid and invalid data.
        - test_parallel_try_insert_fail: Test parallel try_insert with constraint violations.
        - test_truncate_empty_table: Test truncating an empty table.
        - test_truncate_with_records: Test truncating a table with existing records.
        - test_truncate_and_reinsert: Test inserting records after truncating.
        - test_truncate_with_constraints: Test that constraints are preserved after truncate.
        - test_truncate_multiple_times: Test truncating table multiple times in succession.
        - test_truncate_and_parallel_insert: Test parallel insert after truncate.
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
        print("\nTesting Table Class", end="", flush=True)

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
        """Test inserting data with incorrect column names.""" # Updated docstring
        with self.assertRaisesRegex(ValueError, "Data columns mismatch table schema"): # Expect specific error
            # Should fail due typo in column name ("emails")
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
        """Test updating a record that does not exist.""" # Updated docstring
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        # Expect ValueError when updating a non-existent ID
        with self.assertRaisesRegex(ValueError, "Record with ID 10 not found"):
            self.table.update(10, {"name": "Jane Doe", "email": "john@example.com"})
        # ensure no changes to existing record
        record = self.table.get_record_by_id(1) # Use getter for clarity
        self.assertIsNotNone(record)
        if record: # Check added for type hinting
            self.assertEqual(record.data["name"], "John Doe")

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

    def test_truncate_empty_table(self):
        """Test truncating an empty table"""
        self.table.truncate()
        self.assertEqual(len(self.table.records), 0)
        self.assertEqual(self.table.next_id, 1)
        self.assertEqual(len(self.table.record_map), 0) # Check map length instead of index_cnt

    def test_truncate_with_records(self):
        """Test truncating a table with existing records"""
        records = [
            {"name": "John", "email": "john@example.com"},
            {"name": "Jane", "email": "jane@example.com"},
            {"name": "Bob", "email": "bob@example.com"}
        ]
        self.table.bulk_insert(records)
        self.assertGreater(len(self.table.records), 0)
        self.assertGreater(len(self.table.record_map), 0)

        self.table.truncate()
        self.assertEqual(len(self.table.records), 0)
        self.assertEqual(self.table.next_id, 1)
        self.assertEqual(len(self.table.record_map), 0) # Check map length

    def test_truncate_and_reinsert(self):
        """Test inserting records after truncating"""
        # Initial insert
        initial_records = [
            {"name": "Initial1", "email": "initial1@example.com"},
            {"name": "Initial2", "email": "initial2@example.com"}
        ]
        self.table.bulk_insert(initial_records)
        
        # Truncate
        self.table.truncate()
        
        # Insert new records
        new_records = [
            {"name": "New1", "email": "new1@example.com"},
            {"name": "New2", "email": "new2@example.com"}
        ]
        self.table.bulk_insert(new_records)
        
        # Verify new records start with ID 1
        self.assertEqual(len(self.table.records), 2)
        self.assertEqual(self.table.records[0].id, 1)
        self.assertEqual(self.table.records[1].id, 2)
        self.assertEqual(self.table.next_id, 3)

    def test_truncate_with_constraints(self):
        """Test that constraints are preserved after truncate"""
        # Add a constraint
        self.table.add_constraint("name", lambda x: len(x) <= 10)
        
        # Insert valid records
        self.table.insert({"name": "John", "email": "john@example.com"})
        
        # Truncate
        self.table.truncate()
        
        # Verify constraint still works
        with self.assertRaises(ValueError):
            with suppress_print():
                self.table.insert({"name": "VeryLongName", "email": "long@example.com"})

    def test_truncate_multiple_times(self):
        """Test truncating table multiple times in succession"""
        self.table.insert({"name": "John", "email": "john@example.com"})
        self.table.truncate()
        self.table.truncate()
        self.table.truncate()
        self.assertEqual(len(self.table.records), 0)
        self.assertEqual(self.table.next_id, 1)
        self.assertEqual(len(self.table.record_map), 0) # Check map length

    def test_truncate_and_parallel_insert(self):
        """Test parallel insert after truncate"""
        initial_records = [{"name": f"Initial{i}", "email": f"initial{i}@example.com"}
                        for i in range(100)]
        self.table.bulk_insert(initial_records)
        self.table.truncate()

        parallel_records = [{"name": f"Parallel{i}", "email": f"parallel{i}@example.com"}
                        for i in range(100)]
        self.table.parallel_insert(parallel_records)

        self.assertEqual(len(self.table.records), 100)
        self.assertEqual(self.table.record_map[1].id, 1) # Check map access
        self.assertEqual(self.table.next_id, 101)
        self.assertEqual(len(self.table.record_map), 100) # Check map length
            
    # CRUD Operations - Parallel
    # ---------------------------------------------------------------------------------------------
        # CRUD Operations - Parallel
    # ---------------------------------------------------------------------------------------------
    def test_parallel_insert(self):
        """Test basic parallel insertion functionality"""
        records = [{"name": f"John Doe{i}", "email": "john@example.com"} for i in range(100)]
        self.table.parallel_insert(records)
        self.assertEqual(len(self.table.records), 100)
        # Verify IDs are sequential
        ids = [record.id for record in self.table.records]
        self.assertEqual(ids, list(range(self.table.records[0].id, self.table.records[-1].id + 1)))

    def test_parallel_insert_custom_workers(self):
        """Test parallel insert with custom configurations"""
        records = [{"name": f"John Doe{i}", "email": "john@example.com"} for i in range(1000)]
        # Test with specific number of workers
        inserted = self.table.parallel_insert(records, max_workers=2)
        self.assertEqual(inserted, 1000)
        
    def test_parallel_insert_custom_chunk_size(self):
        records = [{"name": f"John Doe{i}", "email": "john@example.com"} for i in range(1000)]
        # Test with specific chunk size
        inserted = self.table.parallel_insert(records, chunk_size=100)
        self.assertEqual(inserted, 1000)

    def test_parallel_insert_empty(self):
        """Test parallel insert with empty record list"""
        with self.assertRaises(ValueError):
            self.table.parallel_insert([])

    def test_parallel_insert_with_ids(self):
        """Test parallel insert with pre-existing IDs in data"""
        records = [{"id": i, "name": f"John{i}", "email": "john@example.com"} for i in range(100)]
        self.table.parallel_insert(records)
        # Verify IDs are sequential and original IDs were ignored
        ids = [record.id for record in self.table.records]
        self.assertEqual(ids, list(range(self.table.records[0].id, self.table.records[-1].id + 1)))

    def test_parallel_insert_large_dataset(self):
        """Test parallel insert with a large number of records"""
        records = [{"name": f"User{i}", "email": f"user{i}@example.com"} for i in range(10000)]
        inserted = self.table.parallel_insert(records)
        self.assertEqual(inserted, 10000)
        self.assertEqual(len(self.table.records), 10000)

    def test_parallel_insert_constraint_violation(self):
        """Test parallel insert with constraint violations"""
        # Add a constraint to the table
        self.table.add_constraint("name", lambda x: len(x) <= 10)
        records = [
            {"name": "John", "email": "john@example.com"},  # Valid
            {"name": "VeryLongName1234567890", "email": "long@example.com"},  # Invalid
        ]
        with self.assertRaises(ValueError):
            with suppress_print():
                self.table.parallel_insert(records) 

    def test_parallel_try_insert(self):
        """Test parallel try_insert with valid and invalid data"""
        records = [{"name": f"John{i}", "email": "john@example.com"} for i in range(100)]
        # First insertion should succeed
        inserted = self.table.parallel_try_insert(records)
        self.assertEqual(inserted, 100)

    def test_parallel_try_insert_fail(self):
        """Test parallel try_insert with constraint violations"""
         # Add a constraint to the table
        self.table.add_constraint("name", lambda x: len(x) <= 5)        
        
        # Second insertion should fail constraint violations, inserting no records
        records = [{"name": f"Jane{i}", "email": "jane@example.com"} for i in range(100)]
        with suppress_print():
            self.table.parallel_try_insert(records)
        self.assertEqual(len(self.table.records), 0)

    def test_parallel_insert_concurrent_modifications(self):
        """Test parallel insert while other operations are happening"""
        initial_records = [{"name": f"Initial{i}", "email": "initial@example.com"} for i in range(100)]
        self.table.bulk_insert(initial_records)
        
        # Prepare parallel insertion records
        parallel_records = [{"name": f"Parallel{i}", "email": "parallel@example.com"} for i in range(100)]
        
        # Insert records in parallel
        inserted = self.table.parallel_insert(parallel_records)
        self.assertEqual(inserted, 100)
        
        # Verify all records are present and IDs are sequential
        self.assertEqual(len(self.table.records), 200)
        ids = [record.id for record in self.table.records]
        self.assertEqual(ids, list(range(self.table.records[0].id, self.table.records[-1].id + 1)))

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