import unittest
from unittest.mock import Mock
from unittest.mock import MagicMock
import logging
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import *
from segadb.database import _process_file_chunk
from tests.utils import suppress_print

class TestDatabase(unittest.TestCase):
    """
    Unit tests for the Database class.
        Methods:
        # Initialization and Configuration
        - test_database: Tests the initialization of a database object.
        - test_create_table: Tests the creation of a table in the database.
        - test_drop_table: Tests the deletion of a table from the database.
        - test_get_table: Tests retrieving a table from the database.
        - test_copy: Tests copying the database.
        - test_restore: Tests restoring the database from a copy.
        - test_create_table_from_csv: Tests creating a table from a CSV file.
    # Table Management
        - test_add_constraint: Tests adding a constraint to a table.
        - test_add_foreign_key_constraint: Tests adding a foreign key constraint to a table.
        - test_join_tables: Tests joining two tables.
        - test_aggregate_table: Tests aggregating data in a table.
        - test_filter_table: Tests filtering data in a table.
    # Parallel Processing
        - test_process_file_chunk: Tests processing a file chunk.
        - test_create_table_from_csv_mp: Tests creating a table from a CSV file using multiprocessing.
        - test_get_file_chunks: Tests getting file chunks for multiprocessing.
        - test_process_file: Tests processing a file using multiprocessing.
    # View Management
        - test_create_view: Tests creating a view in the database.
        - test_create_view_existing: Tests creating a view that already exists.
        - test_get_view_nonexistent: Tests retrieving a view that does not exist.
        - test_delete_view: Tests deleting a view from the database.
        - test_delete_view_nonexistent: Tests deleting a view that does not exist.
    # Materialized View Management
        - test_create_materialized_view: Tests creating a materialized view in the database.
        - test_create_materialized_view_existing: Tests creating a materialized view that already exists.
        - test_get_materialized_view_nonexistent: Tests retrieving a materialized view that does not exist.
        - test_refresh_materialized_view: Tests refreshing a materialized view.
        - test_refresh_materialized_view_nonexistent: Tests refreshing a materialized view that does not exist.
        - test_delete_materialized_view: Tests deleting a materialized view from the database.
        - test_delete_materialized_view_nonexistent: Tests deleting a materialized view that does not exist.
    # Stored Procedures Management
        - test_add_stored_procedure: Tests adding a stored procedure to the database.
        - test_execute_stored_procedure: Tests executing a stored procedure.
        - test_delete_stored_procedure: Tests deleting a stored procedure from the database.
        - test_get_stored_procedure: Tests retrieving a stored procedure from the database.
    # Trigger Management
        - test_add_trigger: Tests adding a trigger to the database.
        - test_execute_triggers: Tests executing triggers.
        - test_delete_trigger: Tests deleting a trigger from the database.
    # Loading Sample Database
        - test_load_sample_database: Tests loading the sample database.
        - test_load_sample_database_custom: Tests loading a custom sample database.
        - test_load_sample_database_exec: Tests executing a stored procedure from the sample database.
    # Logging
        - test_log_method_call: Tests logging a method call.
        - test_log_method_call_fail: Tests logging a method call that raises an error.
        - test_log_method_call_filter: Tests logging a method call with filtered arguments.
    """
    @classmethod
    def setUpClass(cls):
        print("\nTesting Database Class", end="", flush=True)

    # Initialization and Configuration
    # ---------------------------------------------------------------------------------------------
    def test_database(self):
        db = Database("TestDB")
        self.assertEqual(db.name, "TestDB")
        self.assertGreater(len(db.tables), 0)
        self.assertEqual(db.views, {})
        self.assertEqual(db.materialized_views, {})
        self.assertEqual(db.stored_procedures, {})
        self.assertEqual(db.triggers, {'after': {}, 'before': {}})
        
    def test_create_table(self):
        db = Database("TestDB")
        db.create_table("Users", ["id", "name", "email"])
        self.assertIn("Users", db.tables)
        
    def test_drop_table(self):
        db = Database("TestDB")
        db.create_table("Users", ["id", "name", "email"])
        db.drop_table("Users")
        self.assertNotIn("Users", db.tables)
    
    def test_get_table(self):
        db = Database("TestDB")
        db.create_table("Users", ["id", "name", "email"])
        table = db.get_table("Users")
        self.assertIsNotNone(table)
        self.assertEqual(table.name, "Users")
        self.assertEqual(table.columns, ["id", "name", "email"])

    def test_copy(self):
        db = Database("TestDB")
        db.create_table("Users", ["id", "name", "email"])
        db_copy = db.copy()
        self.assertEqual(db.name, db_copy.name)
        self.assertEqual(db.tables.keys(), db_copy.tables.keys())

    def test_restore(self):
        db = Database("TestDB")
        db.create_table("Users", ["id", "name", "email"])
        db_copy = db.copy()
        db.create_table("Orders", ["id", "user_id", "total"])
        db.restore(db_copy)
        self.assertNotIn("Orders", db.tables)
        self.assertIn("Users", db.tables)

    def test_create_table_from_csv(self):
        import tempfile
        import csv

        db = Database("TestDB")
        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["id", "name", "email"])
            writer.writerow([1, "John Doe", "john@example.com"])
            writer.writerow([2, "Jane Doe", "jane@example.com"])
            csvfile_path = csvfile.name

        db.create_table_from_csv(csvfile_path, "Users")
        table = db.get_table("Users")
        self.assertIsNotNone(table)
        self.assertEqual(len(table.records), 2)
        os.remove(csvfile_path)

    # Table Management
    # ---------------------------------------------------------------------------------------------
    def test_add_constraint(self):
        db = Database("TestDB")
        db.create_table("Users", ["id", "name", "email"])
        db.add_constraint("Users", "email", lambda x: "@" in x)
        table = db.get_table("Users")
        self.assertIn("email", table.constraints)
        
    def test_add_foreign_key_constraint(self):
        db = Database("TestDB")
        db.create_table("Users", ["user_id", "name"])
        db.create_table("Orders", ["order_id", "user_id", "product"])
        
        db.add_constraint("Users", "user_id", "UNIQUE")
        db.add_constraint("Orders", "user_id", "FOREIGN_KEY", reference_table_name="Users", reference_column="user_id")
        
        db.get_table("Users").insert({"user_id": 1, "name": "Alice"})
        db.get_table("Users").insert({"user_id": 2, "name": "Bob"})
        
        db.get_table("Orders").insert({"order_id": 1, "user_id": 1, "product": "Laptop"})
        db.get_table("Orders").insert({"order_id": 2, "user_id": 2, "product": "Phone"})
        
        with suppress_print():
            with self.assertRaises(ValueError):
                db.get_table("Orders").insert({"order_id": 3, "user_id": 3, "product": "Tablet"})

    def test_join_tables(self):
        db = Database("TestDB")
        db.create_table("Users", ["user_id", "name"])
        db.create_table("Orders", ["order_id", "user_id", "product"])
        
        db.get_table("Users").insert({"user_id": 1, "name": "Alice"})
        db.get_table("Users").insert({"user_id": 2, "name": "Bob"})
        
        db.get_table("Orders").insert({"order_id": 1, "user_id": 1, "product": "Laptop"})
        db.get_table("Orders").insert({"order_id": 2, "user_id": 2, "product": "Phone"})
        
        joined_table = db.join_tables("Users", "Orders", "user_id", "user_id")
        self.assertEqual(len(joined_table.records), 2)
        self.assertEqual(joined_table.records[0].data["name"], "Alice")
        self.assertEqual(joined_table.records[1].data["name"], "Bob")

    def test_aggregate_table(self):
        db = Database("TestDB")
        db.create_table("Orders", ["order_id", "user_id", "product"])
        
        db.get_table("Orders").insert({"order_id": 1, "user_id": 1, "product": "Laptop"})
        db.get_table("Orders").insert({"order_id": 2, "user_id": 2, "product": "Phone"})
        db.get_table("Orders").insert({"order_id": 3, "user_id": 2, "product": "Tablet"})
        
        count_table = db.aggregate_table("Orders", "user_id", "order_id", "COUNT")
        self.assertEqual(count_table.records[0].data["order_id"], 1)
        self.assertEqual(count_table.records[1].data["order_id"], 2)
        
        count_distinct_table = db.aggregate_table("Orders", "user_id", "order_id", "COUNT_DISTINCT")
        self.assertEqual(count_distinct_table.records[0].data["order_id"], 1)
        self.assertEqual(count_distinct_table.records[1].data["order_id"], 2)
        
        min_table = db.aggregate_table("Orders", "user_id", "order_id", "MIN")
        self.assertEqual(min_table.records[0].data["order_id"], 1)
        self.assertEqual(min_table.records[1].data["order_id"], 2)
        
        max_table = db.aggregate_table("Orders", "user_id", "order_id", "MAX")
        self.assertEqual(max_table.records[0].data["order_id"], 1)
        self.assertEqual(max_table.records[1].data["order_id"], 3)
        
        sum_table = db.aggregate_table("Orders", "user_id", "order_id", "SUM")
        self.assertEqual(sum_table.records[0].data["order_id"], 1)
        self.assertEqual(sum_table.records[1].data["order_id"], 5)
        
        avg_table = db.aggregate_table("Orders", "user_id", "order_id", "AVG")
        self.assertEqual(avg_table.records[0].data["order_id"], 1.0)
        self.assertEqual(avg_table.records[1].data["order_id"], 2.5)

    def test_filter_table(self):
        db = Database("TestDB")
        db.create_table("Orders", ["order_id", "user_id", "product"])
        
        db.get_table("Orders").insert({"order_id": 1, "user_id": 1, "product": "Laptop"})
        db.get_table("Orders").insert({"order_id": 2, "user_id": 2, "product": "Phone"})
        db.get_table("Orders").insert({"order_id": 3, "user_id": 2, "product": "Tablet"})
        
        filtered_table = db.filter_table("Orders", lambda record: record.data["product"] == "Laptop")
        self.assertEqual(len(filtered_table.records), 1)
        self.assertEqual(filtered_table.records[0].data["product"], "Laptop")

        filtered_table = db.filter_table("Orders", lambda record: record.data["user_id"] == 2)
        self.assertEqual(len(filtered_table.records), 2)
        self.assertEqual(filtered_table.records[0].data["user_id"], 2)
        self.assertEqual(filtered_table.records[1].data["user_id"], 2)

    # Parallel Processing
    # ---------------------------------------------------------------------------------------------
    def test_process_file_chunk(self):
        import tempfile
        import csv

        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["id", "name", "email"])
            writer.writerow([1, "John Doe", "john@example.com"])
            writer.writerow([2, "Jane Doe", "jane@example.com"])
            csvfile_path = csvfile.name

        chunk_start = 0
        chunk_end = os.path.getsize(csvfile_path)
        rows = _process_file_chunk(csvfile_path, chunk_start, chunk_end, delim=',', column_names=["id", "name", "email"], headers=True)
        self.assertEqual(len(rows), 2)
        os.remove(csvfile_path)

    def test_create_table_from_csv_mp(self):
        import tempfile
        import csv

        db = Database("TestDB")
        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["id", "name", "email"])
            writer.writerow([1, "John Doe", "john@example.com"])
            writer.writerow([2, "Jane Doe", "jane@example.com"])
            csvfile_path = csvfile.name

        db.create_table_from_csv(csvfile_path, "Users", headers=True, parallel=True)
        table = db.get_table("Users")
        self.assertIsNotNone(table)
        self.assertEqual(len(table.records), 2)
        os.remove(csvfile_path)

    def test_get_file_chunks(self):
        import tempfile

        db = Database("TestDB")
        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as csvfile:
            csvfile.write("id,name,email\n")
            for i in range(10000):
                csvfile.write(f"{i},Name{i},email{i}@example.com\n")
            csvfile_path = csvfile.name

        cpu_count, chunks = db._get_file_chunks(csvfile_path, max_cpu=5, headers=True, max_chunk_size=None)
        self.assertEqual(cpu_count, 5)
        self.assertEqual(len(chunks), 5)
        os.remove(csvfile_path)

    def test_process_file(self):
        import tempfile

        db = Database("TestDB")
        with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='') as csvfile:
            csvfile.write("id,name,email\n")
            for i in range(100):
                csvfile.write(f"{i},Name{i},email{i}@example.com\n")
            csvfile_path = csvfile.name

        cpu_count, chunks = db._get_file_chunks(csvfile_path, max_cpu=4, headers=True)
        records = db._process_file(cpu_count, chunks, delim=',', column_names=["id", "name", "email"], col_types=[int, str, str], progress=False, headers=True)
        self.assertEqual(len(records), 100)
        os.remove(csvfile_path)

    # View Management
    # ---------------------------------------------------------------------------------------------
    def test_create_view(self):
        db = Database("TestDB")
        def UserView():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        db.create_view("UserView", UserView)
        self.assertIn("UserView", db.views)
        view = db.get_view("UserView")
        self.assertEqual(view.name, "UserView")
        self.assertEqual(view.query(), UserView())

    def test_create_view_existing(self):
        db = Database("TestDB")
        def UserView():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        db.create_view("UserView", UserView)
        with self.assertRaises(ValueError):
            db.create_view("UserView", UserView)

    def test_get_view_nonexistent(self):
        db = Database("TestDB")
        with self.assertRaises(ValueError):
            db.get_view("NonExistentView")

    def test_delete_view(self):
        db = Database("TestDB")
        def UserView():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        db.create_view("UserView", UserView)
        db.delete_view("UserView")
        self.assertNotIn("UserView", db.views)

    def test_delete_view_nonexistent(self):
        db = Database("TestDB")
        with self.assertRaises(ValueError):
            db.delete_view("NonExistentView")

    # Materialized View Management
    # ---------------------------------------------------------------------------------------------
    def test_create_materialized_view(self):
        db = Database("TestDB")
        def UserMaterializedView():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        db.create_materialized_view("UserMaterializedView", UserMaterializedView)
        self.assertIn("UserMaterializedView", db.materialized_views)
        materialized_view = db.get_materialized_view("UserMaterializedView")
        self.assertEqual(materialized_view.name, "UserMaterializedView")
        self.assertEqual(materialized_view.query(), UserMaterializedView())

    def test_create_materialized_view_existing(self):
        db = Database("TestDB")
        def UserMaterializedView():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        db.create_materialized_view("UserMaterializedView", UserMaterializedView)
        with self.assertRaises(ValueError):
            db.create_materialized_view("UserMaterializedView", UserMaterializedView)

    def test_get_materialized_view_nonexistent(self):
        db = Database("TestDB")
        with self.assertRaises(ValueError):
            db.get_materialized_view("NonExistentMaterializedView")

    def test_refresh_materialized_view(self):
        db = Database("TestDB")
        UserMaterializedView = MagicMock(return_value=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        UserMaterializedView.__name__ = "UserMaterializedView"
        db.create_materialized_view("UserMaterializedView", UserMaterializedView)
        db.refresh_materialized_view("UserMaterializedView")
        self.assertEqual(UserMaterializedView.call_count, 2)

    def test_refresh_materialized_view_nonexistent(self):
        db = Database("TestDB")
        with self.assertRaises(ValueError):
            db.refresh_materialized_view("NonExistentMaterializedView")

    def test_delete_materialized_view(self):
        db = Database("TestDB")
        def UserMaterializedView():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        db.create_materialized_view("UserMaterializedView", UserMaterializedView)
        db.delete_materialized_view("UserMaterializedView")
        self.assertNotIn("UserMaterializedView", db.materialized_views)

    def test_delete_materialized_view_nonexistent(self):
        db = Database("TestDB")
        with self.assertRaises(ValueError):
            db.delete_materialized_view("NonExistentMaterializedView")
    
    # Stored Procedures Management
    # ---------------------------------------------------------------------------------------------
    def test_add_stored_procedure(self):
        db = Database("TestDB")
        def sample_procedure(db, x, y):
            return x + y
        db.add_stored_procedure("sample_procedure", sample_procedure)
        self.assertIn("sample_procedure", db.stored_procedures)

    def test_execute_stored_procedure(self):
        db = Database("TestDB")
        def sample_procedure(db, x, y):
            return x + y
        db.add_stored_procedure("sample_procedure", sample_procedure)
        result = db.execute_stored_procedure("sample_procedure", 1, 2)
        self.assertEqual(result, 3)

    def test_delete_stored_procedure(self):
        db = Database("TestDB")
        def sample_procedure(db, x, y):
            return x + y
        db.add_stored_procedure("sample_procedure", sample_procedure)
        db.delete_stored_procedure("sample_procedure")
        self.assertNotIn("sample_procedure", db.stored_procedures)

    def test_get_stored_procedure(self):
        db = Database("TestDB")
        def sample_procedure(db, x, y):
            return x + y
        db.add_stored_procedure("sample_procedure", sample_procedure)
        procedure = db.get_stored_procedure("sample_procedure")
        self.assertEqual(procedure, sample_procedure)

    # Trigger Management
    # ---------------------------------------------------------------------------------------------
    def test_add_trigger(self):
        db = Database("TestDB")
        def sample_procedure(db, x, y):
            return x + y
        def before_trigger(db, procedure_name, *args, **kwargs):
            print(f"Before {procedure_name}")
        db.add_stored_procedure("sample_procedure", sample_procedure)
        db.add_trigger("sample_procedure", "before", before_trigger)
        self.assertIn("sample_procedure", db.triggers["before"])

    def test_execute_triggers(self):
        db = Database("TestDB")
        def sample_procedure(db, x, y):
            return x + y
        def before_trigger(db, procedure_name, *args, **kwargs):
            logging.info(f"Before {procedure_name}")
        db.add_stored_procedure("sample_procedure", sample_procedure)
        db.add_trigger("sample_procedure", "before", before_trigger)
        with self.assertLogs(level='INFO') as log:
            db.execute_stored_procedure("sample_procedure", 1, 2)
            self.assertIn("INFO:root:Before sample_procedure", log.output)

    def test_delete_trigger(self):
        db = Database("TestDB")
        def sample_procedure(db, x, y):
            return x + y
        def before_trigger(db, procedure_name, *args, **kwargs):
            print(f"Before {procedure_name}")
        db.add_stored_procedure("sample_procedure", sample_procedure)
        db.add_trigger("sample_procedure", "before", before_trigger)
        db.delete_trigger("sample_procedure", "before", before_trigger)
        self.assertNotIn("sample_procedure", db.triggers["before"])
        
    # Loading Sample Database
    def test_load_sample_database(self):
        db = Database.load_sample_database()
        self.assertGreater(len(db.tables), 0)
        self.assertGreater(len(db.views), 0)
        self.assertGreater(len(db.materialized_views), 0)
        self.assertGreater(len(db.stored_procedures), 0)
        self.assertGreater(len(db.triggers), 0)
        
    def test_load_sample_database_custom(self):
        db = Database.load_sample_database(n_users=100, n_orders=200)
        self.assertEqual(len(db.get_table("users").records), 100)
        self.assertEqual(len(db.get_table("orders").records), 200)
        
    def test_load_sample_database_exec(self):
        db = Database.load_sample_database()
        with suppress_print():
            db.execute_stored_procedure("drop_users_with_no_orders")
        self.assertGreater(len(db.get_table("users").records), 0)

    # Logging
    # ---------------------------------------------------------------------------------------------
    def test_log_method_call(self):
        db = Database("TestDB_database", db_logging=True)
        db.logger = MagicMock()

        @log_method_call
        def sample_method(self, x, y):
            return x + y

        result = sample_method(db, 1, 2)
        self.assertEqual(result, 3)
        db.logger.info.assert_any_call("Method Call: sample_method | Args: {'x': 1, 'y': 2}")
        db.logger.info.assert_any_call("Method Complete: sample_method | Status: Success")
        
    def test_log_method_call_fail(self):
        db = Database("TestDB", db_logging=True)
        db.logger = MagicMock()

        @log_method_call
        def sample_method(self, x, y):
            raise ValueError("Test Error")

        with self.assertRaises(ValueError):
            sample_method(db, 1, 2)
        db.logger.info.assert_any_call("Method Call: sample_method | Args: {'x': 1, 'y': 2}")
        db.logger.error.assert_any_call("Method Error: sample_method | Args: {'x': 1, 'y': 2} | Error: Test Error")
        
    def test_log_method_call_filter(self):
        # Should filter password and password_hash from logs
        db = Database("TestDB", db_logging=True)
        db.logger = MagicMock()
        
        @log_method_call
        def sample_method(self, username, password, password_hash):
            return username + password + password_hash
        
        result = sample_method(db, "user", "password", "hash")
        self.assertEqual(result, "userpasswordhash")        
        db.logger.info.assert_any_call("Method Call: sample_method | Args: {'username': 'user', 'password': '[REDACTED]', 'password_hash': '[REDACTED]'}")
        db.logger.info.assert_any_call("Method Complete: sample_method | Status: Success")

if __name__ == '__main__':
    unittest.main()