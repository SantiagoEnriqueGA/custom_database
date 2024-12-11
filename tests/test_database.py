import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database
from segadb.database import _process_file_chunk
from test_utils import suppress_print

class TestDatabase(unittest.TestCase):
    """
    Unit tests for the Database class.
    Methods:
    - test_create_table: Tests the creation of a table in the database.
    - test_drop_table: Tests the deletion of a table from the database.
    - test_get_table: Tests retrieving a table from the database.
    - test_copy: Tests copying the database.
    - test_restore: Tests restoring the database from a copy.
    - test_create_table_from_csv: Tests creating a table from a CSV file.
    - test_add_constraint: Tests adding a constraint to a table.
    - test_add_foreign_key_constraint: Tests adding a foreign key constraint to a table.
    - test_join_tables: Tests joining two tables.
    - test_aggregate_table: Tests aggregating data in a table.
    - test_filter_table: Tests filtering data in a table.
    - test_process_file_chunk: Tests processing a file chunk.
    - test_create_table_from_csv_mp: Tests creating a table from a CSV file using multiprocessing.
    - test_get_file_chunks: Tests getting file chunks for multiprocessing.
    - test_process_file: Tests processing a file using multiprocessing.
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Database Class")

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

        db.create_table_from_csv(csvfile_path, "Users", headers=True, parrallel=True)
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

if __name__ == '__main__':
    unittest.main()