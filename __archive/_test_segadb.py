# Description: Unit tests for the segadb package.
import unittest
from unittest.mock import Mock

# Change the working directory to the parent directory to allow importing the segadb package.
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the individual classes from the segadb package.
from segadb.database import Database
from segadb.table import Table
from segadb.index import Index
from segadb.record import Record, VectorRecord, TimeSeriesRecord, ImageRecord, TextRecord
from segadb.storage import Storage
from segadb.transaction import Transaction

from contextlib import contextmanager

@contextmanager
def suppress_print():
    """
    Context manager to suppress standard output (stdout).  
    This function redirects the standard output to os.devnull, silencing any print statements or other output to the console within the context.   
    Once the context is exited, the standard output is restored to its original state.  

    Usage:
        with suppress_print():
            # Code that produces output
            print("This will not be printed")
    Yields:
        None
    """
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

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
        
        count_table = db.aggregate_table("Orders", "user_id", "COUNT")
        self.assertEqual(count_table.records[0].data["user_id"], 3)
        
        count_distinct_table = db.aggregate_table("Orders", "user_id", "COUNT_DISTINCT")
        self.assertEqual(count_distinct_table.records[0].data["user_id"], 2)
        
        min_table = db.aggregate_table("Orders", "user_id", "MIN")
        self.assertEqual(min_table.records[0].data["user_id"], 1)
        
        max_table = db.aggregate_table("Orders", "user_id", "MAX")
        self.assertEqual(max_table.records[0].data["user_id"], 2)
        
        sum_table = db.aggregate_table("Orders", "user_id", "SUM")
        self.assertEqual(sum_table.records[0].data["user_id"], 5)
        
        avg_table = db.aggregate_table("Orders", "user_id", "AVG")
        self.assertEqual(avg_table.records[0].data["user_id"], 1.6666666666666667)

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

class TestIndex(unittest.TestCase):
    """
    Unit tests for the Index class.
    Methods:
    - setUp: Initializes a new instance of the Index class before each test method is run.
    - test_add_and_find: Tests that an item can be added to the index and subsequently found.
    - test_remove: Tests that an item can be removed from the index and is no longer found.
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Index Class")

    def setUp(self):
        self.index = Index()

    def test_add_and_find(self):
        self.index.add("name", 1)
        self.assertIn(1, self.index.find("name"))

    def test_remove(self):
        self.index.add("name", 1)
        self.index.remove("name", 1)
        self.assertNotIn(1, self.index.find("name"))

class TestRecord(unittest.TestCase):
    """
    Unit tests for the Record class.
    Methods:
    - test_record_creation: Tests the creation of a record with an ID and data.
    - test_vector_record: Tests the VectorRecord class and its methods.
    - test_time_series_record: Tests the TimeSeriesRecord class and its methods.
    - test_image_record: Tests the ImageRecord class and its methods.
    - test_text_record: Tests the TextRecord class and its
    """
    @classmethod
    def setUpClass(cls):
        print("Testing Record Class")

    def test_record_creation(self):
        record = Record(1, {"name": "John Doe"})
        self.assertEqual(record.id, 1)
        self.assertEqual(record.data["name"], "John Doe")

    def test_vector_record(self):
        import math
        
        vector_record = VectorRecord(1, {"vector": [1, 2, 3]})
        self.assertEqual(vector_record.magnitude(), math.sqrt(14))
        self.assertEqual(vector_record.normalize(), [1/math.sqrt(14), 2/math.sqrt(14), 3/math.sqrt(14)])
        self.assertEqual(vector_record.dot_product([4, 5, 6]), 32)

    def test_time_series_record(self):
        time_series_record = TimeSeriesRecord(1, {"time_series": [1, 2, 3, 4, 5]})
        self.assertEqual(time_series_record.moving_average(3), [2.0, 3.0, 4.0])

    def test_image_record(self):
        import tempfile
        from PIL import Image

        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_image:
            image = Image.new("RGB", (100, 100), color="red")
            image.save(temp_image, format="PNG")
            temp_image_path = temp_image.name

        image_record = ImageRecord(1, {"image_data": temp_image_path})
        self.assertEqual(image_record.image_size, os.path.getsize(temp_image_path))
        resized_image = image_record.resize(0.5)
        self.assertEqual(resized_image.size, (50, 50))
        os.remove(temp_image_path)

    def test_text_record(self):
        text_record = TextRecord(1, {"text": "Hello World"})
        self.assertEqual(text_record.word_count(), 2)
        self.assertEqual(text_record.to_uppercase(), "HELLO WORLD")
        self.assertEqual(text_record.to_lowercase(), "hello world")

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
        print("Testing Transaction Class")

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

