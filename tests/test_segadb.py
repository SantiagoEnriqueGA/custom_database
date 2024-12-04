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
from segadb.record import Record
from segadb.storage import Storage
from segadb.transaction import Transaction

class TestDatabase(unittest.TestCase):
    """
    Unit tests for the Database class.
    Methods:
    - test_create_table: Tests the creation of a table in the database.
    - test_drop_table: Tests the deletion of a table from the database.
    - test_get_table: Tests retrieving a table from the database.
    """
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
        
class TestTable(unittest.TestCase):
    """
    Unit tests for the Table class.
    Methods:
    - setUp: Initializes a Table instance with the name "Users" and columns ["id", "name", "email"].
    - test_insert: Tests that a record can be inserted into the table and verifies the inserted data.
    - test_delete: Tests that a record can be deleted from the table and verifies the deletion.
    - test_update: Tests that a record can be updated in the table and verifies the updated data.
    - test_select: Tests that records can be selected from the table based on a condition and verifies the selected data.
    """
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
        self.table.add_constraint("email", lambda x: "@" in x)
        self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
        self.assertEqual(len(self.table.records), 1)
        self.table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})
        self.assertEqual(len(self.table.records), 1)  # Should not insert due to constraint violation

    def test_print_table(self):
        self.table.insert({"name": "John Doe", "email": "john@example.com"})
        self.table.insert({"name": "Jane Doe", "email": "jane@example.com"})
        self.table.print_table(limit=1)
        self.table.print_table(pretty=True)
        
    def test_add_constraint1(self):
        self.table.add_constraint("email", lambda x: "@" in x)
        self.assertIn("email", self.table.constraints)
        self.table.try_insert({"name": "John Doe", "email": "john@example.com"})
        self.assertEqual(len(self.table.records), 1)
        self.table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})
        self.assertEqual(len(self.table.records), 1)  # Should not insert due to constraint violation

class TestIndex(unittest.TestCase):
    """
    Unit tests for the Index class.
    Methods:
    - setUp: Initializes a new instance of the Index class before each test method is run.
    - test_add_and_find: Tests that an item can be added to the index and subsequently found.
    - test_remove: Tests that an item can be removed from the index and is no longer found.
    """
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
    """
    def test_record_creation(self):
        record = Record(1, {"name": "John Doe"})
        self.assertEqual(record.id, 1)
        self.assertEqual(record.data["name"], "John Doe")

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
        self.assertEqual(loaded_db["name"], "TestDB")
        self.assertIn("Users", loaded_db["tables"])

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

