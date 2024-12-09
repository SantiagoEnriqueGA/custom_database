import unittest
import time

# Change the working directory to the parent directory to allow importing the segadb package.
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database
from segadb.storage import Storage

NUM_RECORDS = 5000

class TestDatabasePerformance(unittest.TestCase):
    """
    Performance tests for the Database class.
    Methods:
    - test_insert_performance: Tests the performance of inserting a large number of records.
    - test_select_performance: Tests the performance of selecting records based on a condition.
    - test_update_performance: Tests the performance of updating a large number of records.
    - test_delete_performance: Tests the performance of deleting a large number of records.
    """
    
    def setUp(self):
        self.db = Database("PerformanceTestDB")
        self.db.create_table("Users", ["id", "name", "email"])
        self.users_table = self.db.get_table("Users")
        for i in range(NUM_RECORDS):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})

    def test_insert_performance(self):
        # Delete all records before testing insert performance
        for i in range(NUM_RECORDS):
            self.users_table.delete(i)
        start_time = time.time()
        for i in range(NUM_RECORDS):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
        end_time = time.time()
        print(f"Insert performance for {NUM_RECORDS} [id, name, email] records: {(end_time - start_time):.2} seconds.")

    def test_select_performance(self):
        start_time = time.time()
        results = self.users_table.select(lambda record: record.data["name"] == f"User{NUM_RECORDS - 1}")
        end_time = time.time()
        print(f"Select performance of 1 out of {NUM_RECORDS} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        self.assertEqual(len(results), 1)

    def test_update_performance(self):
        start_time = time.time()
        for i in range(NUM_RECORDS):
            self.users_table.update(i, {"name": f"UpdatedUser{i}", "email": f"updateduser{i}@example.com"})
        end_time = time.time()
        print(f"Update performance on {NUM_RECORDS} [id, name, email] records: {(end_time - start_time):.2} seconds.")

    def test_delete_performance(self):
        start_time = time.time()
        for i in range(NUM_RECORDS):
            self.users_table.delete(i)
        end_time = time.time()
        print(f"Delete performance on {NUM_RECORDS} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        
    def test_save_performance(self):
        # Measure save performance
        start_time = time.time()
        Storage.save(self.db, "test_db.json")
        end_time = time.time()
        print(f"Save performance for {NUM_RECORDS} records: {(end_time - start_time):.2} seconds.")
                
        # Clean up
        Storage.delete("test_db.json")
        
    def test_load_performance(self):
        # Save the database to a file
        Storage.save(self.db, "test_db.json")
        
        # Measure load performance
        start_time = time.time()
        loaded_db = Storage.load("test_db.json")
        end_time = time.time()
        print(f"Load performance for {NUM_RECORDS} records: {(end_time - start_time):.2} seconds.")
        
    def test_restore_performance(self):
        # Create a shadow copy of the database
        db_copy = self.db.copy()
        
        # Measure restore performance
        start_time = time.time()
        self.db.restore(db_copy)
        end_time = time.time()
        print(f"Restore performance for {NUM_RECORDS} records: {(end_time - start_time):.2} seconds.")
        

if __name__ == '__main__':
    unittest.main()