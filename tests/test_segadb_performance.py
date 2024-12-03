import unittest
import time

# Change the working directory to the parent directory to allow importing the segadb package.
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database

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

    def test_insert_performance(self):
        start_time = time.time()
        for i in range(10000):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
        end_time = time.time()
        print(f"Insert performance: {end_time - start_time} seconds")

    def test_select_performance(self):
        for i in range(10000):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
        start_time = time.time()
        results = self.users_table.select(lambda record: record.data["id"] == 9999)
        end_time = time.time()
        print(f"Select performance: {end_time - start_time} seconds")
        self.assertEqual(len(results), 1)

    def test_update_performance(self):
        for i in range(10000):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
        start_time = time.time()
        for i in range(10000):
            self.users_table.update(i, {"name": f"UpdatedUser{i}", "email": f"updateduser{i}@example.com"})
        end_time = time.time()
        print(f"Update performance: {end_time - start_time} seconds")

    def test_delete_performance(self):
        for i in range(10000):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
        start_time = time.time()
        for i in range(10000):
            self.users_table.delete(i)
        end_time = time.time()
        print(f"Delete performance: {end_time - start_time} seconds")

if __name__ == '__main__':
    unittest.main()