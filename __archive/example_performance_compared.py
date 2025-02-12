
import unittest
import time

# Change the working directory to the parent directory to allow importing the segadb package.
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database
from segadb.storage import Storage


class TestDatabasePerformance:
    """
    Performance tests for the Database class.
    Methods:
    - test_insert_performance: Tests the performance of inserting a large number of records.
    - test_select_performance: Tests the performance of selecting records based on a condition.
    - test_update_performance: Tests the performance of updating a large number of records.
    - test_delete_performance: Tests the performance of deleting a large number of records.
    """
    
    def __init__(self, num_records=1000, verbose=False):
        self.num_records = num_records
        self.verbose = verbose
        self.db = Database("PerformanceTestDB")
        self.db.create_table("Users", ["id", "name", "email"])
        self.users_table = self.db.get_table("Users")
        for i in range(self.num_records):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
            
    def run_all_tests(self):
        results = {}
        results["insert"] = self.test_insert_performance()
        results["select"] = self.test_select_performance()
        results["update"] = self.test_update_performance()
        results["delete"] = self.test_delete_performance()
        results["save"] = self.test_save_performance()
        results["load"] = self.test_load_performance()
        results["restore"] = self.test_restore_performance()
        results["join"] = self.test_join_performance()
        return results

    def test_insert_performance(self):
        # Delete all records before testing insert performance
        for i in range(self.num_records):
            self.users_table.delete(i)
        start_time = time.time()
        for i in range(self.num_records):
            self.users_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
        end_time = time.time()
        if self.verbose: print(f"Insert performance for {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time

    def test_select_performance(self):
        start_time = time.time()
        results = self.users_table.select(lambda record: record.data["name"] == f"User{self.num_records - 1}")
        end_time = time.time()
        if self.verbose:print(f"Select performance of 1 out of {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time

    def test_update_performance(self):
        start_time = time.time()
        for i in range(self.num_records):
            self.users_table.update(i, {"name": f"UpdatedUser{i}", "email": f"updateduser{i}@example.com"})
        end_time = time.time()
        if self.verbose: print(f"Update performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time

    def test_delete_performance(self):
        start_time = time.time()
        for i in range(self.num_records):
            self.users_table.delete(i)
        end_time = time.time()
        if self.verbose: print(f"Delete performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
        
    def test_save_performance(self):
        # Measure save performance
        start_time = time.time()
        Storage.save(self.db, "test_db.segadb")
        end_time = time.time()
        if self.verbose: print(f"Save performance for {self.num_records} records: {(end_time - start_time):.2} seconds.")
        # Clean up
        Storage.delete("test_db.segadb")
        return end_time - start_time
        
    def test_load_performance(self):
        # Save the database to a file
        Storage.save(self.db, "test_db.segadb")
        # Measure load performance
        start_time = time.time()
        loaded_db = Storage.load("test_db.segadb")
        end_time = time.time()
        if self.verbose: print(f"Load performance for {self.num_records} records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
        
    def test_restore_performance(self):
        # Create a shadow copy of the database
        db_copy = self.db.copy()
        # Measure restore performance
        start_time = time.time()
        self.db.restore(db_copy)
        end_time = time.time()
        if self.verbose: print(f"Restore performance for {self.num_records} records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_join_performance(self):
        # Create a new table
        self.db.create_table("Orders", ["id", "user_id", "product_id", "quantity"])
        orders_table = self.db.get_table("Orders")
        for i in range(self.num_records):
            orders_table.insert({"id": i, "user_id": i, "product_id": i, "quantity": i})
        
        start_time = time.time()
        results = self.users_table.join(orders_table, "id", "user_id")
        end_time = time.time()
        print(f"Join performance for {self.num_records} records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time

def main():
    # test_db_performance = TestDatabasePerformance(num_records=1000, verbose=True)
    # results = test_db_performance.run_all_tests()
    # print(results)
    
    # Test performance for increasing number of records
    num_records = [10, 100, 1000]#``, 10_000, 100_000]
    for n in num_records:
        test_db_performance = TestDatabasePerformance(num_records=n, verbose=False)
        results = test_db_performance.run_all_tests()
        print(f"Results for {n} records:")
        print(results)
        print()
    
if __name__ == '__main__':
    main()