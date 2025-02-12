import time
import sqlite3

# Change the working directory to the parent directory to allow importing the segadb package.
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database

class TestDatabasePerformance:   
    def __init__(self, num_records=1000, repeats=1, verbose=False):
        self.db = Database("PerformanceTestDB")
        self.num_records = num_records
        self.repeats = repeats
        self.verbose = verbose
        
    def run_all_tests_avg(self):
        results = {"insert": 0, "select": 0, "update": 0, "aggregate": 0, "filter": 0, "sort": 0, "delete": 0}
        for _ in range(self.repeats):
            results["insert"] += self.test_insert_performance()
            results["select"] += self.test_select_performance()
            results["update"] += self.test_update_performance()
            results["aggregate"] += self.test_aggregate_performance()
            results["filter"] += self.test_filter_performance()
            results["sort"] += self.test_sort_performance()
            results["delete"] += self.test_delete_performance()
        
        for key in results:
            results[key] /= self.repeats
        
        return results
    
    def run_all_tests_raw(self):
        # Returns ALL results for each operation and repeats
        results = {"insert": [], "select": [], "update": [], "aggregate": [], "filter": [], "sort": [], "delete": []}
        for _ in range(self.repeats):
            results["insert"].append(self.test_insert_performance())
            results["select"].append(self.test_select_performance())
            results["update"].append(self.test_update_performance())
            results["aggregate"].append(self.test_aggregate_performance())
            results["filter"].append(self.test_filter_performance())
            results["sort"].append(self.test_sort_performance())
            results["delete"].append(self.test_delete_performance())
            
        return results
     
    def test_insert_performance(self):
        # Create a new table for testing
        self.db.create_table("PerformanceTest", ["id", "name", "email"])
        test_table = self.db.get_table("PerformanceTest")
        
        # Test inserting records
        start_time = time.time()
        for i in range(self.num_records):
            test_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})
        assert len(test_table.records) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Insert performance for {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_select_performance(self):
        # Test selecting records
        start_time = time.time()
        results = self.db.get_table("PerformanceTest").select(lambda record: record.data["name"] == f"User{self.num_records - 1}")
        assert len(results) == 1
        
        end_time = time.time()
        if self.verbose: print(f"Select performance of 1 out of {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_update_performance(self):
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        for i in range(self.num_records):
            test_table.update(i, {"name": f"UpdatedUser{i^2}", "email": f"updateduser{i^2}@example.com"})
        assert len(test_table.records) == self.num_records
        assert test_table.records[-1].data["name"] == f"UpdatedUser{(self.num_records - 1) ^ 2}"
        
        end_time = time.time()
        if self.verbose: print(f"Update performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_aggregate_performance(self):
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        agg_table = test_table.aggregate("name", "email", "COUNT")
        assert len(agg_table.records) == len(test_table.records)
        
        end_time = time.time()
        if self.verbose: print(f"Aggregate performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_filter_performance(self):
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        
        # Filter recors the end with odd names
        filtered_table = test_table.filter(lambda record: record.data["name"][-1] in "13579")
        assert len(filtered_table.records) == self.num_records // 2
        
        end_time = time.time()
        if self.verbose: print(f"Filter performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_sort_performance(self):
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        
        # Sort records by name
        sorted_table = test_table.sort("name")
        assert len(sorted_table.records) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Sort performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_delete_performance(self):
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        for i in range(self.num_records):
            test_table.delete(i)
        assert len(test_table.records) == 0
        
        end_time = time.time()
        if self.verbose: print(f"Delete performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
class TestDatabasePerformanceSQLite:
    """Same tests as above, but using SQLite for comparison."""
    def __init__(self, num_records=1000, repeats=1, verbose=False):
        self.num_records = num_records
        self.repeats = repeats
        self.verbose = verbose
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE PerformanceTest (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
        self.conn.commit()
    
    def run_all_tests_avg(self):
        results = {"insert": 0, "select": 0, "update": 0, "aggregate": 0, "filter": 0, "sort": 0, "delete": 0}
        for _ in range(self.repeats):
            results["insert"] += self.test_insert_performance()
            results["select"] += self.test_select_performance()
            results["update"] += self.test_update_performance()
            results["aggregate"] += self.test_aggregate_performance()
            results["filter"] += self.test_filter_performance()
            results["sort"] += self.test_sort_performance()
            results["delete"] += self.test_delete_performance()
        
        for key in results:
            results[key] /= self.repeats
        
        return results
    
    def run_all_tests_raw(self):
        # Returns ALL results for each operation and repeats
        results = {"insert": [], "select": [], "update": [], "aggregate": [], "filter": [], "sort": [], "delete": []}
        for _ in range(self.repeats):
            results["insert"].append(self.test_insert_performance())
            results["select"].append(self.test_select_performance())
            results["update"].append(self.test_update_performance())
            results["aggregate"].append(self.test_aggregate_performance())
            results["filter"].append(self.test_filter_performance())
            results["sort"].append(self.test_sort_performance())
            results["delete"].append(self.test_delete_performance())
            
        return results
        
    
    def test_insert_performance(self):
        start_time = time.time()
        for i in range(self.num_records):
            self.cursor.execute(f"INSERT INTO PerformanceTest (id, name, email) VALUES ({i}, 'User{i}', 'user{i}@example.com')")
        self.conn.commit()
        assert len(self.cursor.execute("SELECT * FROM PerformanceTest").fetchall()) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Insert performance for {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time

    def test_select_performance(self):
        start_time = time.time()
        self.cursor.execute(f"SELECT * FROM PerformanceTest WHERE name = 'User{self.num_records - 1}'")
        results = self.cursor.fetchall()
        assert len(results) == 1
        
        end_time = time.time()
        if self.verbose: print(f"Select performance of 1 out of {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_update_performance(self):
        start_time = time.time()
        for i in range(self.num_records):
            self.cursor.execute(f"UPDATE PerformanceTest SET name = 'UpdatedUser{i^2}', email = 'updateduser{i^2}@example.com' WHERE id = {i}")
        self.conn.commit()
        assert len(self.cursor.execute("SELECT * FROM PerformanceTest").fetchall()) == self.num_records
        self.cursor.execute(f"SELECT * FROM PerformanceTest WHERE id = {self.num_records - 1}")
        results = self.cursor.fetchall()
        assert results[0][1] == f"UpdatedUser{(self.num_records - 1) ^ 2}"
        
        end_time = time.time()
        if self.verbose: print(f"Update performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_aggregate_performance(self):
        start_time = time.time()
        self.cursor.execute("SELECT name, email, COUNT(*) FROM PerformanceTest GROUP BY name, email")
        results = self.cursor.fetchall()
        assert len(results) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Aggregate performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_filter_performance(self):
        start_time = time.time()
        
        # Filter records that end with odd names (last digit is 13579)
        self.cursor.execute("SELECT * FROM PerformanceTest WHERE SUBSTR(name, -1) IN ('1', '3', '5', '7', '9')")
        results = self.cursor.fetchall()
        assert len(results) == self.num_records // 2
        
        end_time = time.time()
        if self.verbose: print(f"Filter performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_sort_performance(self):
        start_time = time.time()
        self.cursor.execute("SELECT * FROM PerformanceTest ORDER BY name")
        results = self.cursor.fetchall()
        assert len(results) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Sort performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_delete_performance(self):
        start_time = time.time()
        for i in range(self.num_records):
            self.cursor.execute(f"DELETE FROM PerformanceTest WHERE id = {i}")
        self.conn.commit()
        assert len(self.cursor.execute("SELECT * FROM PerformanceTest").fetchall()) == 0
        
        end_time = time.time()
        if self.verbose: print(f"Delete performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
def execute_performance_tests_avg():
    def plot_results(num_records_list, resultsSegadb_list, resultsSQLite_list):
        import matplotlib.pyplot as plt

        operations = ["insert", "select", "update", "aggregate", "filter", "sort", "delete"]
        
        for operation in operations:
            segadb_times = [results[operation] for results in resultsSegadb_list]
            sqlite_times = [results[operation] for results in resultsSQLite_list]

            plt.figure(figsize=(10, 5))
            plt.plot(num_records_list, segadb_times, label='segadb', marker='o')
            plt.plot(num_records_list, sqlite_times, label='SQLite', marker='o')

            plt.xlabel('Number of Records')
            plt.ylabel('Time (seconds)')
            plt.title(f'Performance Comparison for {operation} Operation')
            plt.legend()
            plt.grid(True)
            plt.show()
            
    # Test performance for increasing number of records   
    num_records_list = [10, 50, 100, 500, 1000, 2500, 5000, 7500, 10000]
    num_records_list = [10, 50, 100, 500, 1000]
    resultsSegadb_list = []
    resultsSQLite_list = []
    
    repeat = 5
    
    for n in num_records_list:
        testSegadb = TestDatabasePerformance(num_records=n, repeats=repeat, verbose=False)
        testSQLite = TestDatabasePerformanceSQLite(num_records=n, repeats=repeat, verbose=False)
        resultsSegadb = testSegadb.run_all_tests_avg()
        resultsSQLite = testSQLite.run_all_tests_avg()

        resultsSegadb_list.append(resultsSegadb)
        resultsSQLite_list.append(resultsSQLite)

        print(f"Results for {n} records:")
        print(f"   segadb: {resultsSegadb}")
        print(f"   SQLite: {resultsSQLite}")

    plot_results(num_records_list, resultsSegadb_list, resultsSQLite_list)

def execute_performance_tests_raw():
    import matplotlib.pyplot as plt
    import numpy as np
    
    def tsplot(ax, data, x, **kw):
        est = np.mean(data, axis=0)
        sd = np.std(data, axis=0)
        cis = (est - sd, est + sd)
        ax.fill_between(x, cis[0], cis[1], alpha=0.2, **kw)
        ax.plot(x, est, **kw)
        ax.margins(x=0)

    def plot_results(num_records_list, resultsSegadb_list, resultsSQLite_list):
        operations = ["insert", "select", "update", "aggregate", "filter", "sort", "delete"]
        
        for operation in operations:
            segadb_times = np.array([results[operation] for results in resultsSegadb_list])
            sqlite_times = np.array([results[operation] for results in resultsSQLite_list])

            plt.figure(figsize=(10, 5))
            ax = plt.gca()
            tsplot(ax, np.array(segadb_times).T, x=num_records_list)
            tsplot(ax, np.array(sqlite_times).T, x=num_records_list)
            
            plt.xlabel('Number of Records')
            plt.ylabel('Time (seconds)')
            plt.title(f'Performance Comparison for {operation} Operation')
            plt.legend(['', 'segadb', '', 'SQLite'])
            plt.grid(True)
            plt.show()
    
    # Test performance for increasing number of records   
    num_records_list = [10, 50, 100, 500, 1000, 2500, 5000, 7500, 10000]
    resultsSegadb_list = []
    resultsSQLite_list = []
    
    repeat = 5
    for n in num_records_list:
        testSegadb = TestDatabasePerformance(num_records=n, repeats=repeat, verbose=False)
        testSQLite = TestDatabasePerformanceSQLite(num_records=n, repeats=repeat, verbose=False)
        resultsSegadb = testSegadb.run_all_tests_raw()
        resultsSQLite = testSQLite.run_all_tests_raw()

        resultsSegadb_list.append(resultsSegadb)
        resultsSQLite_list.append(resultsSQLite)

        print(f"Results for {n} records:")
        print(f"   segadb: {resultsSegadb}")
        print(f"   SQLite: {resultsSQLite}")

    plot_results(num_records_list, resultsSegadb_list, resultsSQLite_list)

if __name__ == '__main__':
    # execute_performance_tests_avg()
    execute_performance_tests_raw()