import time
import sqlite3
import matplotlib.pyplot as plt
import numpy as np

# Change the working directory to the parent directory to allow importing the segadb package.
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb.database import Database

class TestDatabasePerformance:
    """Performance tests for the Database class."""
    def __init__(self, num_records=1000, repeats=1, verbose=False):
        """
        Initializes the performance comparison test.

        Args:
            num_records (int, optional): The number of records to be used in the test. Defaults to 1000.
            repeats (int, optional): The number of times the test should be repeated. Defaults to 1.
            verbose (bool, optional): If True, enables verbose output. Defaults to False.
        """
        self.db = Database("PerformanceTestDB")
        self.num_records = num_records
        self.repeats = repeats
        self.verbose = verbose
        
    def run_all_tests_avg(self):
        """
        Runs all performance tests multiple times and calculates the average time for each operation.
        The performance tests include:
        - Insert
        - Select
        - Update
        - Aggregate
        - Filter
        - Sort
        - Delete
        Returns:
            dict: A dictionary containing the average time for each operation.
        """
        results = {"insert": 0, "select": 0, "update": 0, "aggregate": 0, "filter": 0, "sort": 0, "join": 0, "delete": 0}
        for _ in range(self.repeats):
            results["insert"] += self.test_insert_performance()
            results["select"] += self.test_select_performance()
            results["update"] += self.test_update_performance()
            results["aggregate"] += self.test_aggregate_performance()
            results["filter"] += self.test_filter_performance()
            results["sort"] += self.test_sort_performance()
            results["join"] += self.test_join_performance()
            results["delete"] += self.test_delete_performance()
        
        for key in results:
            results[key] /= self.repeats
        
        return results
    
    def run_all_tests_raw(self):
        """
        Executes all performance tests for various database operations multiple times and returns the results.
        The method runs performance tests for the following operations:
        - Insert
        - Select
        - Update
        - Aggregate
        - Filter
        - Sort
        - Delete
        Each test is repeated a number of times specified by the `self.repeats` attribute, and the results are collected in a dictionary.
        Returns:
            dict: A dictionary containing lists of results for each operation. The keys are the operation names 
                  ("insert", "select", "update", "aggregate", "filter", "sort", "delete"), and the values are lists 
                  of results from each test run.
        """
        # Returns ALL results for each operation and repeats
        results = {"insert": [], "select": [], "update": [], "aggregate": [], "filter": [], "sort": [], "join": [], "delete": []}
        for _ in range(self.repeats):
            results["insert"].append(self.test_insert_performance())
            results["select"].append(self.test_select_performance())
            results["update"].append(self.test_update_performance())
            results["aggregate"].append(self.test_aggregate_performance())
            results["filter"].append(self.test_filter_performance())
            results["sort"].append(self.test_sort_performance())
            results["join"].append(self.test_join_performance())
            results["delete"].append(self.test_delete_performance())
            
        return results
     
    def test_insert_performance(self):
        """
        Tests the performance of inserting records into a database table.
        This method creates a new table named "PerformanceTest" with columns "id", "name", and "email".
        It then inserts a specified number of records into the table and measures the time taken to perform the insertions.
        The method asserts that the number of records in the table matches the expected number of records.
        Returns:
            float: The time taken to insert the records, in seconds.
        """
        # Create a new table for testing
        self.db.create_table("PerformanceTest", ["id", "name", "email"])
        test_table = self.db.get_table("PerformanceTest")
        
        # Test inserting records
        start_time = time.time()
        # for i in range(self.num_records):
        #     test_table.insert({"id": i, "name": f"User{i}", "email": f"user{i}@example.com"})

        records = [{"id": i, "name": f"User{i}", "email": f"user{i}@example.com"} for i in range(self.num_records)]
        test_table.parallel_insert(records)

        assert len(test_table.records) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Insert performance for {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_select_performance(self):
        """
        Test the performance of selecting a record from the database.
        This method measures the time it takes to select a specific record from the 
        "PerformanceTest" table in the database. The record is selected based on the 
        condition that the "name" field matches "User{self.num_records - 1}". The method 
        asserts that exactly one record is returned and prints the time taken for the 
        operation if verbose mode is enabled.
        Returns:
            float: The time taken to perform the select operation in seconds.
        """
        # Test selecting records
        start_time = time.time()
        results = self.db.get_table("PerformanceTest").select(lambda record: record.data["name"] == f"User{self.num_records - 1}")
        assert len(results) == 1
        
        end_time = time.time()
        if self.verbose: print(f"Select performance of 1 out of {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_update_performance(self):
        """
        Tests the performance of updating records in the database.
        This method updates the "name" and "email" fields of each record in the "PerformanceTest" table.
        It measures the time taken to perform the updates and asserts that the updates were successful.
        Returns:
            float: The time taken to update the records, in seconds.
        """
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
        """
        Tests the performance of aggregating records in the database.
        This method performs an aggregation operation on the "name" and "email" fields of the "PerformanceTest" table.
        It measures the time taken to perform the aggregation and asserts that the aggregation was successful.
        Returns:
            float: The time taken to aggregate the records, in seconds.
        """
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        agg_table = test_table.aggregate("name", "email", "COUNT")
        assert len(agg_table.records) == len(test_table.records)
        
        end_time = time.time()
        if self.verbose: print(f"Aggregate performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_filter_performance(self):
        """
        Tests the performance of filtering records in the database.
        This method filters records in the "PerformanceTest" table based on a condition.
        It measures the time taken to perform the filtering and asserts that the filtering was successful.
        Returns:
            float: The time taken to filter the records, in seconds.
        """
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        
        # Filter recors the end with odd names
        filtered_table = test_table.filter(lambda record: record.data["name"][-1] in "13579")
        assert len(filtered_table.records) == self.num_records // 2
        
        end_time = time.time()
        if self.verbose: print(f"Filter performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_sort_performance(self):
        """
        Tests the performance of sorting records in the database.
        This method sorts the records in the "PerformanceTest" table by the "name" field.
        It measures the time taken to perform the sorting and asserts that the sorting was successful.
        Returns:
            float: The time taken to sort the records, in seconds.
        """
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        
        # Sort records by name
        sorted_table = test_table.sort("name")
        assert len(sorted_table.records) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Sort performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_join_performance(self):
        """
        Tests the performance of joining two tables in the database.
        This method creates two identical tables and performs an inner join operation between them.
        It measures the time taken to perform the join operation and asserts that the join was successful.
        Returns:
            float: The time taken to perform the join operation, in seconds.
        """
        test_table1 = self.db.get_table("PerformanceTest")
        test_table2 = self.db.get_table("PerformanceTest")
        
        start_time = time.time()
        joined_table = test_table1.join(test_table2, "name", "name")
        assert len(joined_table.records) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Join performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_delete_performance(self):
        """
        Tests the performance of deleting records from the database.
        This method deletes all records from the "PerformanceTest" table.
        It measures the time taken to perform the deletions and asserts that the deletions were successful.
        Returns:
            float: The time taken to delete the records, in seconds.
        """
        start_time = time.time()
        test_table = self.db.get_table("PerformanceTest")
        for i in range(self.num_records):
            test_table.delete(i)
        assert len(test_table.records) == 0
        
        end_time = time.time()
        if self.verbose: print(f"Delete performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
class TestDatabasePerformanceSQLite:
    """Performance tests for SQLite."""
    def __init__(self, num_records=1000, repeats=1, verbose=False):
        """
        Initializes the performance comparison test for SQLite.

        Args:
            num_records (int, optional): The number of records to be used in the test. Defaults to 1000.
            repeats (int, optional): The number of times the test should be repeated. Defaults to 1.
            verbose (bool, optional): If True, enables verbose output. Defaults to False.
        """
        self.num_records = num_records
        self.repeats = repeats
        self.verbose = verbose
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE PerformanceTest (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
        self.conn.commit()
    
    def run_all_tests_avg(self):
        """
        Runs all performance tests multiple times and calculates the average time for each operation.
        The performance tests include:
        - Insert
        - Select
        - Update
        - Aggregate
        - Filter
        - Sort
        - Delete
        Returns:
            dict: A dictionary containing the average time for each operation.
        """
        results = {"insert": 0, "select": 0, "update": 0, "aggregate": 0, "filter": 0, "sort": 0, "join": 0, "delete": 0}
        for _ in range(self.repeats):
            results["insert"] += self.test_insert_performance()
            results["select"] += self.test_select_performance()
            results["update"] += self.test_update_performance()
            results["aggregate"] += self.test_aggregate_performance()
            results["filter"] += self.test_filter_performance()
            results["sort"] += self.test_sort_performance()
            results["join"] += self.test_join_performance()
            results["delete"] += self.test_delete_performance()
        
        for key in results:
            results[key] /= self.repeats
        
        return results
    
    def run_all_tests_raw(self):
        """
        Executes all performance tests for various database operations multiple times and returns the results.
        The method runs performance tests for the following operations:
        - Insert
        - Select
        - Update
        - Aggregate
        - Filter
        - Sort
        - Delete
        Each test is repeated a number of times specified by the `self.repeats` attribute, and the results are collected in a dictionary.
        Returns:
            dict: A dictionary containing lists of results for each operation. The keys are the operation names 
                  ("insert", "select", "update", "aggregate", "filter", "sort", "delete"), and the values are lists 
                  of results from each test run.
        """
        # Returns ALL results for each operation and repeats
        results = {"insert": [], "select": [], "update": [], "aggregate": [], "filter": [], "sort": [], "join": [], "delete": []}
        for _ in range(self.repeats):
            results["insert"].append(self.test_insert_performance())
            results["select"].append(self.test_select_performance())
            results["update"].append(self.test_update_performance())
            results["aggregate"].append(self.test_aggregate_performance())
            results["filter"].append(self.test_filter_performance())
            results["sort"].append(self.test_sort_performance())
            results["join"].append(self.test_join_performance())
            results["delete"].append(self.test_delete_performance())
            
        return results
        
    
    def test_insert_performance(self):
        """
        Tests the performance of inserting records into a SQLite database table.
        This method inserts a specified number of records into the "PerformanceTest" table and measures the time taken to perform the insertions.
        The method asserts that the number of records in the table matches the expected number of records.
        Returns:
            float: The time taken to insert the records, in seconds.
        """
        start_time = time.time()
        for i in range(self.num_records):
            self.cursor.execute(f"INSERT INTO PerformanceTest (id, name, email) VALUES ({i}, 'User{i}', 'user{i}@example.com')")
        self.conn.commit()
        assert len(self.cursor.execute("SELECT * FROM PerformanceTest").fetchall()) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Insert performance for {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time

    def test_select_performance(self):
        """
        Tests the performance of selecting a record from the SQLite database.
        This method measures the time it takes to select a specific record from the "PerformanceTest" table in the database.
        The record is selected based on the condition that the "name" field matches "User{self.num_records - 1}".
        The method asserts that exactly one record is returned and prints the time taken for the operation if verbose mode is enabled.
        Returns:
            float: The time taken to perform the select operation, in seconds.
        """
        start_time = time.time()
        self.cursor.execute(f"SELECT * FROM PerformanceTest WHERE name = 'User{self.num_records - 1}'")
        results = self.cursor.fetchall()
        assert len(results) == 1
        
        end_time = time.time()
        if self.verbose: print(f"Select performance of 1 out of {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_update_performance(self):
        """
        Tests the performance of updating records in the SQLite database.
        This method updates the "name" and "email" fields of each record in the "PerformanceTest" table.
        It measures the time taken to perform the updates and asserts that the updates were successful.
        Returns:
            float: The time taken to update the records, in seconds.
        """
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
        """
        Tests the performance of aggregating records in the SQLite database.
        This method performs an aggregation operation on the "name" and "email" fields of the "PerformanceTest" table.
        It measures the time taken to perform the aggregation and asserts that the aggregation was successful.
        Returns:
            float: The time taken to aggregate the records, in seconds.
        """
        start_time = time.time()
        self.cursor.execute("SELECT name, email, COUNT(*) FROM PerformanceTest GROUP BY name, email")
        results = self.cursor.fetchall()
        assert len(results) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Aggregate performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_filter_performance(self):
        """
        Tests the performance of filtering records in the SQLite database.
        This method filters records in the "PerformanceTest" table based on a condition.
        It measures the time taken to perform the filtering and asserts that the filtering was successful.
        Returns:
            float: The time taken to filter the records, in seconds.
        """
        start_time = time.time()
        
        # Filter records that end with odd names (last digit is 13579)
        self.cursor.execute("SELECT * FROM PerformanceTest WHERE SUBSTR(name, -1) IN ('1', '3', '5', '7', '9')")
        results = self.cursor.fetchall()
        assert len(results) == self.num_records // 2
        
        end_time = time.time()
        if self.verbose: print(f"Filter performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_sort_performance(self):
        """
        Tests the performance of sorting records in the SQLite database.
        This method sorts the records in the "PerformanceTest" table by the "name" field.
        It measures the time taken to perform the sorting and asserts that the sorting was successful.
        Returns:
            float: The time taken to sort the records, in seconds.
        """
        start_time = time.time()
        self.cursor.execute("SELECT * FROM PerformanceTest ORDER BY name")
        results = self.cursor.fetchall()
        assert len(results) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Sort performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_join_performance(self):
        """
        Tests the performance of joining two tables in the SQLite database.
        This method creates two identical tables and performs an inner join operation between them.
        It measures the time taken to perform the join operation and asserts that the join was successful.
        Returns:
            float: The time taken to perform the join operation, in seconds.
        """
        # Drop PerformanceTest2 table if it exists
        self.cursor.execute("DROP TABLE IF EXISTS PerformanceTest2")
        
        # Copy PerformanceTest table to PerformanceTest2
        self.cursor.execute("CREATE TABLE PerformanceTest2 AS SELECT * FROM PerformanceTest")
        
        start_time = time.time()
        self.cursor.execute("SELECT * FROM PerformanceTest JOIN PerformanceTest2 ON PerformanceTest.name = PerformanceTest2.name")
        results = self.cursor.fetchall()
        assert len(results) == self.num_records
        
        end_time = time.time()
        if self.verbose: print(f"Join performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    
    def test_delete_performance(self):
        """
        Tests the performance of deleting records from the SQLite database.
        This method deletes all records from the "PerformanceTest" table.
        It measures the time taken to perform the deletions and asserts that the deletions were successful.
        Returns:
            float: The time taken to delete the records, in seconds.
        """
        start_time = time.time()
        for i in range(self.num_records):
            self.cursor.execute(f"DELETE FROM PerformanceTest WHERE id = {i}")
        self.conn.commit()
        assert len(self.cursor.execute("SELECT * FROM PerformanceTest").fetchall()) == 0
        
        end_time = time.time()
        if self.verbose: print(f"Delete performance on {self.num_records} [id, name, email] records: {(end_time - start_time):.2} seconds.")
        return end_time - start_time
    

def tsplot(ax, data, x, **kw):
    """
    Plot time series data with confidence intervals.
    
    Args:
        ax (matplotlib.axes.Axes): The axes on which to plot the data.
        data (numpy.ndarray): The time series data to plot.
        x (numpy.ndarray): The x-axis values.
        **kw: Additional keyword arguments to pass to the plot function.
    """    
    # Compute the mean and confidence intervals
    mean_estimate = np.mean(data, axis=0)
    std_dev = np.std(data, axis=0)
    ci_bounds = (mean_estimate - std_dev, mean_estimate + std_dev)
    
    # Remove 'label' from kw before passing to fill_between
    fill_kw = {k: v for k, v in kw.items() if k != "label"}

    ax.fill_between(x, ci_bounds[0], ci_bounds[1], alpha=0.2, **fill_kw)
    ax.plot(x, mean_estimate, **kw)
    ax.margins(x=0)     # Remove margins to prevent clipping

def plot_results(num_records_list, resultsSegadb_list, resultsSQLite_list):
    """
    Plot the performance comparison results for each operation.
    
    Args:
        num_records_list (list): A list of the number of records used in the tests.
        resultsSegadb_list (list): A list of dictionaries containing the performance results for Segadb.
        resultsSQLite_list (list): A list of dictionaries containing the performance results for SQLite.
    """
    operations = ["insert", "select", "update", "aggregate", "filter", "sort", "join", "delete"]
    for operation in operations:
        # Extract the times for the current operation
        segadb_times = np.array([results[operation] for results in resultsSegadb_list])
        sqlite_times = np.array([results[operation] for results in resultsSQLite_list])

        # Plot the results
        plt.figure(figsize=(10, 5))
        ax = plt.gca()
        tsplot(ax, np.array(segadb_times).T, x=num_records_list, color='b', label='segadb')
        tsplot(ax, np.array(sqlite_times).T, x=num_records_list, color='g', label='SQLite')
        plt.xlabel('Number of Records')
        plt.ylabel('Time (seconds)')
        plt.title(f'Performance Comparison for {operation.capitalize()} Operation')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # plt.show()
        plt.savefig(f"performance_comparisons/perf_comp_{operation}.png", dpi=600)

def main():
    """Test performance and compare with SQLite."""

    repeat = 10
    num_records_list = [10, 50, 100, 500, 1000, 2500, 5000, 7500, 10000, 15000, 20000]
    # num_records_list = [10, 50, 100, 500, 1000]   # For quick testing
    
    resultsSegadb_list = []
    resultsSQLite_list = []
    
    def print_results(label, results):
        """Print the average time for each operation."""
        print(f"   {label}:", end="")
        # For each operation, calculate the average time
        avg = {key: sum(results[key]) / len(results[key]) for key in results}
        
        # Print the average time for each operation on the same line
        for operation in ["insert", "select", "update", "aggregate", "filter", "sort", "join", "delete"]:
            print(f" {operation.capitalize()}: {avg[operation]:.4f}", end="")
        print()
    
    for n in num_records_list:
        # Initialize the performance tests
        testSegadb = TestDatabasePerformance(num_records=n, repeats=repeat, verbose=False)
        testSQLite = TestDatabasePerformanceSQLite(num_records=n, repeats=repeat, verbose=False)
        
        # Run the performance tests and collect the results
        resultsSegadb = testSegadb.run_all_tests_raw()
        resultsSQLite = testSQLite.run_all_tests_raw()

        # Append the results to the lists
        resultsSegadb_list.append(resultsSegadb)
        resultsSQLite_list.append(resultsSQLite)

        print(f"\nResults for {n} records:")
        print_results("segadb", resultsSegadb)
        print_results("SQLite", resultsSQLite)
        

    plot_results(num_records_list, resultsSegadb_list, resultsSQLite_list)

if __name__ == '__main__':
    main()