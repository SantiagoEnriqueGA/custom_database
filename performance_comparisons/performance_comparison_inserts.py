import sys
import os
import random
from faker import Faker
from multiprocessing import freeze_support
import time
import statistics
import copy
import matplotlib.pyplot as plt
import numpy as np
import sqlite3

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

def timeAndCompare():
    """
    Compare performance between regular insert and parallel insert operations.
    Records average time for multiple runs with different record counts.
    """
    repeat = 3
    num_records_list = [10, 50, 100, 500, 1_000, 2_500, 5_000, 7_500, 10_000, 15_000, 25_000]
    # num_records_list = [10, 50, 100, 500]   # For quick testing

    # Store results for each record count
    results = {n: {'insert': [], 'parallel': [], 'sqlite': []} for n in num_records_list}

    # Initialize database and tables
    db = Database("PerformanceTestDB")
    columns = ["product_id", "name", "category", "price"]
    db.create_table("products", columns)
    db.create_table("products_p", columns)

    table = db.get_table("products")
    table_p = db.get_table("products_p")

    # Create a SQLite database
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE products (product_id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)")
    conn.commit()

    # Create a fake data generator
    fake = Faker()
    
    # Run tests for each record count
    for n in num_records_list:
        print(f"\nTesting with {n} records...")
        
        for r in range(repeat):
            print(f"  Run {r + 1}/{repeat}")
            
            # Truncate tables before each run
            table.truncate()
            table_p.truncate()
            cursor.execute("DELETE FROM products")
            conn.commit()

            # Generate test records
            records = [
                {
                    "product_id": i + 1, 
                    "name": fake.word(), 
                    "category": random.choice(["Electronics", "Clothing", "Home", "Sports"]),
                    "price": round(random.uniform(10.0, 500.0), 2)
                } for i in range(n)
            ]
            
            # Test parallel insert
            records_copy = copy.deepcopy(records)  # Create a deep copy for parallel insert
            start_time = time.time()
            table_p.parallel_insert(records_copy)
            end_time = time.time()
            results[n]['parallel'].append(end_time - start_time)
            
            # Test regular insert
            start_time = time.time()
            for record in records:
                table.insert(record)
            end_time = time.time()
            results[n]['insert'].append(end_time - start_time)

            # Test insert using SQLite
            start_time = time.time()
            for record in records:
                cursor.execute(f"INSERT INTO products (product_id, name, category, price) VALUES ({record['product_id']}, '{record['name']}', '{record['category']}', {record['price']})")
                conn.commit()
            end_time = time.time()
            results[n]['sqlite'].append(end_time - start_time)

            # Check that the tables have the expected number of records
            assert len(table.records) == n
            assert len(table_p.records) == n
            assert len(cursor.execute("SELECT * FROM products").fetchall()) == n

        # Calculate and display averages for this record count
        avg_insert = statistics.mean(results[n]['insert'])
        avg_parallel = statistics.mean(results[n]['parallel'])
        avg_sqlite = statistics.mean(results[n]['sqlite'])

        std_insert = statistics.stdev(results[n]['insert']) if len(results[n]['insert']) > 1 else 0
        std_parallel = statistics.stdev(results[n]['parallel']) if len(results[n]['parallel']) > 1 else 0
        std_sqlite = statistics.stdev(results[n]['sqlite']) if len(results[n]['sqlite']) > 1 else 0
        
        print(f"Results for {n} records:")
        print(f"  Regular Insert:   {avg_insert:.4f} ± {std_insert:.4f} seconds")
        print(f"  Parallel Insert:  {avg_parallel:.4f} ± {std_parallel:.4f} seconds")
        print(f"  SQLite Insert:    {avg_sqlite:.4f} ± {std_sqlite:.4f} seconds")
    
    plot_results(results)

def tsplot(ax, data, x, **kw):
    """
    Plot time series data with confidence intervals.
    """    
    mean_estimate = np.mean(data, axis=0)
    std_dev = np.std(data, axis=0)
    ci_bounds = (mean_estimate - std_dev, mean_estimate + std_dev)
    
    # Remove 'label' from kw before passing to fill_between
    fill_kw = {k: v for k, v in kw.items() if k != "label"}

    ax.fill_between(x, ci_bounds[0], ci_bounds[1], alpha=0.2, **fill_kw)
    ax.plot(x, mean_estimate, **kw)
    ax.margins(x=0)

def plot_results(results):
    """
    Plot the performance comparison results using tsplot.
    """
    record_counts = np.array(list(results.keys()))
    insert_times = np.array([results[n]['insert'] for n in record_counts])
    parallel_times = np.array([results[n]['parallel'] for n in record_counts])
    sqlite_times = np.array([results[n]['sqlite'] for n in record_counts])
    
    plt.figure(figsize=(10, 5))
    ax = plt.gca()
    tsplot(ax, insert_times.T, x=record_counts, color='b', label='Regular Insert')
    tsplot(ax, parallel_times.T, x=record_counts, color='r', label='Parallel Insert')
    tsplot(ax, sqlite_times.T, x=record_counts, color='g', label='SQLite Insert')
    
    plt.xlabel('Number of Records')
    plt.ylabel('Time (seconds)')
    plt.title('Performance Comparison for Insert Operation')
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    # plt.show()
    plt.savefig(f"performance_comparisons/perf_comp_insert_parallel.png", dpi=600)



if __name__ == '__main__':
    freeze_support()
    timeAndCompare()
