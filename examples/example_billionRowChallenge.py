import sys
import os
import time

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

if __name__ == '__main__':
    # Create a new database
    db = Database("BRC_DB")

    # Time the creation of the first table
    start_time = time.time()
    db.create_table_from_csv("example_datasets/measurements_s.txt", "BRC", 
                             headers=False, delim=';', 
                             column_names=['station', 'measure'],  col_types=[str, float],
                             progress=True
                             )
    end_time = time.time()
    print(f"Time to load table BRC: {end_time - start_time} seconds")

    # Time the creation of the second table using multiprocessing
    start_time = time.time()
    db.create_table_from_csv_multiprocessing("example_datasets/measurements_s.txt", "BRC_MP", 
                                             headers=False, delim=';', 
                                             column_names=['station', 'measure'],  col_types=[str, float],
                                             progress=True
                                             )
    end_time = time.time()
    print(f"Time to load table BRC_MP: {end_time - start_time} seconds")

    print("Table BRC")
    avg_temp_mp = db.get_table("BRC_MP").aggregate("station", "measure", "AVG")
    avg_temp_mp.sort("station", ascending=True).print_table(pretty=True)
    
    print("\nTable BRC_MP")
    avg_temp_mp = db.get_table("BRC_MP").aggregate("station", "measure", "AVG")
    avg_temp_mp.sort("station", ascending=True).print_table(pretty=True)
    
    
    # Current results:
    # Time to load table BRC: 411.74463176727295 seconds
    # Time to load table BRC_MP: 342.772310256958 seconds