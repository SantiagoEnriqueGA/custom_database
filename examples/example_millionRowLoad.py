# See createMeasurements.py for the creation of the example dataset.
# Inspired by BRC's example, but with a reduced number of records.
# The example dataset has 1_000_000 records.
#   The table has two columns: station (str) and measure (float).
#   The dataset is a text file with ';' as the delimiter.
#   The dataset has no headers.

import sys
import os
import time

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

if __name__ == '__main__':
    # Create a new database
    db = Database("BRC_DB")

    # --------------------------------------------------------------------------------
    # Note: This method is not recommended, will take a long time to load the table.
    # --------------------------------------------------------------------------------
    # Time the creation of the table using the default method
    # start_time = time.time()
    # db.create_table_from_csv("example_datasets/measurements_s.txt", "MillionRowTable", 
    #                          headers=False, delim=';', 
    #                          column_names=['station', 'measure'],  col_types=[str, float],
    #                          progress=True
    #                          )
    # end_time = time.time()
    # print(f"Time to load table default: {end_time - start_time} seconds")

    # # Aggregate the tables and print the results
    # print("Table MillionRowTable")
    # avg_temp = db.get_table("MillionRowTable").aggregate("station", "measure", "AVG")
    # avg_temp.sort("station", ascending=True).print_table(pretty=True)

    # Time the creation of the second table using multiprocessing
    start_time = time.time()
    db.create_table_from_csv("example_datasets/measurements_s.txt", "MillionRowTable_MP", 
                                             headers=False, delim=';', 
                                             column_names=['station', 'measure'],  col_types=[str, float],
                                             progress=True, parrallel=True
                                             )
    end_time = time.time()
    print(f"Time to load table with multiprocessing: {(end_time - start_time):.2f} seconds")

    
    print("\nFirst 10 rows of the table MillionRowTable_MP")
    avg_temp_mp = db.get_table("MillionRowTable_MP").aggregate("station", "measure", "AVG")
    avg_temp_mp.sort("station", ascending=True).print_table(pretty=True, limit=10)
    
    # Current best, 1_000_000 records: 9.45 seconds

