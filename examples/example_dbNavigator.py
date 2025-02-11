import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *
import curses

# Load navigator
if __name__ == "__main__":
    db = Database.load_sample_database()    # Load the database here
    # db = Database.load_sample_database(n_users=1000, n_orders=10_000, n_products=50, n_reviews=200, n_categories=10, n_suppliers=20)
    curses.wrapper(db_navigator, db)        # Pass the database to the navigator





