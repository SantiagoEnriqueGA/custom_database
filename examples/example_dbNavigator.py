import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *
import curses

# Load navigator
if __name__ == "__main__":
    # Load the database
    # db = Database.load_sample_database()
    db = Database.load_sample_database(n_users=1000, n_orders=10_000, n_products=50, n_reviews=200, n_categories=10, n_suppliers=20)
    
    # Two ways to run the navigator
    # curses.wrapper(db_navigator, db)        # Pass the database to the navigator
    db.show_db_with_curses()                # Call the method from the database object





