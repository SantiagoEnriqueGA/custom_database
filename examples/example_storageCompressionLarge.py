
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

def main():
    # Create a new database
    db = Database("ExampleDB")
    user_manager = db.create_user_manager()
    db.create_authorization()

    # Register users with different roles
    user_manager.register_user("admin", "password123", roles=["admin"])
    user_manager.register_user("user1", "password123", roles=["read_only"])

    # Log in as admin
    user_manager.login_user("admin", "password123")

    # Create a new table from a CSV file
    db.create_table_from_csv("example_datasets/reviews.csv", table_name="tbl", headers=True, parallel=True, progress=False)
    
    # Save the database to a file
    print("\nSaving to example_storage/example_database.segadb:")
    Storage.save(db, "example_storage/example_database.segadb")
    
    # Print the size of the saved file
    file_size_bytes = os.path.getsize("example_storage/example_database.segadb")
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"\tSize of saved file: {file_size_mb:.4f} MB")
    
    # Save the database to a compressed file
    print("\nSaving to example_storage/example_database_compressed.segadb:")
    Storage.save(db, "example_storage/example_database_compressed.segadb", compress=True)
    
    # Print the size of the compressed file
    file_size_bytes = os.path.getsize("example_storage/example_database_compressed.segadb")
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"\tSize of compressed file: {file_size_mb:.4f} MB")
    
    
    # Load both databases from the files
    print("\nLoading databases from files:")
    print("\tLoading example_storage/example_database.segadb:")
    loaded_db = Storage.load("example_storage/example_database.segadb", 
                             user="admin", password="password123", 
                             parrallel=True
                             )
    print("\tLoading example_storage/example_database_compressed.segadb:")
    loaded_db_compressed = Storage.load("example_storage/example_database_compressed.segadb", 
                                        user="admin", password="password123", 
                                        compression=True, 
                                        parrallel=True
                                        )
    
    # Print the tables after loading
    print("\nComparing tables after loading:")
    print("Uncompressed:")
    loaded_db.get_table("tbl").print_table(limit=5, pretty=True)
    print("\nCompressed:")
    loaded_db_compressed.get_table("tbl").print_table(limit=5, pretty=True)
    
if __name__ == '__main__':
    main()