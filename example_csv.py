from segadb import *

# Create a new database
db = Database("MyTestDB")

# Create a new table from a CSV file
db.create_table_from_csv("example_datasets/pima-indians-diabetes.csv", "WBP", headers=True)

# Print the table
db.get_table("WBP").print_table(limit=5)
print("\n")
db.get_table("WBP").print_table(limit=5, pretty=True)

