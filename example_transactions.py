from segadb import *

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["id", "name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.insert({"name": "John Doe", "email": "john@example.com"})

print("Before transaction:")
users_table.print_table()

# Start a transaction
transaction = Transaction(db)
transaction.begin()

# Insert a record within a transaction
users_table.insert({"name": "Jane Doe2", "email": "jane@example.com"}, transaction=transaction)

print("\nAfter insert transaction:")
users_table.print_table()

# Rollback the transaction
transaction.rollback()
users_table = db.get_table("Users")

print("\nAfter rollback:")
users_table.print_table()

# Save the database to a file
Storage.save(db, "database.json")

# Load the database from a file
loaded_db = Storage.load("database.json")