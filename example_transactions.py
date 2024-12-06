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

# Preview the transaction
print("\nPreview transaction:")
transaction.preview()

# Rollback the transaction
transaction.rollback()
users_table = db.get_table("Users")

print("\nAfter rollback:")
users_table.print_table()

# Insert a record within a transaction
users_table.insert({"name": "Jane Doe2", "email": "jane@example.com"}, transaction=transaction)

# Commit the transaction
transaction.commit()

print("\nAfter commit:")
users_table.print_table()