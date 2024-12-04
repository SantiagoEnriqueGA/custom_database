from segadb import Database, Storage, Transaction

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["id", "name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.insert({"name": "John Doe", "email": "john@example.com"})

print("Before Insert:")
users_table.print_table()

# Add a constraint to the table
users_table.add_constraint("email", lambda x: "@" in x)

# Try to insert a record that violates the constraint
users_table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})

# Try to insert a record that satisfies the constraint
users_table.insert({"name": "Jane Doe", "email": "jane@example.com"})

print("\nAfter Insert:")
users_table.print_table()