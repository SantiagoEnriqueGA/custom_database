from segadb import *

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.insert({"name": "John Doe", "email": "john@example.com"})

print("Before Insert:")
users_table.print_table()

# Add a constraint to the table
users_table.add_constraint("email", lambda x: "@" in x)
print("\n--Constraint added to the table: email must contain '@'")

users_table.try_insert({"name": "Jane Doe", "email": "janeexample.com"})    # violates the constraint
users_table.insert({"name": "Jane Doe", "email": "jane@example.com"})       # satisfies the constraint

print("\nAfter Insert:")
users_table.print_table()


# Add constraint for unique email
users_table.add_constraint("email", "UNIQUE")
print("\n--Constraint added to the table: email must be unique")

# Try to insert a record with a duplicate email
users_table.try_insert({"name": "John Doe", "email": "john@example.com"})    # violates the constraint
users_table.try_insert({"name": "James Doe", "email": "james@example.com"})   # satisfies the constraint

print("\nAfter Insert:")
users_table.print_table()