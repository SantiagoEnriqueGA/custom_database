from segadb import Database, Storage, Transaction

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.try_insert({"name": "John1 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John2 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John3 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John4 Doe", "email": "john@example.com"})
users_table.try_insert({"name": "John5 Doe", "email": "john@example.com"})

print("Before Insert:")
users_table.print_table()

# Modify the record id
users_table.records[0].id = 100

print("\nAfter Changing ID 1:")
users_table.print_table()

# Can insert the record with desired id
users_table.try_insert({"id": 100, "name": "John6 Doe", "email": "john@example.com"})
users_table.try_insert({"id": 1, "name": "John7 Doe", "email": "john@example.com"})

print("\nAfter Insert with ID 1:")
users_table.print_table()
