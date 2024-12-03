from segadb import Database, Storage, Table

# Create a new database
db = Database("MyTestDB")

# Create a new table
db.create_table("Users", ["id", "name", "email"])

# Insert a record
users_table = db.get_table("Users")
users_table.insert({"name": "John Doe", "email": "john@example.com"})

# Save the database to a file
Storage.save(db, "database.json")

# Load the database from a file
loaded_db = Storage.load("database.json")
