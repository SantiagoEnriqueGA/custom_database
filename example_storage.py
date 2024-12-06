from segadb import *

# Create a new database
db = Database("ExampleDB")

# Create two tables: users and orders
db.create_table("users", ["user_id", "name", "email"])
db.create_table("orders", ["user_id", "product", "order_id", "order_date"])

# Add a unique constraint to the users table on the id column
db.add_constraint("users", "user_id", "UNIQUE")

# Add a foreign key constraint to the orders table on the user_id column (user_id in orders must exist in users)
db.add_constraint("orders", "user_id", "FOREIGN_KEY", reference_table_name="users", reference_column="user_id")

# Insert data into the users table
db.get_table("users").insert({"user_id": 1, "name": "Alice", "email": "alice@abc.com"})
db.get_table("users").insert({"user_id": 2, "name": "Bob", "email": "bob@abc.com"})
db.get_table("users").insert({"user_id": 3, "name": "Charlie", "email": "charlie@abc.com"})
db.get_table("users").insert({"user_id": 4, "name": "David", "email": "david@abc.com"})

# Insert data into the orders table
db.get_table("orders").insert({"user_id": 1, "product": "Laptop", "order_id": 1, "order_date": "2021-01-01"})
db.get_table("orders").insert({"user_id": 2, "product": "Phone", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Laptop", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 2, "product": "Smartwatch", "order_id": 2, "order_date": "2021-01-02"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 3, "product": "Tablet", "order_id": 3, "order_date": "2021-01-03"})
db.get_table("orders").insert({"user_id": 4, "product": "Smartwatch", "order_id": 4, "order_date": "2021-01-04"})

# Print the tables before saving
print("Before Saving:")
print("--------------------------------------------------------------------------------")
db.print_db()


# Save the database to a file
print("\n--------------------------------------------------------------------------------")
print("Saving to database.json:")
print("--------------------------------------------------------------------------------")
Storage.save(db, "database.json")

# Load the database from a file
loaded_db = Storage.load("database.json")


# Print the tables after loading
print("\nAfter Loading:")
print("--------------------------------------------------------------------------------")
# print(loaded_db)
loaded_db.print_db()


# Check constraints
print("\nCheck constraints - Unique constraint violation:")
print("--------------------------------------------------------------------------------")
print("\nTry to insert order with non-existent user_id in orders table:")
loaded_db.get_table("users").try_insert({"user_id": 1, "name": "Alice", "email": "alice@abc.com"})

print("\nCheck constraints - Foreign key constraint violation:")
print("--------------------------------------------------------------------------------")
print("Try to insert order with non-existent user_id in orders table:")
loaded_db.get_table("orders").try_insert({"user_id": 5, "product": "Smartwatch", "order_id": 5, "order_date": "2021-01-05"})

