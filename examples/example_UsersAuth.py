import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb import *

# Initialize Database and UserManager
db = Database("MyTestDB")
user_manager = UserManager(db)
auth = Authorization(db)

# Register users
user_manager.register_user("admin", "password123", roles=["admin"])
user_manager.register_user("user1", "password123", roles=["read_only"])

# Authenticate users
admin = user_manager.authenticate_user("admin", "password123")
user1 = user_manager.authenticate_user("user1", "password123")

print(f"Admin permisions: {user_manager.get_user_permissions('admin')}")
print(f"User1 permisions: {user_manager.get_user_permissions('user1')}")

# Log in users
admin_session = user_manager.login_user("admin", "password123")
user1_session = user_manager.login_user("user1", "password123")

print(f"\nAdmin session token: {admin_session}")
print(f"\nUser1 session token: {user1_session}")


# Try creating a table with each user
print("\nTrying to create a table with each user...")
try:
    db.create_table("test_table", ["col1", "col2"], session_token=admin_session)
    print("Admin created a table.")
except PermissionError as e:
    print(f"Admin failed to create a table: {e}")

try:
    db.create_table("test_table2", ["col1", "col2"], session_token=user1_session)
    print("User1 created a table.")
except PermissionError as e:
    print(f"User1 failed to create a table: {e}")

# Try dropping a table with each user
print("\nTrying to drop a table with each user...")
try:
    db.drop_table("test_table", session_token=admin_session)
    print("Admin dropped a table.")
except PermissionError as e:
    print(f"Admin failed to drop a table: {e}")

try:
    db.drop_table("test_table2", session_token=user1_session)
    print("User1 dropped a table.")
except PermissionError as e:
    print(f"User1 failed to drop a table: {e}")


# Log out users
user_manager.logout_user(admin_session)
user_manager.logout_user(user1_session)

print("\nUsers logged out.")

