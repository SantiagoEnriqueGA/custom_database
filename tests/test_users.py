
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.database import Database
from segadb.users import User, UserManager, Authorization, PRESET_ROLES
from test_utils import suppress_print

class TestUser(unittest.TestCase):
    """
    Unit tests for the User class.
    Methods:
    - test_user_creation: Tests the creation of a User object.
    - test_user_roles: Tests the roles of a User object.
    """
    def setUp(self):
        self.username = "testuser"
        self.password = "password123"
        self.user = User(self.username, self.password)

    def test_user_creation(self):
        self.assertEqual(self.user.username, self.username)
        self.assertTrue(self.user.check_password(self.password))
        self.assertFalse(self.user.check_password("wrongpassword"))

    def test_user_roles(self):
        user_with_roles = User(self.username, self.password, roles=["admin", "editor"])
        self.assertIn("admin", user_with_roles.roles)
        self.assertIn("editor", user_with_roles.roles)

class TestUserManager(unittest.TestCase):
    """
    Unit tests for the UserManager class.
    Methods:
    - test_register_user: Tests registering a new user.
    - test_authenticate_user: Tests authenticating a user.
    - test_get_user_permissions: Tests getting user permissions.
    - test_login_user: Tests logging in a user.
    - test_logout_user: Tests logging out a user.
    - test_remove_user: Tests removing a user.
    """
    def setUp(self):
        self.db = Mock(spec=Database)
        self.user_manager = UserManager(self.db)

    def test_register_user(self):
        self.user_manager.register_user("newuser", "newpassword", ["admin"])
        self.db.register_user.assert_called_with("newuser", "newpassword", ["admin"])

    def test_authenticate_user(self):
        self.db.authenticate_user.return_value = User("testuser", "password123")
        user = self.user_manager.authenticate_user("testuser", "password123")
        self.assertIsNotNone(user)
        self.assertEqual(user.username, "testuser")

    def test_get_user_permissions(self):
        self.db.get_user.return_value = {"roles": ["admin", "read_only"]}
        permissions = self.user_manager.get_user_permissions("testuser")
        expected_permissions = PRESET_ROLES["admin"] + PRESET_ROLES["read_only"]
        self.assertListEqual(permissions, expected_permissions)

    def test_login_user(self):
        self.db.authenticate_user.return_value = User("testuser", "password123")
        self.db.create_session.return_value = "sessiontoken123"
        session_token = self.user_manager.login_user("testuser", "password123")
        self.assertEqual(session_token, "sessiontoken123")

    def test_logout_user(self):
        self.user_manager.logout_user("sessiontoken123")
        self.db.delete_session.assert_called_with("sessiontoken123")
        
    def test_remove_user(self):
        with suppress_print():
            self.user_manager.remove_user("testuser")
            self.db.remove_user.assert_called_with("testuser")

class TestAuthorization(unittest.TestCase):
    """
    Unit tests for the Authorization class.
    Methods:
    - test_add_permission: Tests adding a permission to a user.
    - test_check_permission: Tests checking if a user has a permission.
    """
    def setUp(self):
        self.db = Mock(spec=Database)
        self.authorization = Authorization(self.db)

    def test_add_permission(self):
        self.authorization.add_permission("testuser", "editor")
        self.db.add_role.assert_called_with("testuser", "editor")

    def test_check_permission(self):
        self.db.get_user.return_value = {"roles": ["admin"]}
        has_permission = self.authorization.check_permission("testuser", "create_table")
        self.assertTrue(has_permission)
        has_permission = self.authorization.check_permission("testuser", "nonexistent_permission")
        self.assertFalse(has_permission)

if __name__ == '__main__':
    unittest.main()
