# Imports: Third party
import bcrypt 

PRESET_ROLES = {
    "admin": ["create_table", "delete_table", "update_table", "read_table"],
    "read_only": ["read_table"],
    "editor": ["update_table", "read_table"]
}

class User:
    def __init__(self, username, password, roles=None):
        """
        Initialize a new User instance.
        Args:
            username (str): The username of the user.
            password (str): The password of the user.
            roles (list, optional): A list of roles assigned to the user. Defaults to an empty list.
        Attributes:
            username (str): The username of the user.
            password_hash (str): The hashed password of the user.
            roles (list): A list of roles assigned to the user.
        """
        self.username = username
        self.password_hash = self.hash_password(password)
        self.roles = roles if roles else []

    @staticmethod
    def hash_password(password):
        """
        Hashes a password using bcrypt.
        Args:
            password (str): The password to be hashed.
        Returns:
            bytes: The hashed password.
        """
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    def check_password(self, password):
        """
        Check if the provided password matches the stored password hash.
        Args:
            password (str): The plaintext password to check.
        Returns:
            bool: True if the password matches the stored hash, False otherwise.
        """
        return bcrypt.checkpw(password.encode('utf-8'), self.password_hash)
    
class UserManager:
    def __init__(self, db):
        """
        Initializes the Users class.
        Args:
            db (Database): The database object to use for user management.
        """
        self.db = db

    def register_user(self, username, password, roles=None):
        """
        Registers a new user with the given username, password, and optional roles.
        Args:
            username (str): The username for the new user.
            password (str): The password for the new user.
            roles (list, optional): A list of roles assigned to the new user. Defaults to None.
        Raises:
            ValueError: If the username already exists in the users dictionary.
        """
        self.db.register_user(username, password, roles)

    def authenticate_user(self, username, password):
        """
        Authenticate a user by their username and password.
        Args:
            username (str): The username of the user.
            password (str): The password of the user.
        Returns:
            User: The authenticated user object if the username and password are correct.
            None: If the authentication fails.
        """
        return self.db.authenticate_user(username, password)
    
    def get_user_permissions(self, username):
        """
        Get the permissions associated with a user.
        Args:
            username (str): The username of the user.
        Returns:
            list: A list of permissions associated with the user.
        """
        user = self.db.get_user(username)
        if user:
            permissions = []
            for role in user["roles"]:
                permissions.extend(PRESET_ROLES.get(role, []))
            return permissions
        return []

    def login_user(self, username, password):
        """
        Logs in a user by their username and password.
        Args:
            username (str): The username of the user.
            password (str): The password of the user.
        Returns:
            str: A session token if login is successful.
            None: If the login fails.
        """
        user = self.authenticate_user(username, password)
        if user:
            session_token = self.db.create_session(username)
            self.db.active_session = session_token
            return session_token
        return None

    def logout_user(self, session_token):
        """
        Logs out a user by their session token.
        Args:
            session_token (str): The session token of the user.
        """
        self.db.delete_session(session_token)
        self.db.active_session = None
        
    def remove_user(self, username):
        """
        Removes a user from the database.
        Args:
            username (str): The username of the user to be removed.
        """
        self.db.remove_user(username)
        print(f"User {username} has been removed.")
    
class Authorization:
    def __init__(self, db):
        """
        Initializes a new instance of the class.
        Attributes:
            permissions (dict): A dictionary to store user permissions.
        """
        self.db = db

    def add_permission(self, username, role):
        """
        Adds a permission to a specified role.
        Args:
            role (str): The role to which the permission will be added.
            permission (str): The permission to be added to the role.
        """
        self.db.add_role(username, role)


    def check_permission(self, username, permission):
        """
        Check if a user has a specific permission.
        Args:
            username (str): The username of the user.
            role (str): The role to check for.
        Returns:
            bool: True if the user has the permission, False otherwise.
        """
        user = self.db.get_user(username)
        if user:
            for role in user["roles"]:
                if permission in PRESET_ROLES.get(role, []):
                    return True
        return False