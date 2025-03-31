# tests/test_index.py

import unittest
import sys
import os

# Adjust path to import segadb
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.index import Index

class TestIndex(unittest.TestCase):
    """
    Unit tests for the updated Index class.
    """
    @classmethod
    def setUpClass(cls):
        print("\nTesting Index Class...", end="", flush=True)

    def setUp(self):
        """Set up common index instances for tests."""
        # Non-unique index on 'email' column
        self.email_index = Index(name="idx_email", column="email", unique=False)
        # Unique index on 'username' column
        self.username_index = Index(name="idx_username", column="username", unique=True)

    # --- Initialization Tests ---
    def test_init_success(self):
        """Test successful initialization."""
        self.assertEqual(self.email_index.name, "idx_email")
        self.assertEqual(self.email_index.column, "email")
        self.assertFalse(self.email_index.unique)
        self.assertEqual(len(self.email_index.index_data), 0)

        self.assertEqual(self.username_index.name, "idx_username")
        self.assertEqual(self.username_index.column, "username")
        self.assertTrue(self.username_index.unique)
        self.assertEqual(len(self.username_index.index_data), 0)

    def test_init_empty_name_fails(self):
        """Test initialization fails with empty name."""
        with self.assertRaisesRegex(ValueError, "Index name cannot be empty"):
            Index(name="", column="email")

    def test_init_empty_column_fails(self):
        """Test initialization fails with empty column."""
        with self.assertRaisesRegex(ValueError, "Index column cannot be empty"):
            Index(name="idx_email", column="")

    # --- Add & Find (Non-Unique) Tests ---
    def test_add_and_find_single_non_unique(self):
        """Test adding and finding a single record ID."""
        self.email_index.add("test@example.com", 1)
        self.assertEqual(self.email_index.find("test@example.com"), [1])
        self.assertEqual(self.email_index.find("other@example.com"), [])

    def test_add_and_find_multiple_same_key_non_unique(self):
        """Test adding multiple record IDs for the same key."""
        self.email_index.add("test@example.com", 1)
        self.email_index.add("test@example.com", 5)
        self.email_index.add("test@example.com", 3)
        # Order might not be guaranteed, so use assertCountEqual (or sort)
        self.assertCountEqual(self.email_index.find("test@example.com"), [1, 5, 3])

    def test_add_and_find_multiple_different_keys_non_unique(self):
        """Test adding records for different keys."""
        self.email_index.add("test@example.com", 1)
        self.email_index.add("another@example.com", 2)
        self.assertEqual(self.email_index.find("test@example.com"), [1])
        self.assertEqual(self.email_index.find("another@example.com"), [2])

    def test_add_same_id_same_key_non_unique(self):
        """Test adding the same record ID multiple times for the same key (should only store once)."""
        self.email_index.add("test@example.com", 1)
        self.email_index.add("test@example.com", 1) # Add again
        self.assertEqual(self.email_index.find("test@example.com"), [1])

    # --- Add & Find (Unique) Tests ---
    def test_add_and_find_single_unique(self):
        """Test adding and finding a single record ID in a unique index."""
        self.username_index.add("alice", 1)
        self.assertEqual(self.username_index.find("alice"), [1])
        self.assertEqual(self.username_index.find("bob"), [])

    def test_add_duplicate_key_unique_fails(self):
        """Test that adding a duplicate key to a unique index raises ValueError."""
        self.username_index.add("alice", 1)
        with self.assertRaisesRegex(ValueError, "Unique constraint violation.*alice"):
            self.username_index.add("alice", 2) # Different ID, same key
        # Ensure original entry is still there
        self.assertEqual(self.username_index.find("alice"), [1])

    def test_add_different_keys_unique(self):
        """Test adding different keys to a unique index."""
        self.username_index.add("alice", 1)
        self.username_index.add("bob", 2)
        self.assertEqual(self.username_index.find("alice"), [1])
        self.assertEqual(self.username_index.find("bob"), [2])

    # --- Remove Tests ---
    def test_remove_single_id_from_list(self):
        """Test removing one ID when multiple exist for a key."""
        self.email_index.add("test@example.com", 1)
        self.email_index.add("test@example.com", 5)
        self.email_index.add("test@example.com", 3)
        self.email_index.remove("test@example.com", 5)
        self.assertCountEqual(self.email_index.find("test@example.com"), [1, 3])
        self.assertTrue("test@example.com" in self.email_index.index_data) # Key should still exist

    def test_remove_last_id_for_key(self):
        """Test removing the last ID for a key, which should remove the key."""
        self.email_index.add("test@example.com", 1)
        self.email_index.remove("test@example.com", 1)
        self.assertEqual(self.email_index.find("test@example.com"), [])
        self.assertNotIn("test@example.com", self.email_index.index_data) # Key should be gone

    def test_remove_non_existent_id(self):
        """Test removing a record ID that doesn't exist for a key (should not error)."""
        self.email_index.add("test@example.com", 1)
        self.email_index.remove("test@example.com", 99) # ID 99 never added
        self.assertEqual(self.email_index.find("test@example.com"), [1]) # Original remains

    def test_remove_non_existent_key(self):
        """Test removing from a key that doesn't exist (should not error)."""
        self.email_index.remove("nonexistent@example.com", 1)
        self.assertEqual(len(self.email_index.index_data), 0)

    # --- Update Tests (Non-Unique) ---
    def test_update_key_non_unique(self):
        """Test updating a key in a non-unique index."""
        self.email_index.add("old@example.com", 1)
        self.email_index.update("old@example.com", "new@example.com", 1)
        self.assertEqual(self.email_index.find("old@example.com"), [])
        self.assertEqual(self.email_index.find("new@example.com"), [1])

    def test_update_key_with_multiple_ids_non_unique(self):
        """Test updating one record's key when others share the old key."""
        self.email_index.add("shared@example.com", 1)
        self.email_index.add("shared@example.com", 2)
        self.email_index.update("shared@example.com", "new@example.com", 1) # Update only record 1
        self.assertEqual(self.email_index.find("shared@example.com"), [2]) # Record 2 remains
        self.assertEqual(self.email_index.find("new@example.com"), [1]) # Record 1 moved

    def test_update_to_existing_key_non_unique(self):
        """Test updating a record's key to a key that already exists."""
        self.email_index.add("a@example.com", 1)
        self.email_index.add("b@example.com", 2)
        self.email_index.update("a@example.com", "b@example.com", 1) # Move record 1 to key 'b'
        self.assertEqual(self.email_index.find("a@example.com"), [])
        self.assertCountEqual(self.email_index.find("b@example.com"), [2, 1]) # Both now under 'b'

    def test_update_key_to_itself_non_unique(self):
        """Test updating a key to the same value (should be a no-op)."""
        self.email_index.add("test@example.com", 1)
        self.email_index.update("test@example.com", "test@example.com", 1)
        self.assertEqual(self.email_index.find("test@example.com"), [1])

    # --- Update Tests (Unique) ---
    def test_update_key_unique_success(self):
        """Test successfully updating a key in a unique index."""
        self.username_index.add("alice", 1)
        self.username_index.update("alice", "alice_new", 1)
        self.assertEqual(self.username_index.find("alice"), [])
        self.assertEqual(self.username_index.find("alice_new"), [1])

    def test_update_key_unique_violation_fails(self):
        """Test updating to a key that already exists in a unique index fails."""
        self.username_index.add("alice", 1)
        self.username_index.add("bob", 2)
        with self.assertRaisesRegex(ValueError, "Unique constraint violation.*bob"):
            self.username_index.update("alice", "bob", 1) # Try to change alice's username to bob
        # Ensure state is unchanged
        self.assertEqual(self.username_index.find("alice"), [1])
        self.assertEqual(self.username_index.find("bob"), [2])

    def test_update_key_to_itself_unique(self):
        """Test updating a key to the same value in a unique index."""
        self.username_index.add("alice", 1)
        self.username_index.update("alice", "alice", 1)
        self.assertEqual(self.username_index.find("alice"), [1])

    # --- Other Methods Tests ---
    def test_len(self):
        """Test the __len__ method."""
        self.assertEqual(len(self.email_index), 0)
        self.email_index.add("a@a.com", 1)
        self.assertEqual(len(self.email_index), 1)
        self.email_index.add("b@b.com", 2)
        self.assertEqual(len(self.email_index), 2)
        self.email_index.add("a@a.com", 3) # Same key, length doesn't change
        self.assertEqual(len(self.email_index), 2)
        self.email_index.remove("a@a.com", 1) # Still one ID for key 'a'
        self.assertEqual(len(self.email_index), 2)
        self.email_index.remove("a@a.com", 3) # Last ID for key 'a', key removed
        self.assertEqual(len(self.email_index), 1)

    def test_str_repr(self):
        """Test the __str__ and __repr__ methods."""
        self.assertEqual(str(self.email_index), "Index(name='idx_email', column='email', unique=False, keys=0)")
        self.assertEqual(repr(self.email_index), "Index(name='idx_email', column='email', unique=False, keys=0)")
        self.email_index.add("a@a.com", 1)
        self.email_index.add("b@b.com", 2)
        self.assertEqual(str(self.email_index), "Index(name='idx_email', column='email', unique=False, keys=2)")
        self.assertEqual(repr(self.email_index), "Index(name='idx_email', column='email', unique=False, keys=2)")

    def test_to_dict_definition(self):
        """Test the to_dict_definition method."""
        expected_email = {"name": "idx_email", "column": "email", "unique": False}
        self.assertEqual(self.email_index.to_dict_definition(), expected_email)

        expected_username = {"name": "idx_username", "column": "username", "unique": True}
        self.assertEqual(self.username_index.to_dict_definition(), expected_username)

    def test_clear(self):
        """Test the clear method."""
        self.email_index.add("a@a.com", 1)
        self.email_index.add("b@b.com", 2)
        self.assertEqual(len(self.email_index), 2)
        self.email_index.clear()
        self.assertEqual(len(self.email_index), 0)
        self.assertEqual(self.email_index.index_data, {})
        self.assertEqual(self.email_index.find("a@a.com"), [])

    def test_get_all_keys(self):
        """Test the get_all_keys method."""
        self.assertEqual(self.email_index.get_all_keys(), [])
        self.email_index.add("a@a.com", 1)
        self.email_index.add("b@b.com", 2)
        self.email_index.add("c@c.com", 3)
        self.email_index.add("a@a.com", 4)
        self.assertCountEqual(self.email_index.get_all_keys(), ["a@a.com", "b@b.com", "c@c.com"])
        self.email_index.remove("b@b.com", 2)
        self.assertCountEqual(self.email_index.get_all_keys(), ["a@a.com", "c@c.com"])

    def test_find_returns_copy(self):
        """Test that find returns a copy, not a reference."""
        self.email_index.add("a@a.com", 1)
        self.email_index.add("a@a.com", 2)

        found_list = self.email_index.find("a@a.com")
        self.assertCountEqual(found_list, [1, 2])

        # Modify the returned list
        found_list.append(99)

        # Check the original index data is unchanged
        self.assertCountEqual(self.email_index.index_data["a@a.com"], [1, 2])
        # Check that find still returns the original data
        self.assertCountEqual(self.email_index.find("a@a.com"), [1, 2])


if __name__ == '__main__':
    unittest.main(verbosity=0) # Use verbosity=0 for cleaner output with setUpClass/tearDownClass messages