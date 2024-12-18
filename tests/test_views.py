
import unittest
from unittest.mock import Mock
import unittest
from unittest.mock import Mock
import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.views import View, MaterializedView
from test_utils import suppress_print

class TestViews(unittest.TestCase):
    """
    Unit tests for the View class.
    Methods:
    - test_view_initialization: Tests the initialization of a View instance.
    - test_view_get_data: Tests the get_data method of a View instance.
    """
    def test_view_initialization(self):
        query = Mock(return_value=[{"id": 1, "name": "Alice"}])
        view = View("TestView", query)
        self.assertEqual(view.name, "TestView")
        self.assertEqual(view.query, query)

    def test_view_get_data(self):
        query = Mock(return_value=[{"id": 1, "name": "Alice"}])
        view = View("TestView", query)
        data = view.get_data()
        self.assertEqual(data, [{"id": 1, "name": "Alice"}])
        query.assert_called_once()

class TestMaterializedView(TestViews):
    """
    Unit tests for the MaterializedView class.
    Methods:
    - test_materialized_view_initialization: Tests the initialization of a MaterializedView instance.
    - test_materialized_view_get_data: Tests the get_data method of a MaterializedView instance.
    - test_materialized_view_refresh: Tests the refresh method of a MaterializedView instance.
    """
    def test_materialized_view_initialization(self):
        query = Mock(return_value=[{"id": 1, "name": "Alice"}])
        mview = MaterializedView("TestMaterializedView", query)
        self.assertEqual(mview.name, "TestMaterializedView")
        self.assertEqual(mview.query, query)
        self.assertEqual(mview.data, [{"id": 1, "name": "Alice"}])

    def test_materialized_view_get_data(self):
        query = Mock(return_value=[{"id": 1, "name": "Alice"}])
        mview = MaterializedView("TestMaterializedView", query)
        data = mview.get_data()
        self.assertEqual(data, [{"id": 1, "name": "Alice"}])

    def test_materialized_view_refresh(self):
        query = Mock(return_value=[{"id": 1, "name": "Alice"}])
        mview = MaterializedView("TestMaterializedView", query)
        query.return_value = [{"id": 2, "name": "Bob"}]
        mview.refresh()
        self.assertEqual(mview.data, [{"id": 2, "name": "Bob"}])

if __name__ == '__main__':
    unittest.main()