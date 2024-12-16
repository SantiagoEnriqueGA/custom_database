
import unittest
import glob
import os
import sys
import importlib.util
import contextlib
import io

from test_utils import suppress_print

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from examples.example_millionRowLoad import main
class TestExampleMillionRowLoad(unittest.TestCase):
    """
    Test case for the example file 'example_millionRowLoad.py'.
    """
    def test_main(self, example_file):
        print(f"Testing file: {example_file}")
        with suppress_print():
            main()

class TestExamples(unittest.TestCase):
    """
    Test cases for the example files.  
    Holds dynamically generated test cases for each example file.
    """
    pass

def load_tests(loader, tests, pattern):
    """
    Dynamically load test cases for each example file.
    args:
        loader: The test loader instance.
        tests: The test cases to load.
        pattern: The pattern to match test files.
    """
    # Find all example files in the examples directory. (Files starting with 'example_')
    example_files = glob.glob(os.path.join(os.path.dirname(__file__), '..\\examples\\example_*.py'))
    
    # Raise an error if no example files are found.
    if not example_files:
        raise FileNotFoundError("No example files found.")
    
    # Dynamically generate test cases for each example file.
    for example_file in example_files:
        test_name = f'test_{os.path.basename(example_file)}'
        
        def test_func(self, example_file=example_file):
            """Tests the functionality of a given example file by importing it as a module and executing it."""
            print(f"Testing file: {example_file}")
            
            # Import the example file as a module and execute it.
            spec = importlib.util.spec_from_file_location("module.name", example_file)
            example_module = importlib.util.module_from_spec(spec)
            
            # Redirect stdout to suppress output from the example file.
            with contextlib.redirect_stdout(io.StringIO()):
                spec.loader.exec_module(example_module)
        
        # Dynamically add the test function to the TestExamples class.
        if test_name == 'test_example_millionRowLoad.py':
            setattr(TestExamples, test_name, lambda self, example_file=example_file: TestExampleMillionRowLoad().test_main(example_file))
        else:
            setattr(TestExamples, test_name, test_func)
    
    # Load the dynamically generated test cases.
    return loader.loadTestsFromTestCase(TestExamples)

if __name__ == '__main__':
    unittest.main()