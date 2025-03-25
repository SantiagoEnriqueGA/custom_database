import unittest
import os
import sys
import warnings
warnings.filterwarnings("ignore")

# Change the working directory to the parent directory to allow importing the package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def reorder_tests(tests):
    """ Reorder tests to run import tests first, then normal tests, and finally example file tests."""
    perf_tests = []
    example_tests = []
    normal_tests = []
    for test in tests:
        if 'performance' in str(test).lower():
            perf_tests.append(test)
        elif 'example' in str(test).lower():
            example_tests.append(test)
        else:
            normal_tests.append(test)
    return normal_tests + example_tests + perf_tests

if __name__ == '__main__':
    # Discover and run all tests in the 'tests' directory
    loader = unittest.TestLoader()
    suite = loader.discover('tests', pattern='test_*.py')
    
    # Reorder tests
    ordered_tests = reorder_tests(list(suite))
    ordered_suite = unittest.TestSuite(ordered_tests)
    
    testRunner = unittest.TextTestRunner()
    testRunner.run(ordered_suite)   