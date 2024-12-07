
import sys
import os
from contextlib import contextmanager

@contextmanager
def suppress_print():
    """
    A context manager that suppresses all print statements within its block.  
    This function redirects the standard output to os.devnull, silencing any print statements executed within its context.  
    Once the context is exited, the standard output is restored to its original state.  
    Usage:
        with suppress_print():
            # Any print statements here will be suppressed
    Yields:
        None
    """
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout