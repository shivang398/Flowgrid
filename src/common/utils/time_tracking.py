import time
from contextlib import contextmanager
from typing import Generator

def current_timestamp() -> float:
    """Returns the current time in seconds since the Epoch."""
    return time.time()

@contextmanager
def timer_context() -> Generator[dict, None, None]:
    """
    Context manager to track execution time.
    Yields a dictionary that gets populated with `elapsed_time` upon exit.
    
    Example:
        with timer_context() as stats:
            do_something()
        print(stats['elapsed_time'])
    """
    stats = {}
    start_time = time.perf_counter()
    try:
        yield stats
    finally:
        end_time = time.perf_counter()
        stats["elapsed_time"] = end_time - start_time
