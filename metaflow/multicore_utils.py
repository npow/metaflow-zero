"""Multiprocessing utilities for Metaflow."""

from multiprocessing import Pool


def parallel_map(func, iterable):
    """Apply func to each element of iterable using multiprocessing.

    Falls back to sequential map if multiprocessing fails (e.g., with lambdas).
    """
    items = list(iterable)
    if not items:
        return []
    try:
        with Pool() as pool:
            return pool.map(func, items)
    except Exception:
        return list(map(func, items))
