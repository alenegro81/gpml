from collaborative_filtering.fixed_heapq import FixedHeap
from random import random


def test_undercapacity():
    """it should return the sorted list of items if it fits the capacity"""
    x = FixedHeap(10)
    items = [random() for _ in range(8)]
    [x.push(item, item) for item in items]
    heap_items = x.items()
    expected_items = list(sorted(items, reverse=True))
    assert heap_items == expected_items, f'{heap_items} != {expected_items}'


def test_at_capacity():
    """it should return the sorted list of items if it fits exactly the capacity"""
    x = FixedHeap(10)
    items = [random() for _ in range(10)]
    [x.push(item, item) for item in items]
    heap_items = x.items()
    expected_items = list(sorted(items, reverse=True))
    assert heap_items == expected_items, f'{heap_items} != {expected_items}'


def test_overcapacity():
    """it should return a portion of the sorted list of items if it exceeds the capacity"""
    x = FixedHeap(10)
    items = [random() for _ in range(100)]
    [x.push(item, item) for item in items]
    heap_items = x.items()
    expected_items = list(sorted(items, reverse=True))[:10]
    assert heap_items == expected_items, f'{heap_items} != {expected_items}'


def test_stable_sorting():
    """it should preserve the insertion order on score collision (not try to compare item themselves)"""
    x = FixedHeap(10)
    items = [{"item": 1}, {"item": 2}]
    [x.push(0, item) for item in items]
    heap_items = x.items()
    expected_items = items
    assert heap_items == expected_items, f'{heap_items} != {expected_items}'