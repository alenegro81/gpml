from collaborative_filtering.sparse import Vector
from nose.tools import assert_raises


def test_mismatching_cardinality():
    """it should not allow vector with mismatching indexes and values"""
    with assert_raises(ValueError):
        v = Vector([0, 1], [0.6])


def test_creation():
    """it should create a sparse vector form its definition"""
    v = Vector([12, 42], [0.5, 0.1])


def test_creation_from_dictionary():
    """it should convert a dictionary of component:values into a vector"""
    v = Vector.from_dict({12: 0.5, 42: 0.1})
    assert v.cardinality == 2
    assert set(v.indexes) == {12, 42}
    assert set(v.values) == {0.5, 0.1}


def test_dot():
    """it should compute the dot vector correctly"""
    v0 = Vector([1, 2, 3], [0.1, 0.2, 0.3])
    vo = Vector([4, 5, 6], [0.4, 0.5, 0.6])
    vx = Vector([2, 3, 4], [1.2, 1.3, 1.4])
    assert v0.dot(vo) == 0
    assert v0.dot(vx) == 0.2 * 1.2 + 0.3 * 1.3
    assert v0.dot(v0) == 0.1 * 0.1 + 0.2 * 0.2 + 0.3 * 0.3


def test_norm():
    """it should compute norm for a vector"""
    v0 = Vector([1, 2, 3], [0.1, 0.2, 0.3])
    assert v0.norm() ** 2 == v0.dot(v0)


def test_cosine_similarity_does_not_overshoot():
    """it should compute the self cosine similarity being exactly one"""
    v0 = Vector([1, 2, 3], [0.1, 0.2, 0.3])
    assert Vector.cosine_similarity(v0, v0) == 1.0
