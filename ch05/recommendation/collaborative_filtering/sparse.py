from math import sqrt
from typing import List, Dict


class Vector(object):
    def __init__(self, indexes: List[int], values: List[float]):
        if len(indexes) != len(values):
            raise ValueError(
                f"the index cardinality ({len(indexes)}) should match the number of values ({len(values)})")
        cardinality = len(indexes)
        self.cardinality = cardinality
        self.indexes = indexes
        self.values = values

    @classmethod
    def from_dict(cls, vector: Dict[int, float]):
        components = list(vector.items())
        components = sorted(components)
        indexes, values = zip(*components)
        return Vector(indexes, values)

    def to_dict(self):
        return dict(zip(self.indexes, self.values))

    def dot(self, other_vector: 'Vector'):
        if self.cardinality == 0 or other_vector.cardinality == 0 or other_vector is None:
            return 0.0
        common_components = set(self.indexes).intersection(other_vector.indexes)
        values = self.to_dict()
        other_values = other_vector.to_dict()
        return sum(values[i] * other_values[i] for i in common_components)

    def norm(self):
        return sqrt(self.dot(self))

    @staticmethod
    def cosine_similarity(x_vector: 'Vector', y_vector: 'Vector'):
        a = x_vector.dot(y_vector)
        b = x_vector.norm() * y_vector.norm()
        return a / b if b > 0 else 0
