import sys
from enum import Enum
from typing import Dict, List

from neo4j import GraphDatabase, Transaction

from pathlib import Path

sys.path.append(Path(__file__).parent.parent)
from fixed_heapq import FixedHeap
from sparse import Vector as SparseVector


class BaseRecomander(object):
    label = None
    property = None
    relation = None
    sparse_vector_query = None
    score_query = None

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)

    def compute_and_store_KNN(self, size: int) -> None:
        print("fetching vectors")
        vectors = self.get_vectors()
        print(f"computing KNN for {len(vectors)} vectors")
        for i, (key, vector) in enumerate(vectors.items()):
            vector = SparseVector.from_dict(vector)
            knn = FixedHeap(size)
            for (other_key, other_vector) in vectors.items():
                if key != other_key:
                    other_vector = SparseVector.from_dict(other_vector)
                    score = SparseVector.cosine_similarity(vector, other_vector)
                    if score > 0:
                        knn.push(score, {"secondNode": other_key, "similarity": score})
            self.store_KNN(key, knn.items())
            if (i % 1000 == 0) and i > 0:
                print(f"{i} vectors processed...")
        print("KNN computation done")

    def get_vectors(self) -> Dict[int, Dict[int, float]]:
        with self._driver.session() as session:
            tx = session.begin_transaction()
            ids = self.get_elements(tx)
            vectors = {id_: self.get_sparse_vector(tx, id_) for id_ in ids}
        return vectors

    def get_elements(self, tx) -> List[str]:
        query = f"MATCH (u:{self.label}) RETURN u.{self.property} as id"
        result = tx.run(query).value()
        return result

    def get_sparse_vector(self, tx: Transaction, current_id: str) -> Dict[int, float]:
        params = {"id": current_id}
        result = tx.run(self.sparse_vector_query, params)
        return dict(result.values())

    def store_KNN(self, key: str, sims: List[Dict]) -> None:
        deleteQuery = f"""
            MATCH (n:{self.label})-[s:{self.relation}]->()
            WHERE n.{self.property} = $id
            DELETE s"""

        query = f"""
            MATCH (n:{self.label}) 
            WHERE n.{self.property} = $id 
            UNWIND $sims as sim
            MATCH (o:{self.label}) 
            WHERE o.{self.property} = sim.secondNode 
            CREATE (n)-[s:{self.relation} {{ value: toFloat(sim.similarity) }}]->(o)"""

        with self._driver.session() as session:
            tx = session.begin_transaction()
            params = {
                "id": key,
                "sims": sims}
            tx.run(deleteQuery, params)
            tx.run(query, params)
            tx.commit()

    def get_recommendations(self, user_id: str, size: int) -> List[str]:
        not_seen_yet_items = self.get_not_seen_yet_items(user_id)
        recommendations = FixedHeap(size)
        for item in not_seen_yet_items:
            score = self.get_score(user_id, item)
            recommendations.push(score, item)
        return recommendations.items()

    def get_not_seen_yet_items(self, user_id: str) -> List[str]:
        query = """
                MATCH (user:User {userId:$userId})
                WITH user
                MATCH (item:Item)
                WHERE NOT EXISTS((user)-[:PURCHASES]->(item))
                return item.itemId
        """
        with self._driver.session() as session:
            tx = session.begin_transaction()
            params = {"userId": user_id}
            result = tx.run(query, params).value()
        return result

    def get_score(self, user_id: str, item_id: str) -> float:
        with self._driver.session() as session:
            tx = session.begin_transaction()
            params = {"userId": user_id, "itemId": item_id}
            result = tx.run(self.score_query, params)
            result = result.value() + [0.0]
        return result[0]

    def clean_KNN(self):
        print("cleaning previus computed KNNs")
        delete_query = f"MATCH p=()-[r:{self.relation}]->() DELETE r"
        with self._driver.session() as session:
            tx = session.begin_transaction()
            tx.run(delete_query)
            tx.commit()


class UserRecomander(BaseRecomander):
    label = "User"
    property = "userId"
    relation = "USER_SIMILARITY"
    sparse_vector_query = """
        MATCH (u:User {userId: $id})-[:PURCHASES]->(i:Item)
        return id(i) as index, 1.0 as value
        order by index
    """
    score_query = """
        MATCH (user:User)-[:USER_SIMILARITY]->(otherUser:User)
        WHERE user.userId = $userId
        WITH otherUser, count(otherUser) as size
        MATCH (otherUser)-[r:PURCHASES]->(target:Item)
        WHERE target.itemId = $itemId
        return (1.0f/size)*count(r) as score
    """


class ItemRecomander(BaseRecomander):
    label = "Item"
    property = "itemId"
    relation = "ITEM_SIMILARITY"
    sparse_vector_query = """
        MATCH (u:User )-[:PURCHASES]->(i:Item {itemId: $id})
        return id(u) as index, 1.0 as value
        order by index
    """
    score_query = """
        MATCH (user:User)-[:PURCHASES]->(item:Item)-[r:ITEM_SIMILARITY]->(target:Item)
        WHERE user.userId = $userId AND target.itemId = $itemId
        return sum(r.value) as score
    """


class Recommender(object):
    class KNNType(Enum):
        USER = 1
        ITEM = 2

    def __init__(self, uri: str, user: str, password: str):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self.strategies: Dict[Recommender.KNNType, BaseRecomander] = {
            Recommender.KNNType.USER: UserRecomander(uri, user, password),
            Recommender.KNNType.ITEM: ItemRecomander(uri, user, password)
        }

    def compute_and_store_KNN(self, type_: KNNType) -> None:
        strategy = self.strategies[type_]
        strategy.compute_and_store_KNN(20)

    def clean_KNN(self, type_: KNNType):
        strategy = self.strategies[type_]
        strategy.clean_KNN()

    def get_recommendations(self, user_id: str, size: int, type_: KNNType):
        strategy = self.strategies[type_]
        return strategy.get_recommendations(user_id, size)


def main():
    recommender = Recommender("bolt://localhost:7687", "neo4j", "q1")

    recommender.clean_KNN(recommender.KNNType.USER)
    recommender.compute_and_store_KNN(recommender.KNNType.USER)
    user_id = "121688"
    print(f"User-based recommendations for user {user_id}")
    recommendations = recommender.get_recommendations(user_id, 10, recommender.KNNType.USER)
    print(recommendations)

    recommender.clean_KNN(recommender.KNNType.ITEM)
    recommender.compute_and_store_KNN(recommender.KNNType.ITEM)
    user_id = "121688"
    print(f"Item-based recommendations for user {user_id}")
    recommendations = recommender.get_recommendations(user_id, 10, recommender.KNNType.ITEM)
    print(recommendations)


if __name__ == '__main__':
    main()
