from enum import Enum
from typing import Dict, List

from neo4j import Transaction

from util.fixed_heapq import FixedHeap
from util.sparse_vector import cosine_similarity
from util.graphdb_base import GraphDBBase


class BaseRecommender(GraphDBBase):
    label = None
    property = None
    sparse_vector_query = None
    score_query = None

    def __init__(self):
        super().__init__(command=__file__)

    def compute_and_store_KNN(self, size: int) -> None:
        print("fetching vectors")
        vectors = self.get_vectors()
        print(f"computing KNN for {len(vectors)} vectors")
        for i, (key, vector) in enumerate(vectors.items()):
            # index only vectors
            vector = sorted(vector.keys())
            knn = FixedHeap(size)
            for (other_key, other_vector) in vectors.items():
                if key != other_key:
                    # index only vectors
                    other_vector = sorted(other_vector.keys())
                    score = cosine_similarity(vector, other_vector)
                    if score > 0:
                        knn.push(score, {"secondNode": other_key, "similarity": score})
            self.store_KNN(key, knn.items())
            if (i % 1000 == 0) and i > 0:
                print(f"{i} vectors processed...")
        print("KNN computation done")

    def get_vectors(self) -> Dict:
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
            MATCH (n:{self.label})-[s:SIMILARITY]->()
            WHERE n.{self.property} = $id
            DELETE s"""

        query = f"""
            MATCH (n:{self.label}) 
            WHERE n.{self.property} = $id 
            UNWIND $sims as sim
            MATCH (o:{self.label}) 
            WHERE o.{self.property} = sim.secondNode 
            CREATE (n)-[s:SIMILARITY {{ value: toFloat(sim.similarity) }}]->(o)"""

        with self._driver.session() as session:
            tx = session.begin_transaction()
            params = {
                "id": key,
                "sims": sims}
            tx.run(deleteQuery, params)
            tx.run(query, params)
            tx.commit()

    def get_recommendations(self, user_id: str, size: int) -> List[int]:
        not_seen_yet_items = self.get_not_seen_yet_items(user_id)
        recommendations = FixedHeap(size)
        for item in not_seen_yet_items:
            score = self.get_score(user_id, item)
            recommendations.push(score, item)
        return recommendations.items()

    def get_not_seen_yet_items(self, user_id: str) -> List[int]:
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


class UserRecommender(BaseRecommender):
    label = "User"
    property = "userId"
    sparse_vector_query = """
        MATCH (u:User {userId: $id})-[:PURCHASES]->(i:Item)
        return id(i) as index, 1.0 as value
        order by index
    """
    score_query = """
        MATCH (user:User)-[:SIMILARITY]->(otherUser:User)
        WHERE user.userId = $userId
        WITH otherUser, count(otherUser) as size
        MATCH (otherUser)-[r:PURCHASES]->(target:Target)
        WHERE target.itemId = $itemId
        return (1.0f/size)*count(r) as score
    """


class ItemRecommender(BaseRecommender):
    label = "User"
    property = "userId"
    sparse_vector_query = """
        MATCH (u:User )-[:PURCHASES]->(i:Item {itemId: $id})
        return id(u) as index, 1.0 as value
        order by index
    """
    score_query = """
        MATCH (user:User)-[:PURCHASES]->(item:Item)-[r:SIMILARITY]->(target:Item)
        WHERE user.userId = $userId AND target.itemId = $itemId
        return sum(r.value) as score
    """


class Recommender(GraphDBBase):
    class KNNType(Enum):
        USER = 1
        ITEM = 2

    def __init__(self):
        super().__init__(command=__file__)
        self.strategies: Dict[Recommender.KNNType, BaseRecommender] = {
            Recommender.KNNType.USER: UserRecommender(),
            Recommender.KNNType.ITEM: ItemRecommender()
        }

    def compute_and_store_KNN(self, type_: KNNType) -> None:
        strategy = self.strategies[type_]
        strategy.compute_and_store_KNN(20)

    def clean_KNN(self):
        print("cleaning previously computed KNNs")
        delete_query = "MATCH p=()-[r:SIMILARITY]->() DELETE r"
        with self._driver.session() as session:
            tx = session.begin_transaction()
            tx.run(delete_query)
            tx.commit()

    def get_recommendations(self, user_id: str, size: int, type_: KNNType):
        strategy = self.strategies[type_]
        return strategy.get_recommendations(user_id, size)


def main():
    # TODO: pass the user ID in the command-line
    recommender = Recommender()
    recommender.clean_KNN()
    recommender.compute_and_store_KNN(recommender.KNNType.USER)
    user_id = "121688"
    print(f"User-based recommendations for user {user_id}")
    recommendations = recommender.get_recommendations(user_id, 10, recommender.KNNType.USER)
    print(recommendations)
    recommender.clean_KNN()
    recommender.compute_and_store_KNN(recommender.KNNType.ITEM)
    user_id = "121688"
    print(f"Item-based recommendations for user {user_id}")
    recommendations = recommender.get_recommendations(user_id, 10, recommender.KNNType.ITEM)
    print(recommendations)


if __name__ == '__main__':
    main()
