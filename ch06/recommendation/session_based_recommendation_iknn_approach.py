import numpy as np
from neo4j import GraphDatabase

from util.sparse_vector import cosine_similarity


class SessionBasedRecommender(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)

    def close(self):
        self._driver.close()

    def compute_and_store_similarity(self):
        items_VSM = self.get_item_vectors()
        for item in items_VSM:
            knn = self.compute_knn(item, items_VSM.copy(), 20)
            self.store_knn(item, knn)

    def compute_knn(self, item, items, k):
        dtype = [ ('itemId', 'U10'),('value', 'f4')]
        knn_values = np.array([], dtype=dtype)
        #https: // github.com / brandonrobertz / SparseLSH
        for other_item in items:
            if other_item != item:
                value = cosine_similarity(items[item], items[other_item])
                if value > 0:
                    knn_values = np.concatenate((knn_values, np.array([(other_item, value)], dtype=dtype)))
        knn_values = np.sort(knn_values, kind='mergesort', order='value' )[::-1]
        return np.split(knn_values, [k])[0]

    def get_item_vectors(self):
        list_of_items_query = """
                    MATCH (item:Item)
                    RETURN item.itemId as itemId
                """

        query = """
                    MATCH (item:Item)<-[:IS_RELATED_TO]-(click:Click)<-[:CONTAINS]-(session:Session)
                    WHERE item.itemId = $itemId
                    WITH session 
                    ORDER BY id(session)
                    RETURN collect(distinct id(session)) as vector;
                """
        items_VSM_sparse = {}
        with self._driver.session() as session:
            i = 0
            for item in session.run(list_of_items_query):
                item_id = item["itemId"]
                vector = session.run(query, {"itemId": item_id})
                items_VSM_sparse[item_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "rows processed")
            print(i, "rows processed")
        print(len(items_VSM_sparse))
        return items_VSM_sparse

    def store_knn(self, item, knn):
        with self._driver.session() as session:
            tx = session.begin_transaction()
            knnMap = {a : b.item() for a,b in knn}
            clean_query = """
                MATCH (item:Item)-[s:SIMILAR_TO]->()
                WHERE item.itemId = $itemId
                DELETE s
            """
            query = """
                MATCH (item:Item)
                WHERE item.itemId = $itemId
                UNWIND keys($knn) as otherItemId
                MATCH (other:Item)
                WHERE other.itemId = toInteger(otherItemId)
                MERGE (item)-[:SIMILAR_TO {weight: $knn[otherItemId]}]->(other)
            """
            tx.run(clean_query, {"itemId": item})
            tx.run(query, {"itemId": item, "knn": knnMap})
            tx.commit()

    def recommend_to(self, item_id, k):
        top_items = []
        query = """
            MATCH (i:Item)-[r:SIMILAR_TO]->(oi:Item)
            WHERE i.itemId = $itemId
            RETURN oi.itemId as itemId, r.weight as score
            ORDER BY score desc
            LIMIT %s
        """
        with self._driver.session() as session:
            tx = session.begin_transaction()
            for result in tx.run(query % (k), {"itemId": item_id}):
                top_items.append((result["itemId"], result["score"]))

        top_items.sort(key=lambda x: -x[1])
        return top_items

if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "q1" # pippo1
    recommender = SessionBasedRecommender(uri=uri, user=user, password=password)
    recommender.compute_and_store_similarity()
    top10 = recommender.recommend_to(214842060, 10)
    recommender.close()
    print(top10)
