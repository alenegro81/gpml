import sys
from util.sparse_vector import cosine_similarity
from util.graphdb_base import GraphDBBase


class ContextAwareRecommender(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)

    def compute_and_store_similarity(self, contexts):
        for context in contexts:
            items_VSM = self.get_item_vectors(context)
            for item in items_VSM:
                knn = self.compute_knn(item, items_VSM.copy(), 20)
                self.store_knn(item, knn, context)

    def compute_knn(self, item, items, k):
        knn_values = []
        for other_item in items:
            if other_item != item:
                value = cosine_similarity(items[item], items[other_item])
                if value > 0:
                    knn_values.append((other_item, value))
        knn_values.sort(key=lambda x: -x[1])
        return knn_values[:k]

    def get_item_vectors(self, context):
        list_of_items_query = """
                    MATCH (item:Item)
                    RETURN item.itemId as itemId
                """
        context_info = context[1].copy()
        match_query = """
                    MATCH (event:Event)-[:EVENT_ITEM]->(item:Item)
                    MATCH (event)-[:EVENT_USER]->(user:User)
                """
        where_query = """
                    WHERE item.itemId = $itemId
                """

        if "location" in context_info:
            match_query += "MATCH (event)-[:EVENT_LOCATION]->(location:Location) "
            where_query += "AND location.value = $location "

        if "time" in context_info:
            match_query += "MATCH (event)-[:EVENT_TIME]->(time:Time) "
            where_query += "AND time.value = $time "

        if "companion" in context_info:
            match_query += "MATCH (event)-[:EVENT_COMPANION]->(companion:Companion) "
            where_query += "AND companion.value = $companion "

        return_query = """
                    WITH user.userId as userId, event.rating as rating
                    ORDER BY id(user)
                    RETURN collect(distinct userId) as vector 
                """

        query = match_query + where_query + return_query
        items_VSM_sparse = {}
        with self._driver.session() as session:
            i = 0
            for item in session.run(list_of_items_query):
                item_id = item["itemId"]
                context_info["itemId"] = item_id
                vector = session.run(query, context_info)
                items_VSM_sparse[item_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "rows processed")
            print(i, "lines processed")
        print(len(items_VSM_sparse))
        return items_VSM_sparse

    def store_knn(self, item, knn, context):
        context_id = context[0]
        params = context[1].copy()
        with self._driver.session() as session:
            tx = session.begin_transaction()
            knnMap = {a: b for a, b in knn}
            clean_query = """
                MATCH (s:Similarity)-[:RELATED_TO_SOURCE_ITEM]->(item:Item)
                WHERE item.itemId = $itemId AND s.contextId = $contextId
                DETACH DELETE s
            """

            query = """
                MATCH (item:Item)
                WHERE item.itemId = $itemId
                UNWIND keys($knn) as otherItemId
                MATCH (other:Item)
                WHERE other.itemId = otherItemId
                CREATE (similarity:Similarity {weight: $knn[otherItemId], contextId: $contextId})
                MERGE (item)<-[:RELATED_TO_SOURCE_ITEM]-(similarity)
                MERGE (other)<-[:RELATED_TO_DEST_ITEM ]-(similarity)
            """

            if "location" in params:
                query += "WITH similarity MATCH (location:Location {value: $location}) "
                query += "MERGE (location)<-[:RELATED_TO]-(similarity) "

            if "time" in params:
                query += "WITH similarity MATCH (time:Time {value: $time}) "
                query += "MERGE (time)<-[:RELATED_TO]-(similarity) "

            if "companion" in params:
                query += "WITH similarity MATCH (companion:Companion {value: $companion}) "
                query += "MERGE (companion)<-[:RELATED_TO]-(similarity) "

            tx.run(clean_query, {"itemId": item, "contextId": context_id})
            params["itemId"] = item
            params["contextId"] = context_id
            params["knn"] = knnMap
            tx.run(query, params)
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
    recommender = ContextAwareRecommender(sys.argv[1:])
    contexts = [(1, {"location": "Home", "companion": "Alone", "time": "Weekday"}),
                (2, {"location": "Cinema", "companion": "Partner", "time": "Weekend"}),
                (3, {"location": "Cinema", "companion": "Partner"})]
    recommender.compute_and_store_similarity(contexts)
    top10 = recommender.recommend_to(214842060, 10)
    recommender.close()
    print(top10)
