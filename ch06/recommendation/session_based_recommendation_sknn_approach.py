import numpy as np
import sys

from util.sparse_vector import cosine_similarity

from util.graphdb_base import GraphDBBase

class SessionBasedRecommender(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)

    def close(self):
        self.close()

    def compute_and_store_similarity(self):
        sessions_VSM = self._driver.session_vectors()
        for session in sessions_VSM:
            knn = self.compute_knn(session, sessions_VSM.copy(), 20)
            self.store_knn(session, knn)

    def compute_knn(self, session, sessions, k):
        dtype = [ ('itemId', 'U10'),('value', 'f4')]
        knn_values = np.array([], dtype=dtype)
        for other_session in sessions:
            if other_session != session:
                value = cosine_similarity(sessions[session], sessions[other_session])
                if value > 0:
                    knn_values = np.concatenate((knn_values, np.array([(other_session, value)], dtype=dtype)))
        knn_values = np.sort(knn_values, kind='mergesort', order='value' )[::-1]
        return np.split(knn_values, [k])[0]

    def get_session_vectors(self):
        list_of_sessions_query = """
                    MATCH (session:Session)
                    RETURN session.sessionId as sessionId
                    LIMIT 2000
                """

        query = """
                    MATCH (item:Item)<-[:IS_RELATED_TO]-(click:Click)<-[:CONTAINS]-(session:Session)
                    WHERE session.sessionId = $sessionId
                    WITH item 
                    ORDER BY id(item)
                    RETURN collect(distinct id(item)) as vector;
                """
        sessions_VSM_sparse = {}
        with self._driver.session() as session:
            i = 0
            for result in session.run(list_of_sessions_query):
                session_id = result["sessionId"]
                vector = session.run(query, {"sessionId": session_id})
                sessions_VSM_sparse[session_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "rows processed")

            print(i, "rows processed")
        print(len(sessions_VSM_sparse))
        return sessions_VSM_sparse

    def store_knn(self, session_id, knn):
        with self._driver.session() as session:
            tx = session.begin_transaction()
            knnMap = {str(a) : b.item() for a,b in knn}
            clean_query = """
                MATCH (session:Session)-[s:SIMILAR_TO]->()
                WHERE session.sessionId = $sessionId
                DELETE s
            """
            query = """
                MATCH (session:Session)
                WHERE session.sessionId = $sessionId
                UNWIND keys($knn) as otherSessionId
                MATCH (other:Session)
                WHERE other.sessionId = toInteger(otherSessionId)
                MERGE (session)-[:SIMILAR_TO {weight: $knn[otherSessionId]}]->(other)
            """
            tx.run(clean_query, {"sessionId": session_id})
            tx.run(query, {"sessionId": session_id, "knn": knnMap})
            tx.commit()

    def recommend_to(self, session_id, k):
        top_items = []
        query = """
            MATCH (target:Session)-[r:SIMILAR_TO]->(d:Session)-[:CONTAINS]->(:Click)-[:IS_RELATED_TO]->(item:Item) 
            WHERE target.sessionId = $sessionId
            WITH DISTINCT item.itemId as itemId, r
            RETURN itemId, sum(r.weight) as score
            ORDER BY score desc
            LIMIT %s
        """
        with self._driver.session() as session:
            tx = session.begin_transaction()
            for result in tx.run(query % (k), {"sessionId": session_id}):
                top_items.append((result["itemId"], result["score"]))

        top_items.sort(key=lambda x: -x[1])
        return top_items

if __name__ == '__main__':
    recommender = SessionBasedRecommender(sys.argv[1:])
    recommender.compute_and_store_similarity()
    top10 = recommender.recommend_to(907, 10)
    recommender.close()
    print(top10)
