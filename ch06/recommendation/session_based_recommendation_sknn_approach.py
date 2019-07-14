import numpy as np
from neo4j import GraphDatabase
from util.sparse_vector import cosine_similarity


class SessionBasedRecommender(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def compute_and_store_similarity(self):
        sessions_VSM = self.get_session_vectors()
        for session in sessions_VSM:
            knn = self.compute_knn(session, sessions_VSM.copy(), 20);
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
                    LIMIT 100
                """

        query = """
                    MATCH (item:Item)<-[:RELATED_TO]-(click:Click)<-[:CONTAINS]-(session:Session)
                    WHERE session.sessionId = {sessionId}
                    WITH item 
                    ORDER BY id(item)
                    RETURN collect(distinct id(item)) as vector;
                """
        sessions_VSM_sparse = {}
        with self._driver.session() as session:
            i = 0
            for result in session.run(list_of_sessions_query):
                session_id = result["sessionId"];
                vector = session.run(query, {"sessionId": session_id})
                sessions_VSM_sparse[session_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "rows processed")
                    break
            print(i, "lines processed")
        print(len(sessions_VSM_sparse))
        return sessions_VSM_sparse

    def store_knn(self, session_id, knn):
        with self._driver.session() as session:
            tx = session.begin_transaction()
            knnMap = {a : b.item() for a,b in knn}
            clean_query = """
                MATCH (session:Session)-[s:SIMILAR_TO]-()
                WHERE session.sessionId = {sessionId}
                DELETE s
            """
            query = """
                MATCH (session:Session)
                WHERE session.sessionId = {sessionId}
                UNWIND keys({knn}) as otherSessionId
                MATCH (other:Session)
                WHERE other.sessionId = toInt(otherSessionId)
                MERGE (session)-[:SIMILAR_TO {weight: {knn}[otherSessionId]}]-(other)
            """
            tx.run(clean_query, {"sessionId": session_id})
            tx.run(query, {"sessionId": session_id, "knn": knnMap})
            tx.commit()

    def recommendTo(self, user_id, k):
        dtype = [('movieId', 'U10'), ('value', 'f4')]
        top_movies = np.array([], dtype=dtype)
        query = """
            MATCH (user:User)
            WHERE user.userId = {userId}
            WITH user
            MATCH (targetMovie:Movie)
            WHERE NOT EXISTS((user)-[]->(targetMovie))
            WITH targetMovie, user
            MATCH (user:User)-[]->(movie:Movie)-[r:SIMILAR_TO]->(targetMovie)
            RETURN targetMovie.movieId as movieId, sum(r.weight)/count(r) as relevance
            order by relevance desc
            LIMIT %s
        """
        with self._driver.session() as session:
            tx = session.begin_transaction()
            for result in tx.run(query % (k), {"userId": user_id}):
                top_movies = np.concatenate((top_movies, np.array([(result["movieId"], result["relevance"])], dtype=dtype)))

        return top_movies

if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    recommender = SessionBasedRecommender(uri=uri, user="neo4j", password="pippo1")
    recommender.compute_and_store_similarity();
    top10 = recommender.recommendTo("598", 10);
    recommender.close()
    print(top10)
