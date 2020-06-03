import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from neo4j import GraphDatabase


class ContentBasedRecommenderSecondApproach(object):

    # Run this before:
    # MATCH(feature)
    # WHERE "Genre" in labels(feature) OR "Director" in labels(feature)
    # SET feature: Feature

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)

    def recommendTo(self, userId, k):
        user_VSM = self.get_user_vector(userId)
        movies_VSM = self.get_movie_vectors(userId)
        top_k = self.compute_top_k (user_VSM, movies_VSM, k);
        return top_k

    def compute_top_k(self, user, movies, k):
        dtype = [ ('movieId', 'U10'),('value', 'f4')]
        knn_values = np.array([], dtype=dtype)
        for other_movie in movies:
            value = cosine_similarity([user], [movies[other_movie]])
            if value > 0:
                knn_values = np.concatenate((knn_values, np.array([(other_movie, value)], dtype=dtype)))
        knn_values = np.sort(knn_values, kind='mergesort', order='value' )[::-1]
        return np.array_split(knn_values, [k])[0]

    def get_user_vector(self, user_id):
        query = """
                MATCH p=(user:User)-[:WATCHED|RATES]->(movie)
                WHERE user.userId = $userId
                with count(p) as total
                MATCH (feature:Feature)
                WITH feature, total
                ORDER BY id(feature)
                MATCH (user:User)
                WHERE user.userId = $userId
                OPTIONAL MATCH (user)-[r:INTERESTED_IN]-(feature)
                WITH CASE WHEN r IS null THEN 0 ELSE (r.weight*1.0f)/(total*1.0f) END as value
                RETURN collect(value) as vector
            """
        user_VSM = None
        with self._driver.session() as session:
            tx = session.begin_transaction()
            vector = tx.run(query, {"userId": user_id})
            user_VSM = vector.single()[0]
        print(len(user_VSM))
        return user_VSM

    def get_movie_vectors(self, user_id):
        list_of_moview_query = """
                MATCH (movie:Movie)-[r:DIRECTED|HAS_GENRE]-(feature)<-[i:INTERESTED_IN]-(user:User {userId: $userId})
                WHERE NOT EXISTS((user)-[]->(movie)) AND EXISTS((user)-[]->(feature))
                WITH movie, count(i) as featuresCount
                WHERE featuresCount > 5
                RETURN movie.movieId as movieId
            """

        query = """
                MATCH (feature:Feature)
                WITH feature
                ORDER BY id(feature)
                MATCH (movie:Movie)
                WHERE movie.movieId = $movieId
                OPTIONAL MATCH (movie)-[r:DIRECTED|HAS_GENRE]-(feature)
                WITH CASE WHEN r IS null THEN 0 ELSE 1 END as value
                RETURN collect(value) as vector;
            """
        movies_VSM = {}
        with self._driver.session() as session:
            tx = session.begin_transaction()

            i = 0
            for movie in tx.run(list_of_moview_query, {"userId": user_id}):
                movie_id = movie["movieId"];
                vector = tx.run(query, {"movieId": movie_id})
                movies_VSM[movie_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "lines processed")
            print(i, "lines processed")
        print(len(movies_VSM))
        return movies_VSM


if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    recommender = ContentBasedRecommenderSecondApproach(uri=uri, user="neo4j", password="pippo1")
    top10 = recommender.recommendTo("598", 10); #Replace 598 with any other user id you are interested in
    # would be nice to enrich this information with at least titles
    print(top10)

