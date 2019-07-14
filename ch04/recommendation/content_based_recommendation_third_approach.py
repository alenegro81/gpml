import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from neo4j.v1 import GraphDatabase


class ContentBasedRecommender(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def compute_and_store_similarity(self):
        movies_VSM = self.get_movie_vectors()
        for movie in movies_VSM:
            knn = self.compute_knn(movie, movies_VSM.copy(), 20);
            self.store_knn(movie, knn)

    def compute_knn(self, movie, movies, k):
        dtype = [ ('movieId', 'U10'),('value', 'f4')]
        knn_values = np.array([], dtype=dtype)
        for other_movie in movies:
            if other_movie != movie:
                value = cosine_similarity([movies[movie]], [movies[other_movie]])
                if value > 0:
                    knn_values = np.concatenate((knn_values, np.array([(other_movie, value)], dtype=dtype)))
        knn_values = np.sort(knn_values, kind='mergesort', order='value' )[::-1]
        return np.split(knn_values, [k])[0]

    def get_movie_vectors(self):
        list_of_moview_query = """
                    MATCH (movie:Movie)
                    RETURN movie.movieId as movieId
                """

        query = """
                    MATCH (feature:Feature)
                    WITH feature
                    ORDER BY id(feature)
                    MATCH (movie:Movie)
                    WHERE movie.movieId = {movieId}
                    OPTIONAL MATCH (movie)-[r:DIRECTED|:HAS_GENRE]-(feature)
                    WITH CASE WHEN r IS null THEN 0 ELSE 1 END as value
                    RETURN collect(value) as vector;
                """
        movies_VSM = {}
        with self._driver.session() as session:
            tx = session.begin_transaction()

            i = 0
            for movie in tx.run(list_of_moview_query):
                movie_id = movie["movieId"];
                vector = tx.run(query, {"movieId": movie_id})
                movies_VSM[movie_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "lines processed")
            print(i, "lines processed")
        print(len(movies_VSM))
        return movies_VSM;

    def store_knn(self, movie, knn):
        with self._driver.session() as session:
            tx = session.begin_transaction()
            test = {a : b.item() for a,b in knn}
            clean_query = """MATCH (movie:Movie)-[s:SIMILAR_TO]-()
                WHERE movie.movieId = {movieId}
                DELETE s
            """
            query = """
                MATCH (movie:Movie)
                WHERE movie.movieId = {movieId}
                UNWIND keys({knn}) as otherMovieId
                MATCH (other:Movie)
                WHERE other.movieId = otherMovieId
                MERGE (movie)-[:SIMILAR_TO {weight: {knn}[otherMovieId]}]-(other)
            """
            tx.run(clean_query, {"movieId": movie})
            tx.run(query, {"movieId": movie, "knn": test})
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
    recommender = ContentBasedRecommender(uri=uri, user="neo4j", password="pippo1")
    recommender.compute_and_store_similarity();
    top10 = recommender.recommendTo("598", 10);
    print(top10)
