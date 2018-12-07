import csv
import time
from neo4j.v1 import GraphDatabase
from imdb import IMDb

class import_movielens(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._ia = IMDb()

    def close(self):
        self._driver.close()

    def import_movies(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session() as session:
                session.run("CREATE CONSTRAINT ON (a:Movie) ASSERT a.movieId IS UNIQUE; ")
                session.run("CREATE CONSTRAINT ON (a:Genre) ASSERT a.genre IS UNIQUE; ")


                tx = session.begin_transaction()

                i = 0;
                j = 0;
                for row in reader:
                    try:
                        if row:
                            movie_id = strip(row[0])
                            title = strip(row[1])
                            genres = strip(row[2])
                            query = """
                                MERGE (movie:Movie {movieId: {movieId}, title: {title}})
                                with movie
                                UNWIND {genres} as genre
                                MERGE (g:Genre {genre: genre})
                                MERGE (movie)-[:HAS_GENRE]->(g)
                            """
                            tx.run(query, {"movieId":movie_id, "title": title, "genres":genres.split("|")})
                            i += 1
                            j += 1

                        if i == 1000: #submits a batch every 1000 lines read
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row, reader.line_num)
                tx.commit()
                print(j, "lines processed")


    def import_movie_details(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session() as session:
                session.run("CREATE CONSTRAINT ON (a:Person) ASSERT a.name IS UNIQUE;")
                tx = session.begin_transaction()
                i = 0;
                j = 0;
                for row in reader:
                    try:
                        if row:
                            movie_id = strip(row[0])
                            imdb_id = strip(row[1])
                            # get a movie
                            movie = self._ia.get_movie(imdb_id)
                            self.process_movie_info(movie_info=movie, tx=tx, movie_id=movie_id)
                            i += 1
                            j += 1

                        if i == 10: #submits a batch every 1000 lines read
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row, reader.line_num)
                tx.commit()
                print(j, "lines processed")


    def process_movie_info(self, tx, movie_info, movie_id):
        # print the names of the directors of the movie
        query = """
            MATCH (movie:Movie {movieId: {movieId}})
            SET movie.plot = {plot}
            FOREACH (director IN {directors} | MERGE (d:Person {name: director}) SET d:Director MERGE (d)-[:DIRECTED]->(movie))
            FOREACH (actor IN {actors} | MERGE (d:Person {name: actor}) SET d:Actor MERGE (d)-[:ACTS_IN]->(movie))
            FOREACH (producer IN {producers} | MERGE (d:Person {name: producer}) SET d:Producer MERGE (d)-[:PRODUCES]->(movie))
            FOREACH (writer IN {writers} | MERGE (d:Person {name: writer}) SET d:Writer MERGE (d)-[:WRITES]->(movie))
            FOREACH (genre IN {genres} | MERGE (g:Genre {genre: genre}) MERGE (movie)-[:HAS_GENRE]->(g))
        """
        directors = []
        for director in movie_info['directors']:
            if 'name' in director.data:
                directors.append(director['name'])

        # print the genres of the movie
        genres = ''
        if 'genres' in movie_info:
            genres = movie_info['genres']

        actors = []
        for actor in movie_info['cast']:
            if 'name' in actor.data:
                actors.append(actor['name'])

        writers = []
        for writer in movie_info['writers']:
            if 'name' in writer.data:
                writers.append(writer['name'])

        producers = []
        for producer in movie_info['producers']:
            producers.append(producer['name'])

        plot = ''
        if 'plot outline' in movie_info:
            plot = movie_info['plot outline']

        tx.run(query, {"movieId":movie_id, "directors": directors, "genres": genres, "actors": actors, "plot": plot, "writers": writers, "producers": producers})


def strip(string): return''.join([c if 0 < ord(c) < 128 else ' ' for c in string]) #removes non utf-8 chars from string within cell

if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    importing = import_movielens(uri=uri, user="neo4j", password="pippo1")
    #importing.import_movies(file="/Users/ale/neo4j-servers/gpml/dataset/ml-20m/movies.csv")
    importing.import_movie_details(file="/Users/ale/neo4j-servers/gpml/dataset/ml-20m/links.csv")
    end = time.time() - start
    print("Time to complete:", end)