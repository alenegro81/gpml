import csv
import time
import threading
from queue import Queue
from urllib.error import HTTPError

from neo4j.v1 import GraphDatabase
from imdb import IMDb, IMDbDataAccessError


class ImportMovielensParallel(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._ia = IMDb(reraiseExceptions=True)
        self._movie_queue = Queue()
        self._writing_queue = Queue();
        self._print_lock = threading.Lock()

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
                session.run("CREATE CONSTRAINT ON (a:Person) ASSERT a.name IS UNIQUE")

                i = 0;
                j = 0;
                for k in range(50):
                    print("starting thread: ", k)
                    movie_info_thread = threading.Thread(target=self.get_movie_info)
                    movie_info_thread.daemon = True
                    movie_info_thread.start()

                writing_thread = threading.Thread(target=self.writing_movie_on_db)
                writing_thread.daemon = True
                writing_thread.start()

                for row in reader:
                    if row:
                        #check if it exist already
                        self._movie_queue.put(row)
                        i += 1
                        j += 1
                    if i == 1000:
                        print(j, "lines processed")
                        i = 0
                print(j, "lines processed")

                self._movie_queue.join()
                self._writing_queue.join()
                print("Done")

    def get_movie_info(self):
        while True:
            row = self._movie_queue.get()
            with self._print_lock:
                print("Getting info for row: ", row)
            movie_id = strip(row[0])
            imdb_id = strip(row[1])
            # get a movie
            retry = 0
            while retry < 10:
                try:
                    movie = self._ia.get_movie(imdb_id)
                    with self._print_lock:
                        print("Writing to the other queue: ", movie)
                    self._writing_queue.put([movie_id, movie])
                    break
                except :
                    with self._print_lock:
                        print("An error occurred")
                    retry = retry + 1
                    if retry == 10:
                        with self._print_lock:
                            print("Error while getting", row)
                    else:
                        with self._print_lock:
                            print("Failed...... ", retry)
                        time.sleep(10)
            self._movie_queue.task_done()

    def writing_movie_on_db(self):
        query = """
            MATCH (movie:Movie {movieId: {movieId}})
            SET movie.plot = {plot}
            FOREACH (director IN {directors} | MERGE (d:Person {name: director}) SET d:Director MERGE (d)-[:DIRECTED]->(movie))
            FOREACH (actor IN {actors} | MERGE (d:Person {name: actor}) SET d:Actor MERGE (d)-[:ACTS_IN]->(movie))
            FOREACH (producer IN {producers} | MERGE (d:Person {name: producer}) SET d:Producer MERGE (d)-[:PRODUCES]->(movie))
            FOREACH (writer IN {writers} | MERGE (d:Person {name: writer}) SET d:Writer MERGE (d)-[:WRITES]->(movie))
            FOREACH (genre IN {genres} | MERGE (g:Genre {genre: genre}) MERGE (movie)-[:HAS_GENRE]->(g))
        """
        while True:
            # print the names of the directors of the movie
            movie_id, movie_info = self._writing_queue.get()
            with self._print_lock:
                print("Writing movie", movie_id)
            with self._driver.session() as session:
                try:
                    directors = []
                    if 'directors' in  movie_info:
                        for director in movie_info['directors']:
                            if 'name' in director.data:
                                directors.append(director['name'])

                    # print the genres of the movie
                    genres = ''
                    if 'genres' in movie_info:
                        genres = movie_info['genres']

                    actors = []
                    if 'cast' in movie_info:
                        for actor in movie_info['cast']:
                            if 'name' in actor.data:
                                actors.append(actor['name'])

                    writers = []
                    if 'writers' in movie_info:
                        for writer in movie_info['writers']:
                            if 'name' in writer.data:
                                writers.append(writer['name'])

                    producers = []
                    if 'producers' in movie_info:
                        for producer in movie_info['producers']:
                            producers.append(producer['name'])

                    plot = ''
                    if 'plot outline' in movie_info:
                        plot = movie_info['plot outline']
                    session.run(query, {"movieId":movie_id, "directors": directors, "genres": genres, "actors": actors, "plot": plot, "writers": writers, "producers": producers})
                except Exception as e:
                    print(movie_id, e)


def strip(string): return''.join([c if 0 < ord(c) < 128 else ' ' for c in string]) #removes non utf-8 chars from string within cell


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    importing = ImportMovielensParallel(uri=uri, user="neo4j", password="pippo1")
    importing.import_movies(file="/Users/ale/neo4j-servers/gpml/dataset/ml-20m/movies.csv")
    importing.import_movie_details(file="/Users/ale/neo4j-servers/gpml/dataset/ml-20m/links.csv")
    end = time.time() - start
    print("Time to complete:", end)
