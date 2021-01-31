import csv
import time
import threading
from queue import Queue
from imdb import IMDb
import sys
import os

from util.graphdb_base import GraphDBBase
from util.string_util import strip


class MoviesImporterParallel(GraphDBBase):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self._ia = IMDb(reraiseExceptions=True)
        self._movie_queue = Queue()
        self._writing_queue = Queue()
        self._print_lock = threading.Lock()

    def close(self):
        self._driver.close()

    def import_movies(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self.get_session() as session:
                self.execute_without_exception("CREATE CONSTRAINT ON (a:Movie) ASSERT a.movieId IS UNIQUE; ")
                self.execute_without_exception("CREATE CONSTRAINT ON (a:Genre) ASSERT a.genre IS UNIQUE; ")
                tx = session.begin_transaction()
                i = 0
                j = 0
                for row in reader:
                    try:
                        if row:
                            movie_id = strip(row[0])
                            title = strip(row[1])
                            genres = strip(row[2])
                            query = """
                                MERGE (movie:Movie {movieId: $movieId, title: $title})
                                with movie
                                UNWIND $genres as genre
                                MERGE (g:Genre {genre: genre})
                                MERGE (movie)-[:HAS]->(g)
                            """
                            tx.run(query, {"movieId": movie_id, "title": title, "genres": genres.split("|")})
                            i += 1
                            j += 1

                        if i == 1000:
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row, reader.line_num)
                tx.commit()
                print(j, "lines processed")

    def import_user_item(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self.get_session() as session:
                self.execute_without_exception("CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE")

                tx = session.begin_transaction()
                i = 0
                j = 0
                for row in reader:
                    try:
                        if row:
                            user_id = strip(row[0])
                            movie_id = strip(row[1])
                            rating = strip(row[2])
                            timestamp = strip(row[3])

                            query = """
                                MATCH (movie:Movie {movieId: $movieId})
                                MERGE (user:User {userId: $userId})
                                MERGE (user)-[:RATED {rating: $rating, timestamp: $timestamp}]->(movie)
                            """
                            tx.run(query,
                                   {"movieId": movie_id, "userId": user_id, "rating": rating, "timestamp": timestamp})
                            i += 1
                            j += 1

                        if i == 1000:
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
            with self.get_session() as session:
                self.execute_without_exception("CREATE CONSTRAINT ON (a:Person) ASSERT a.name IS UNIQUE")
                i = 0
                j = 0
                for k in range(50):
                    print("starting thread: ", k)
                    movie_info_thread = threading.Thread(target=self.get_movie_info)
                    movie_info_thread.daemon = True
                    movie_info_thread.start()

                writing_thread = threading.Thread(target=self.write_movie_on_db)
                writing_thread.daemon = True
                writing_thread.start()

                for row in reader:
                    if row:
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
                except:
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

    def write_movie_on_db(self):
        query = """
            MATCH (movie:Movie {movieId: $movieId})
            SET movie.plot = $plot
            FOREACH (director IN $directors | MERGE (d:Person {name: director}) SET d:Director MERGE (d)-[:DIRECTED]->(movie))
            FOREACH (actor IN $actors | MERGE (d:Person {name: actor}) SET d:Actor MERGE (d)-[:ACTS_IN]->(movie))
            FOREACH (producer IN $producers | MERGE (d:Person {name: producer}) SET d:Producer MERGE (d)-[:PRODUCED]->(movie))
            FOREACH (writer IN $writers | MERGE (d:Person {name: writer}) SET d:Writer MERGE (d)-[:WRITED]->(movie))
            FOREACH (genre IN $genres | MERGE (g:Genre {genre: genre}) MERGE (movie)-[:HAS]->(g))
        """
        while True:
            # print the names of the directors of the movie
            movie_id, movie_info = self._writing_queue.get()
            with self._print_lock:
                print("Writing movie", movie_id)
            with self._driver.session() as session:
                try:
                    directors = []
                    if 'directors' in movie_info:
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
                    session.run(query, {"movieId": movie_id, "directors": directors, "genres": genres, "actors": actors,
                                        "plot": plot, "writers": writers, "producers": producers})
                except Exception as e:
                    print(movie_id, e)


if __name__ == '__main__':
    importing = MoviesImporterParallel(argv=sys.argv[1:])
    base_path = importing.source_dataset_path

    if not base_path:
        print("source path directory is mandatory. Setting it to defaul.")
        base_path = "../../../dataset/movielens/ml-latest-small"

    if not os.path.isdir(base_path):
        print(base_path, "It isn't a directory")
        sys.exit(1)

    movies_path = os.path.join(base_path, "movies.csv")
    links_path = os.path.join(base_path, "links.csv")
    ratings_path = os.path.join(base_path, "ratings.csv")

    if not os.path.isfile(movies_path):
        print(movies_path, "doesn't exist in ", base_path)
        sys.exit(1)
    if not os.path.isfile(links_path):
        print(links_path, "doesn't exist in ", base_path)
        sys.exit(1)
    if not os.path.isfile(ratings_path):
        print(ratings_path, "doesn't exist in ", base_path)
        sys.exit(1)

    start = time.time()
    importing.import_movies(file=movies_path)
    importing.import_movie_details(file=links_path)
    importing.import_user_item(file=ratings_path)
    end = time.time() - start
    importing.close()
    print("Time to complete:", end)
