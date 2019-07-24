import time
from neo4j import GraphDatabase
from imdb import IMDb
from imdb._exceptions import IMDbParserError
import csv
from queue import Queue
import threading



class DePaulMovieImporter(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._ia = IMDb(reraiseExceptions=True)
        self._movie_queue = Queue()
        self._writing_queue = Queue()
        self._print_lock = threading.Lock()

    def close(self):
        self._driver.close()

    def import_event_data(self, file):
        with self._driver.session() as session:
            session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE")
            session.run("CREATE CONSTRAINT ON (i:Item) ASSERT i.itemId IS UNIQUE")
            session.run("CREATE CONSTRAINT ON (t:Time) ASSERT t.value IS UNIQUE")
            session.run("CREATE CONSTRAINT ON (l:Location) ASSERT l.value IS UNIQUE")
            session.run("CREATE CONSTRAINT ON (c:Companion) ASSERT c.value IS UNIQUE")

            j = 0;
            with open(file, 'r+') as in_file:
                reader = csv.reader(in_file, delimiter=',')
                next(reader, None)
                tx = session.begin_transaction()
                i = 0;
                query = """
                        MERGE (user:User {userId: {userId}})
                        MERGE (time:Time {value: {time}})
                        MERGE (location:Location {value: {location}})
                        MERGE (companion:Companion {value: {companion}})
                        MERGE (item:Item {itemId: {itemId}})
                        CREATE (event:Event {rating:{rating}})
                        CREATE (event)-[:EVENT_USER]->(user)
                        CREATE (event)-[:EVENT_ITEM]->(item)
                        CREATE (event)-[:EVENT_LOCATION]->(location)
                        CREATE (event)-[:EVENT_COMPANION]->(companion)
                        CREATE (event)-[:EVENT_TIME]->(time)
                    """

                for row in reader:
                    try:
                        if row:
                            user_id = row[0]
                            item_id = strip(row[1])
                            rating = strip(row[2])
                            time = strip(row[3])
                            location = strip(row[4])
                            companion = strip(row[5])
                            tx.run(query, {"userId": user_id, "time": time, "location": location, "companion": companion, "itemId": item_id, "rating": rating})
                            i += 1
                            j += 1
                            if i == 1000:
                                tx.commit()
                                print(j, "lines processed")
                                i = 0
                                tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row)
                tx.commit()
                print(j, "lines processed")
            print(j, "lines processed")

    def import_movie_details(self):
        for k in range(50):
            print("starting thread: ", k)
            movie_info_thread = threading.Thread(target=self.get_movie_info)
            movie_info_thread.daemon = True
            movie_info_thread.start()

        writing_thread = threading.Thread(target=self.write_movie_on_db)
        writing_thread.daemon = True
        writing_thread.start()

        get_items_query = """
                            MATCH (item:Item)
                            RETURN item.itemId as itemId
                        """

        with self._driver.session() as session:
            i = 0;
            j = 0;

            for item in session.run(get_items_query):
                item_id = item["itemId"];
                self._movie_queue.put(item_id)
                i += 1
                j += 1
                if i == 1000:
                    print(j, "lines processed")
                    i = 0
            print(j, "lines processed")
            self._movie_queue.join()
            self._writing_queue.put(["0000", 0])
            self._writing_queue.join()
            print("Done")

    def get_movie_info(self):
        while True:
            imdb_id  = self._movie_queue.get()
            with self._print_lock:
                print("Getting info for row: ", imdb_id)
            # get a movie
            retry = 0
            while retry < 5:
                try:
                    movie = self._ia.get_movie(imdb_id.replace('tt', ''))
                    with self._print_lock:
                        print("Writing to the other queue: ", movie)
                    self._writing_queue.put([imdb_id, movie])
                    break
                except IMDbParserError as e:
                    with self._print_lock:
                        print("Error while getting. Ignoring", imdb_id)
                    break
                except :
                    with self._print_lock:
                        print("An error occurred")
                    retry = retry + 1
                    if retry == 10:
                        with self._print_lock:
                            print("Error while getting", imdb_id)
                    else:
                        with self._print_lock:
                            print("Failed...... ", retry)
                        time.sleep(10)
            self._movie_queue.task_done()

    def write_movie_on_db(self):
        query = """
            MATCH (movie:Item {itemId: {movieId}})
            SET movie.plot = {plot}, movie.title = {title}
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
            if movie_id == "0000":
                with self._print_lock:
                    print("Writing movie exiting", self._writing_queue.empty())
                self._writing_queue.task_done()
                break
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

                    title = ''
                    if 'title' in movie_info:
                        title = movie_info['title']
                    session.run(query, {"movieId":movie_id, "directors": directors, "genres": genres, "actors": actors, "plot": plot, "writers": writers, "producers": producers, 'title': title})
                except Exception as e:
                    print(movie_id, e)
            self._writing_queue.task_done()



def strip(string): return ''.join([c if 0 < ord(c) < 128 else ' ' for c in string])


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    importing = DePaulMovieImporter(uri=uri, user="neo4j", password="pippo1")
    #importing.import_event_data(file="/Users/ale/neo4j-servers/gpml/dataset/Movie_DePaulMovie/ratings.txt")
    importing.import_movie_details()
    end = time.time() - start
    importing.close()
    print("Time to complete:", end)
