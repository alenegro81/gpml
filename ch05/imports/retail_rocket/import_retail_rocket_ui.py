import csv
import time
from neo4j.v1 import GraphDatabase
from imdb import IMDb


class RetailRocketImporter(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def import_user_item(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session() as session:
                session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE")
                session.run("CREATE CONSTRAINT ON (u:Item) ASSERT u.itemId IS UNIQUE")

                tx = session.begin_transaction()
                i = 0;
                j = 0;
                query = """
                    MERGE (item:Item {itemId: {itemId}})
                    MERGE (user:User {userId: {userId}})
                    MERGE (user)-[:PURCHASES { timestamp: {timestamp}}]->(item)
                """
                for row in reader:
                    try:
                        if row:
                            timestamp = strip(row[0])
                            user_id = strip(row[1])
                            event_type = strip(row[2])
                            item_id = strip(row[3])

                            if event_type == "transaction":
                                tx.run(query, {"itemId":item_id, "userId": user_id,  "timestamp": timestamp})
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


def strip(string): return ''.join([c if 0 < ord(c) < 128 else ' ' for c in string])


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    importing = RetailRocketImporter(uri=uri, user="neo4j", password="pippo1")
    importing.import_user_item(file="/Users/ale/neo4j-servers/gpml/dataset/retailrocket-recommender-system-dataset/events.csv")
    end = time.time() - start
    print("Time to complete:", end)
