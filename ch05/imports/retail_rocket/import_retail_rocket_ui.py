import csv
import time
from neo4j import GraphDatabase
import sys


class RetailRocketImporter(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)

    def close(self):
        self._driver.close()

    def executeNoException(self, session, query):
        try:
            session.run(query)
        except Exception as e:
            pass

    def import_user_item(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session() as session:
                # this needs to be wrapped into 
                self.executeNoException(session, "CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE")
                self.executeNoException(session, "CREATE CONSTRAINT ON (u:Item) ASSERT u.itemId IS UNIQUE")

                tx = session.begin_transaction()
                i = 0
                j = 0
                query = """
                    MERGE (item:Item {itemId: $itemId})
                    MERGE (user:User {userId: $userId})
                    MERGE (user)-[:PURCHASES { timestamp: $timestamp}]->(item)
                """
                for row in reader:
                    try:
                        if row:
                            timestamp = strip(row[0])
                            user_id = strip(row[1])
                            event_type = strip(row[2])
                            item_id = strip(row[3])

                            if event_type == "transaction":
                                tx.run(query, {"itemId": item_id, "userId": user_id, "timestamp": timestamp})
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
    user = "neo4j"
    password = "q1"  # pippo1
    file_path = "/Users/ale/neo4j-servers/gpml/dataset/retailrocket-recommender-system-dataset/events.csv"
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    importing = RetailRocketImporter(uri=uri, user=user, password=password)
    importing.import_user_item(file=file_path)
    end = time.time() - start
    print("Time to complete:", end)
