import pandas as pd
import numpy as np
import sys
import time
from neo4j import GraphDatabase


class YoochooseImporter(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)

    def close(self):
        self._driver.close()

    def executeNoException(self, session, query):
        try:
            session.run(query)
        except Exception as e:
            pass
        
    def import_session_data(self, file):
        with self._driver.session() as session:
            self.executeNoException(session, "CREATE CONSTRAINT ON (s:Session) ASSERT s.sessionId IS UNIQUE")
            self.executeNoException(session, "CREATE CONSTRAINT ON (i:Item) ASSERT i.itemId IS UNIQUE")
            dtype = {"sessionID": np.int64, "itemID": np.int64, "category": np.object}
            j = 0;
            for chunk in pd.read_csv(file,
                                     header=0,
                                     dtype=dtype,
                                     names=['sessionID', 'timestamp', 'itemID', 'category'],
                                     parse_dates=['timestamp'],
                                     chunksize=10**6):
                df = chunk
                tx = session.begin_transaction()
                i = 0;
                query = """
                        MERGE (session:Session {sessionId: $sessionId})
                        MERGE (item:Item {itemId: $itemId, category: $category})
                        CREATE (click:Click {timestamp: $timestamp})
                        CREATE (session)-[:CONTAINS]->(click)
                        CREATE (click)-[:IS_RELATED_TO]->(item)
                    """

                for row in df.itertuples():
                    try:
                        timestamp = row.timestamp
                        session_id = row.sessionID
                        category = strip(row.category)
                        item_id = row.itemID
                        tx.run(query, {"sessionId": session_id, "itemId": item_id, "timestamp": str(timestamp), "category": category})
                        i += 1
                        j += 1
                        if i == 10000:
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row)
                tx.commit()
                print(j, "lines processed")
            print(j, "lines processed")
            #tx = session.begin_transaction()
            #query to set the last transaction
            #tx.commit

    def import_buys_data(self, file):
        with self._driver.session() as session:
            dtype = {"sessionID": np.int64, "itemID": np.int64, "price": np.float, "quantity": np.int}
            j = 0;
            for chunk in pd.read_csv(file,
                                     header=0,
                                     dtype=dtype,
                                     names=['sessionID', 'timestamp', 'itemID', 'price', 'quantity'],
                                     parse_dates=['timestamp'],
                                     chunksize=10**6):
                df = chunk
                tx = session.begin_transaction()
                i = 0;
                query = """
                        MATCH (session:Session {sessionId: $sessionId})
                        MATCH (item:Item {itemId: $itemId})
                        CREATE (buy:Buy:Click {timestamp: $timestamp})
                        CREATE (session)-[:CONTAINS]->(buy)
                        CREATE (buy)-[:IS_RELATED_TO]->(item)
                """

                for row in df.itertuples():
                    try:
                        timestamp = row.timestamp
                        session_id = row.sessionID
                        item_id = row.itemID
                        tx.run(query, {"sessionId": session_id, "itemId": item_id, "timestamp": str(timestamp)})
                        i += 1
                        j += 1
                        if i == 10000:
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row)
                tx.commit()
                print(j, "lines processed")
            print(j, "lines processed")


def strip(string): return ''.join([c if 0 < ord(c) < 128 else ' ' for c in string])


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "q1" # pippo1
    base_path = "/Users/ale/neo4j-servers/gpml/dataset/yoochoose-data"
    if (len(sys.argv) > 1):
        base_path = sys.argv[1]
    importing = YoochooseImporter(uri=uri, user=user, password=password)
    importing.import_session_data(file=base_path + "/yoochoose-clicks.dat")
    importing.import_buys_data(file=base_path + "/yoochoose-buys.dat")
    end = time.time() - start
    print("Time to complete:", end)
