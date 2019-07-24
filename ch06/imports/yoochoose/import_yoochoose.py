import pandas as pd
import numpy as np
import time
from neo4j import GraphDatabase


class YoochooseImporter(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def import_session_data(self, file):
        with self._driver.session() as session:
            session.run("CREATE CONSTRAINT ON (s:Session) ASSERT s.sessionId IS UNIQUE")
            session.run("CREATE CONSTRAINT ON (i:Item) ASSERT i.itemId IS UNIQUE")
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
                        MERGE (session:Session {sessionId: {sessionId}})
                        MERGE (item:Item {itemId: {itemId}, category:{category}})
                        CREATE (click:Click {timestamp:{timestamp}})
                        CREATE (session)-[:CONTAINS]->(click)
                        CREATE (click)-[:RELATED_TO]->(item)
                    """
                #        CREATE (session)-[:LAST_CLICK]->(click)

                #        WITH click, session
                #        MATCH (session)-[r:LAST_CLICK]->(lastClick:Click)
                #        WHERE id(click) <> id(lastClick)
                #        CREATE (lastClick)-[:NEXT]->(click)
                #        DELETE r

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
                        MATCH (session:Session {sessionId: {sessionId}})
                        MATCH (item:Item {itemId: {itemId}})
                        CREATE (buy:Buy:Click {timestamp:{timestamp}})
                        CREATE (session)-[:CONTAINS]->(buy)
                        CREATE (buy)-[:RELATED_TO]->(item)
                """
                #        CREATE (session)-[:LAST_CLICK]->(buy)
                #        WITH buy, session
                #        MATCH (session)-[r:LAST_CLICK]->(lastClick:Click)
                #        WHERE id(buy) <> id(lastClick)
                #        CREATE (lastClick)-[:NEXT]->(buy)
                #        DELETE r

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
            # tx = session.begin_transaction()
            # query to set the last transaction
            # tx.commit


def strip(string): return ''.join([c if 0 < ord(c) < 128 else ' ' for c in string])


if __name__ == '__main__':
    start = time.time()
    uri = "bolt://localhost:7687"
    importing = YoochooseImporter(uri=uri, user="neo4j", password="pippo1")
    importing.import_session_data(file="/Users/ale/neo4j-servers/gpml/dataset/yoochoose-data/yoochoose-clicks.dat")
    importing.import_buys_data(file="/Users/ale/neo4j-servers/gpml/dataset/yoochoose-data/yoochoose-buys.dat")
    end = time.time() - start
    print("Time to complete:", end)
