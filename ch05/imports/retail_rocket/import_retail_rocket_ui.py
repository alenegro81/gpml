import csv
import time
import os
import sys


from util.graphdb_base import GraphDBBase
from util.string_util import strip


class RetailRocketImporter(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)

    def import_user_item(self, file):
        with open(file, 'r+') as in_file:
            reader = csv.reader(in_file, delimiter=',')
            next(reader, None)
            with self._driver.session() as session:
                self.execute_without_exception("CREATE CONSTRAINT ON (u:User) ASSERT u.userId IS UNIQUE")
                self.execute_without_exception("CREATE CONSTRAINT ON (u:Item) ASSERT u.itemId IS UNIQUE")

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


if __name__ == '__main__':
    start = time.time()
    importing = RetailRocketImporter(argv=sys.argv[1:])
    base_path = importing.source_dataset_path
    if not base_path:
        base_path = "../../../dataset/retailrocket/"
    file_path = os.path.join(base_path, "events.csv")
    importing.import_user_item(file=file_path)
    end = time.time() - start
    print("Time to complete:", end)
