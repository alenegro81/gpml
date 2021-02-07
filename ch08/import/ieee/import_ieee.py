import pandas as pd
import time
import threading
from queue import Queue
import math
import sys
import os

from util.graphdb_base import GraphDBBase

num_threads = 20

class IEEEImporter(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self._transactions = Queue()
        self._dictionaries = {}
        self._print_lock = threading.Lock()
        with self._driver.session() as session:
            self.execute_without_exception("CREATE CONSTRAINT ON (s:Transaction) ASSERT s.transactionId IS UNIQUE")
            self.execute_without_exception("CREATE INDEX ON :Transaction(isFraud)")
            self.execute_without_exception("CREATE INDEX ON :Transaction(isTrain)")
            
    def import_transaction(self, base_path):
        j = 0

        train_transactions = pd.read_csv(os.path.join(base_path, "train_transaction.csv"))
        train_transactions.set_index("TransactionID", inplace=True, drop = False)
        train_transactions.insert(1, 'train', 1)
        train_identity = pd.read_csv(os.path.join(base_path, "train_identity.csv"))
        train_identity.set_index("TransactionID", inplace=True)

        train = train_transactions.join(train_identity, how='left')

        test_transactions = pd.read_csv(os.path.join(base_path, "test_transaction.csv"))
        test_transactions.set_index("TransactionID", inplace=True, drop = False)
        test_transactions.insert(1, 'train', 0)

        test_identity = pd.read_csv(os.path.join(base_path, "test_identity.csv"))
        test_identity.set_index("TransactionID", inplace=True)
        test = test_transactions.join(test_identity, how='left')

        transactions = pd.concat([train, test])

        # Starting threads for parallel writing
        for k in range(num_threads):
            print("starting thread: ", k)
            writing_thread = threading.Thread(target = self.write_transaction)
            writing_thread.daemon = True
            writing_thread.start()

        for index, row in transactions.iterrows():
            j += 1
            transaction = {
                'transactionId': row['TransactionID'],
                'isFraud': row['isFraud'],
                'isTrain': row['train'],
                'transactionDt': row['TransactionDT'],
                'transactionAmt': row['TransactionAmt'],
                'productCd': row['ProductCD']}
            vector = self.normalize(row, ['TransactionID', 'isFraud', 'TransactionDT', 'train'])
            transaction['vector'] = vector
            self._transactions.put(transaction)
            # ADD ROW
            if j % 10000 == 0:
                print(j, "lines processed")
        print(j, "lines processed")
        self._transactions.join()
        print("Done")

    def normalize(self, row, exludes):
        vector = []
        for item in list(row.items()):
            if item[0] in exludes:
                continue
            if isinstance(item[1], str):
                vocab = {}
                if item[0] in self._dictionaries:
                    vocab = self._dictionaries[item[0]]
                else:
                    self._dictionaries[item[0]] = vocab

                if item[1] in vocab:
                    vocab_index = vocab[item[1]]
                else:
                    vocab_index = len(vocab) + 1
                    vocab[item[1]] = vocab_index
                vector.append(float(vocab_index))
                self._dictionaries[item[0]] = vocab
            elif math.isnan(item[1]):
                vector.append(float(0))
            else:
                vector.append(float(item[1]))
        return vector

    def write_transaction(self):
        query = """
                    WITH $row as map
                    CREATE (transaction:Transaction {transactionId: map.transactionId})
                    SET transaction += map
                """
        i = 0
        while True:
            row = self._transactions.get()
            with self._driver.session() as session:
                try:
                    session.run(query, {"row": row})
                    i += 1
                    if i % 2000 == 0:
                        with self._print_lock:
                            print(i, "lines processed on one thread")
                except Exception as e:
                    print(e, row)
            self._transactions.task_done()


if __name__ == '__main__':
    importer = IEEEImporter(sys.argv[1:])

    start = time.time()
    base_path = importer.source_dataset_path
    if not base_path:
        base_path = "../../../dataset/ieee"
    importer.import_transaction(base_path)
    print("Time to complete IEEE ingestion:", time.time() - start)

    # intermediate = time.time()
    # importer.post_processing(sess_clicks=sessions)
    # print("Time to complete post processing:", time.time() - intermediate)

    print("Time to complete end-to-end:", time.time() - start)

    importer.close()
