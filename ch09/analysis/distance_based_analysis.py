import hnswlib
import numpy as np
from neo4j import GraphDatabase
import time
import sklearn
from sklearn.neighbors import NearestNeighbors

class DistanceBasedAnalysis(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def compute_and_store_similarity(self, k, exact):
        start = time.time()
        data, data_labels = self.get_transaction_vectors()
        print("Time to get vectors:", time.time() - start)
        start = time.time()
        #selected_feature = np.loadtxt("array.txt")
        #new_data = [np.multiply(vector, selected_feature).tolist() for vector in data]
        if exact:
            ann_labels, ann_distances = self.compute_knn(data, data_labels, k)
        else:
            ann_labels, ann_distances = self.compute_ann(data, data_labels, k)
        print("Time to compute ann:", time.time() - start)
        start = time.time()
        self.store_ann(data_labels, ann_labels, ann_distances)
        print("Time to store ann:", time.time() - start)
        print("done")

    def compute_ann(self, data, data_labels, k):
        dim = len(data[0])
        num_elements = len(data_labels)
        # Declaring index
        p = hnswlib.Index(space='l2', dim=dim)  # possible options are l2, cosine or ip
        # Initing index - the maximum number of elements should be known beforehand
        p.init_index(max_elements=num_elements, ef_construction=800, M=200)
        # Element insertion (can be called several times):
        p.add_items(data, data_labels)
        # Controlling the recall by setting ef:
        p.set_ef(800)  # ef should always be > k
        # Query dataset, k - number of closest elements (returns 2 numpy arrays)
        labels, distances = p.knn_query(data, k = k)
        return labels, distances

    def compute_knn(self, data, data_labels, k):
        pre_processed_data = [np.array(item) for item in data]
        nbrs = NearestNeighbors(n_neighbors=k, algorithm='brute', metric='mahalanobis', n_jobs=-1).fit(pre_processed_data)
        knn_distances, knn_labels = nbrs.kneighbors(pre_processed_data)
        distances = knn_distances
        labels = [[data_labels[element] for element in item] for item in knn_labels]
        return labels, distances

    def get_transaction_vectors(self):
        list_of_transaction_query = """
                    MATCH (transaction:Transaction)
                    RETURN transaction.transactionId as transactionId, transaction.vector as vector
                """
        data = []
        data_labels = []
        with self._driver.session() as session:
            i = 0
            for result in session.run(list_of_transaction_query):
                transaction_id = result["transactionId"]
                vector = result["vector"];

                data.append(vector)
                data_labels.append(transaction_id)
                i += 1
                if i % 10000 == 0:
                    print(i, "rows processed")
            print(i, "lines processed")
        return data, data_labels

    def store_ann(self, data_labels, ann_labels, ann_distances): #ADD the opportunity to specify the nsme of the relationship
        clean_query = """
            MATCH (transaction:Transaction)-[s:SIMILAR_TO_400_L2]->()
            WHERE transaction.transactionId = {transactionId}
            DELETE s
        """
        query = """
            MATCH (transaction:Transaction)
            WHERE transaction.transactionId = {transactionId}
            UNWIND keys({knn}) as otherSessionId
            MATCH (other:Transaction)
            WHERE other.transactionId = toInt(otherSessionId) and other.transactionId <> {transactionId}
            MERGE (transaction)-[:SIMILAR_TO_400_L2 {weight: {knn}[otherSessionId]}]->(other)
        """
        with self._driver.session() as session:
            i = 0;
            for label in data_labels:
                ann_labels_array = ann_labels[i]
                ann_distances_array = ann_distances[i]
                i += 1
                knnMap = {}
                j = 0
                for ann_label in ann_labels_array:
                    value = np.float(ann_distances_array[j]);
                    knnMap[str(ann_label)] = value
                    j += 1
                tx = session.begin_transaction()
                tx.run(clean_query, {"transactionId": label})
                tx.run(query, {"transactionId": label, "knn": knnMap})
                tx.commit()

                if i % 10000 == 0:
                    print(i, "transactions processed")


if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    analyzer = DistanceBasedAnalysis(uri=uri, user="neo4j", password="pippo1")
    analyzer.compute_and_store_similarity(400, False);
    analyzer.close()