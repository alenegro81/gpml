import hnswlib
import numpy as np
from neo4j import GraphDatabase
import time
from sklearn.neighbors import NearestNeighbors

class DistanceBasedAnalysis(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)

    def close(self):
        self._driver.close()

    def compute_and_store_distances(self, k, exact, distance_function, relationship_name):
        start = time.time()
        data, data_labels = self.get_transaction_vectors()
        print("Time to get vectors:", time.time() - start)
        start = time.time()

        if exact:
            ann_labels, ann_distances = self.compute_knn(data, data_labels, k, distance_function)
        else:
            ann_labels, ann_distances = self.compute_ann(data, data_labels, k, distance_function)

        print("Time to compute nearest neighbors:", time.time() - start)
        start = time.time()
        self.store_ann(data_labels, ann_labels, ann_distances, relationship_name)
        print("Time to store nearest neighbors:", time.time() - start)
        print("done")

    def compute_ann(self, data, data_labels, k, distance_function):
        dim = len(data[0])
        num_elements = len(data_labels)
        # Declaring index
        p = hnswlib.Index(space=distance_function, dim=dim)  # possible options for ditance_formula are l2, cosine or ip
        # Initing index - the maximum number of elements should be known beforehand
        p.init_index(max_elements=num_elements, ef_construction=800, M=200)
        # Element insertion (can be called several times):
        p.add_items(data, data_labels)
        # Controlling the recall by setting ef:
        p.set_ef(800)  # ef should always be > k
        # Query dataset, k - number of closest elements (returns 2 numpy arrays)
        labels, distances = p.knn_query(data, k = k)
        return labels, distances

    def compute_knn(self, data, data_labels, k, distance_function):
        pre_processed_data = [np.array(item) for item in data]
        nbrs = NearestNeighbors(n_neighbors=k, algorithm='brute', metric=distance_function, n_jobs=-1).fit(pre_processed_data)
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
                vector = result["vector"]

                data.append(vector)
                data_labels.append(transaction_id)
                i += 1
                if i % 10000 == 0:
                    print(i, "rows processed")
            print(i, "lines processed")
        return data, data_labels

    def store_ann(self, data_labels, ann_labels, ann_distances, label): #ADD the opportunity to specify the nsme of the relationship
        clean_query = """
            MATCH (transaction:Transaction)-[s:{}]->()
            WHERE transaction.transactionId = $transactionId
            DELETE s
        """.format(label)
        
        query = """
            MATCH (transaction:Transaction)
            WHERE transaction.transactionId = $transactionId
            UNWIND keys($knn) as otherSessionId
            MATCH (other:Transaction)
            WHERE other.transactionId = toInteger(otherSessionId) and other.transactionId <> $transactionId
            MERGE (transaction)-[:{} {{weight: $knn[otherSessionId]}}]->(other)
        """.format(label)
        with self._driver.session() as session:
            i = 0
            for label in data_labels:
                ann_labels_array = ann_labels[i]
                ann_distances_array = ann_distances[i]
                i += 1
                knnMap = {}
                j = 0
                for ann_label in ann_labels_array:
                    value = np.float(ann_distances_array[j])
                    knnMap[str(ann_label)] = value
                    j += 1
                tx = session.begin_transaction()
                tx.run(clean_query, {"transactionId": label})
                tx.run(query, {"transactionId": label, "knn": knnMap})
                tx.commit()

                if i % 1000 == 0:
                    print(i, "transactions processed")


if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    distance_formula_value = "l2" #'mahalanobis' for exact
    #relationship_name_value = "DISTANT_FROM_EXACT"
    relationship_name_value = "DISTANT_FROM"
    analyzer = DistanceBasedAnalysis(uri=uri, user="neo4j", password="q1")
    analyzer.compute_and_store_distances(25, False, distance_formula_value, relationship_name_value)
    # Uncomment this line to calculate exact NNs, but it will take a lot of time!
    # analyzer.compute_and_store_distances(25, True);
    analyzer.close()
