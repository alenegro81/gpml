import time
import sys

from util.sparse_matrix import SparseMatrix
from util.lsh import LSH
from statistics import mean

from util.graphdb_base import GraphDBBase

class SessionBasedRecommender(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self.__time_to_query = []
        self.__time_to_knn = []
        self.__time_to_sort = []
        self.__time_to_store = []

    def close(self):
        self.close()

    # https: // github.com / brandonrobertz / SparseLSH
    def compute_and_store_similarity(self):
        start = time.time()
        items_VSM, items_id = self.get_item_vectors()
        print("Time to create the vector:", time.time() - start)
        lsh = LSH(13,
                  items_VSM.shape[1],
                  num_hashtables=1,
                  storage_config={"dict": None})
        start = time.time()
        i = 0
        overall_size = items_VSM.shape[0]
        for ix in range(overall_size):
            x = items_VSM.getrow(ix)
            c = items_id[ix]
            lsh.index(x, extra_data = c)
            i += 1
            if i % 1000 == 0:
                print(i, "rows processed over", overall_size)
        print("Time to index:", time.time() - start)
        knn_start = time.time()

        i = 0
        for ix in range(overall_size):
            knn = self.compute_knn(ix, items_id, items_VSM, lsh, 10)
            start = time.time()
            self.store_knn(items_id[ix], knn)
            self.__time_to_store.append(time.time() - start)
            i +=1
            if i%100 == 0:
                print(i, "rows processed over", overall_size)
                print(mean(self.__time_to_query),
                      mean(self.__time_to_knn),
                      mean(self.__time_to_sort),
                      mean(self.__time_to_store))
                self.__time_to_query = []
                self.__time_to_knn = []
                self.__time_to_sort = []
                self.__time_to_store = []
        print("Time to compute knn:", time.time() - knn_start)

    def compute_knn(self, ix, items_id, items_VSM, lsh, k):
        knn_values = []
        X_sim = items_VSM.getrow(ix)
        item_id = items_id[ix]
        start = time.time()
        other_items = lsh.query(X_sim, num_results=k, distance_func="cosine")
        self.__time_to_query.append(time.time() - start)
        start = time.time()
        for other_item in other_items:
            if other_item[0][1] != item_id:
                value = 1 - other_item[1].item(0)
                if value > 0:
                    knn_values.append((other_item[0][1], value))
        self.__time_to_knn.append(time.time() - start)
        start = time.time()
        knn_values.sort(key=lambda x: -x[1])
        self.__time_to_sort.append(time.time() - start)
        return knn_values[:k]

    def get_item_vectors(self):
        list_of_items_query = """
                    MATCH (item:Item)
                    RETURN item.itemId as itemId
                """

        query = """
                    MATCH (item:Item)<-[:RELATED_TO]-(click:Click)<-[:CONTAINS]-(session:Session)
                    WHERE item.itemId = $itemId
                    WITH session 
                    order by click.timestamp desc
                    limit 200
                    with session
                    ORDER BY id(session)
                    RETURN collect(distinct id(session)) as vector;
                """
        items_VSM_sparse = SparseMatrix()
        items_id = []
        with self._driver.session() as session:
            i = 0
            for item in session.run(list_of_items_query):
                item_id = item["itemId"]
                vector = session.run(query, {"itemId": item_id})
                items_VSM_sparse.addVector(vector.single()[0])
                items_id.append(item_id)
                #items_VSM_sparse[item_id] = vector.single()[0]
                i += 1
                if i % 1000 == 0:
                    print(i, "rows processed")
            print(i, "lines processed")
        return items_VSM_sparse.getMatrix(), items_id

    def store_knn(self, item, knn):
        with self._driver.session() as session:
            tx = session.begin_transaction()
            knnMap = {str(a) : b for a,b in knn}
            clean_query = """
                MATCH (item:Item)-[s:SIMILAR_TO]->()
                WHERE item.itemId = $itemId
                DELETE s
            """
            query = """
                MATCH (item:Item)
                WHERE item.itemId = $itemId
                UNWIND keys($knn) as otherItemId
                MATCH (other:Item)
                WHERE other.itemId = toInteger(otherItemId)
                MERGE (item)-[:SIMILAR_TO {weight: $knn[otherItemId]}]->(other)
            """
            tx.run(clean_query, {"itemId": item})
            if len(knn) > 0:
                tx.run(query, {"itemId": item, "knn": knnMap})
            tx.commit()

if __name__ == '__main__':
    recommender = SessionBasedRecommender(sys.argv[1:])
    recommender.compute_and_store_similarity()
    recommender.close()
