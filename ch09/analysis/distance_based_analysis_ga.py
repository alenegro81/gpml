import hnswlib
import pandas as pd
import numpy as np
import sys
import time
from random import randrange
from random import shuffle

from util.graphdb_base import GraphDBBase


class DistanceBasedAnalysis(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)

    def feature_selection(self, num_generations, sol_per_pop, num_parents_mating, newly_generated_elements, threshold):
        start = time.time()
        data, data_labels, data_fraud, data_labels_fraud, data_no_fraud, data_no_labels_fraud = self.get_data()
        print("Time to get vectors:", time.time() - start)
        start = time.time()
        vector_size = len(data[0])
        pop_size = (sol_per_pop, vector_size)
        new_population = np.random.uniform(low=0.0, high=1.0, size=pop_size)
        #new_population = np.array([[1 if item > threshold else 0 for item in vector] for vector in new_population])
        best_solution = []
        current_delta = 0
        previous_delta = 0
        for generation in range(num_generations):
            print("Starting iteration:", generation)
            if generation > 1 and (np.random.uniform(low=0.0, high=1.0) > 0.8 or abs(current_delta - previous_delta) < 10000):
                print("Generate a new sample")
                data, data_labels, data_fraud, data_labels_fraud, data_no_fraud, data_no_labels_fraud = self.get_data()

            pop_fitness = []
            print("Size of new population: ", new_population.shape[0])
            for individual in new_population:
                new_data = [np.multiply(vector, individual).tolist() for vector in data]
                model = self.compute_ann(new_data, data_labels)
                labels, distances = model.knn_query(data_fraud, k=25)
                fitness_fraud = compute_average_value(labels, distances)

                labels, distances = model.knn_query(data_no_fraud, k=25)
                fitness_no_fraud = compute_average_value(labels, distances)

                pop_fitness.append((individual, fitness_fraud, fitness_no_fraud))

            parents = select_mating_pool(pop_fitness, num_parents_mating)
            print("Size of best parents: ", len(parents))
            print("Size of best parents: ", [""+ str(fitness[1]) +" - " + str(fitness[2])+ ": " + str(fitness[1]-fitness[2]) for fitness in parents])

            print("Curret best parents: ", {fitness[1]:fitness[2] for fitness in parents})
            offspring_crossover = crossover([x[0] for x in parents], offspring_size=(pop_size[0] - len(parents) - newly_generated_elements, vector_size))
            offspring_mutation = mutation(offspring_crossover)
            new_population[0:len(parents), :] = np.array([x[0] for x in parents])
            new_population[len(parents):new_population.shape[0] - newly_generated_elements, :] = offspring_mutation
            elements = np.random.uniform(low=0.0, high=1.0, size=(newly_generated_elements, vector_size))
            new_population[new_population.shape[0] - newly_generated_elements:, :] = elements #np.array([[1 if item > threshold else 0 for item in vector] for vector in elements])
            best_solution = parents[0][0]
            previous_delta = current_delta
            current_delta = parents[0][1] - parents[0][2]

        #print(best_solution)
        np.savetxt("array.txt", best_solution, fmt="%s")
        #data = np.loadtxt("array_molto_bene.txt")
        print("done")

    def get_data(self):
        all_data_df = self.get_transaction_vectors("all")
        fraud_data_df = self.get_transaction_vectors("only_fraud")
        no_fraud_data_df = self.get_transaction_vectors("no_fraud")

        data_fraud = np.array(fraud_data_df['vector'].tolist())
        data_labels_fraud = np.array(fraud_data_df['transactionId'].tolist())
        data_no_fraud = np.array(no_fraud_data_df['vector'].tolist())
        data_no_labels_fraud = np.array(no_fraud_data_df['transactionId'].tolist())

        merge_df = pd.concat([all_data_df, fraud_data_df])
        merge_df = pd.concat([merge_df, no_fraud_data_df])

        merge_df = merge_df[~merge_df.index.duplicated(keep='first')]
        data = np.array(merge_df['vector'].tolist())

        data_labels = np.array(merge_df['transactionId'].tolist())

        print("Dataframe size:", merge_df.shape)
        return data, data_labels, data_fraud, data_labels_fraud, data_no_fraud, data_no_labels_fraud

    def compute_ann(self, data, data_labels):
        dim = len(data[0])
        num_elements = len(data_labels)
        # Declaring index
        p = hnswlib.Index(space='l2', dim=dim)  # possible options are l2, cosine or ip
        # Initing index - the maximum number of elements should be known beforehand
        p.init_index(max_elements=num_elements, ef_construction=400, M=200)
        # Element insertion (can be called several times):
        p.add_items(data, data_labels)
        # Controlling the recall by setting ef:
        p.set_ef(200)  # ef should always be > k
        # Query dataset, k - number of closest elements (returns 2 numpy arrays)
        #labels, distances = p.knn_query(data, k = 25)
        return p

    def get_transaction_vectors(self, fraud):

        if fraud == "only_fraud":
            list_of_transaction_query = """
                    MATCH (transaction:Transaction)
                    WHERE transaction.isFraud = 1
                    RETURN transaction.transactionId as transactionId, transaction.vector as vector, rand() as rand
                    order by rand 
                    LIMIT 492
                """
        elif fraud == "all":
            list_of_transaction_query = """
                    MATCH (transaction:Transaction)
                    RETURN transaction.transactionId as transactionId, transaction.vector as vector, rand() as rand
                    order by rand 
                    LIMIT 5000
                """
        else:
            list_of_transaction_query = """
                    MATCH (transaction:Transaction)
                    WHERE transaction.isFraud = 0
                    RETURN transaction.transactionId as transactionId, transaction.vector as vector, rand() as rand
                    order by rand 
                    LIMIT 492
                """

        data = []
        with self._driver.session() as session:
            i = 0
            for result in session.run(list_of_transaction_query):
                transaction_id = result["transactionId"]
                vector = result["vector"]

                data.append([transaction_id, vector])
                i += 1
                if i % 10000 == 0:
                    print(i, "rows processed")
            print(i, "lines processed")
        df = pd.DataFrame(data, columns = ['transactionId', 'vector'])
        df.set_index("transactionId", inplace=True, drop=False)
        return df


def compute_fitness(labels, distances):
    return np.sum(distances)


def compute_average_value(labels, distances):
    return np.average(distances)


def select_mating_pool(fitnesses, num_parents):
    return sorted(fitnesses, key=lambda item: item[1]/item[2], reverse=True)[0:num_parents]
    #if np.random.uniform(low=0.0, high=1.0) > 0.5:
    #    print("Optimizing by maximizing fraud distance")
    #    parents = sorted(fitnesses, key=lambda item: item[1], reverse=True)
    #    return parents[0:num_parents]
    #else:
    #    print("Optimizing by minimizing no fraud distance")
    #    parents = sorted(fitnesses, key=lambda item: item[2])
    #    return parents[0:num_parents]


def crossover(parents, offspring_size):
    offspring = np.empty(offspring_size)
    # The point at which crossover takes place between two parents. Usually, it is at the center.
    crossover_point = np.uint8(offspring_size[1] / 2)
    #Randomize parents
    shuffle(parents)
    for k in range(offspring_size[0]):
        # Index of the first parent to mate.
        parent1_idx = k % len(parents)
        # Index of the second parent to mate.
        parent2_idx = (k + 1) % len(parents)
        # The new offspring will have its first half of its genes taken from the first parent.
        offspring[k, 0:crossover_point] = parents[parent1_idx][0:crossover_point]
        # The new offspring will have its second half of its genes taken from the second parent.
        offspring[k, crossover_point:] = parents[parent2_idx][crossover_point:]
    return offspring


def mutation(offspring_crossover):
    # Mutation changes a single gene in each offspring randomly.
    for idx in range(offspring_crossover.shape[0]):
        # The random value to be added to the gene.
        number_of_genes_to_mutate =  randrange(int(offspring_crossover.shape[1]*0.2))
        random_values = np.random.uniform(low=0.0, high=1.0, size = number_of_genes_to_mutate)
        for gene in random_values:
            gene_index = randrange(offspring_crossover.shape[1])
            offspring_crossover[idx, gene_index] = gene
    return offspring_crossover


if __name__ == '__main__':
    analyzer = DistanceBasedAnalysis(sys.argv[1:])
    analyzer.feature_selection(num_generations = 40, sol_per_pop = 34, num_parents_mating = 12,
                               newly_generated_elements = 6, threshold=0.1)
    analyzer.close()
