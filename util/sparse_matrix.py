from scipy.sparse import csr_matrix


class SparseMatrix(object):

    def __init__(self):
        self.__indptr = [0]
        self.__indices = []
        self.__data = []
        self.__vocabulary = {}

    def addVector(self, vect):
        for element in vect:
            index = self.__vocabulary.setdefault(element, len(self.__vocabulary))
            self.__indices.append(index)
            self.__data.append(1)
        self.__indptr.append(len(self.__indices))

    def getMatrix(self):
        return csr_matrix((self.__data, self.__indices, self.__indptr), dtype=int)