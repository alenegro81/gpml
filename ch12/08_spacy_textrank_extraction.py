import spacy
from neo4j import GraphDatabase
import neuralcoref
import pytextrank
import pandas as pd
import sys

import sys,os
sys.path.append(os.path.abspath(os.path.join(__file__, '..', '..')))
from util.query_utils import executeNoException
from ch12.text_processors import TextProcessor

class GraphBasedNLP(object):

    def __init__(self, language, uri, user, password):
        spacy.prefer_gpu()
        self.nlp = spacy.load('en_core_web_sm')
        #coref = neuralcoref.NeuralCoref(self.nlp.vocab)
        #self.nlp.add_pipe(coref, name='neuralcoref');
        tr = pytextrank.TextRank()
        self.nlp.add_pipe(tr.PipelineComponent, name='textrank', last=True)
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=0)
        self.__text_processor = TextProcessor(self.nlp, self._driver)
        self.create_constraints()

    def close(self):
        self._driver.close()

    def create_constraints(self):
        with self._driver.session() as session:
            executeNoException(session, "CREATE CONSTRAINT ON (u:Tag) ASSERT (u.id) IS NODE KEY")
            executeNoException(session, "CREATE CONSTRAINT ON (i:TagOccurrence) ASSERT (i.id) IS NODE KEY")
            executeNoException(session, "CREATE CONSTRAINT ON (t:Sentence) ASSERT (t.id) IS NODE KEY")
            executeNoException(session, "CREATE CONSTRAINT ON (l:AnnotatedText) ASSERT (l.id) IS NODE KEY")
            executeNoException(session, "CREATE CONSTRAINT ON (l:NamedEntity) ASSERT (l.id) IS NODE KEY")
            executeNoException(session, "CREATE CONSTRAINT ON (l:Keyword) ASSERT (l.id) IS NODE KEY")

    def import_data(self, file):
        j = 0;
        for chunk in pd.read_csv(file,
                                 header=None,
                                 skiprows=1,
                                 chunksize=10 ** 3):
            df = chunk
            for record in df.to_dict("records"):
                row = record.copy()
                j += 1
                self.tokenize_and_store(
                    row[7],
                    j,
                    False)
                if j % 500 == 0:
                    print(j, "lines processed")

        print(j, "total lines")

    def tokenize_and_store(self, text, text_id, storeTag):
        docs = self.nlp.pipe([text])
        for doc in docs:
            annotated_text = self.__text_processor.create_annotated_text(doc, text_id)
            spans = self.__text_processor.process_sentences(annotated_text, doc, storeTag, text_id)
            self.__text_processor.process_entities(spans, text_id)
            #self.process_coreference(doc, text_id)
            self.__text_processor.process_textrank(doc, text_id)

if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    basic_nlp = GraphBasedNLP(language="en", uri=uri, user="neo4j", password="pippo1")
    # basic_nlp.tokenize_and_store("Marie Curie received the Nobel Prize in Physic in 1903. She became the first woman to win the prize and the first person — man or woman — to win the award twice.", 3,
    #                            False)
    #basic_nlp.tokenize_and_store("The Committee awarded the Nobel Prize in Physic to Marie Curie.", 5, False)
    base_path = "/Users/ale/neo4j-servers/gpml/dataset"
    if (len(sys.argv) > 1):
        base_path = sys.argv[1]
    basic_nlp.import_data(file=os.path.abspath(os.path.join(base_path, "wiki_movie_plots_deduped.csv")))
    basic_nlp.close()
