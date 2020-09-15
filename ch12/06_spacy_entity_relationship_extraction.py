import spacy
from neo4j import GraphDatabase
import neuralcoref
import pytextrank
import pandas as pd
from typing import List
from collections import defaultdict
from spacy.matcher import Matcher, DependencyMatcher
from ch12.text_processors import TextProcessor
from util.query_utils import executeNoException



class GraphBasedNLP(object):

    def __init__(self, language, uri, user, password):
        spacy.prefer_gpu()
        self.nlp = spacy.load('en_core_web_sm')
        coref = neuralcoref.NeuralCoref(self.nlp.vocab)
        self.nlp.add_pipe(coref, name='neuralcoref');
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
            executeNoException(session, "CREATE CONSTRAINT ON (l:Entity) ASSERT (l.type, l.id) IS NODE KEY")
            executeNoException(session, "CREATE CONSTRAINT ON (l:Evidence) ASSERT (l.id) IS NODE KEY")
            executeNoException(session, "CREATE CONSTRAINT ON (l:Relationship) ASSERT (l.id) IS NODE KEY")

    def tokenize_and_store(self, text, text_id, storeTag):
        docs = self.nlp.pipe([text])
        for doc in docs:
            annotated_text = self.__text_processor.create_annotated_text(doc, text_id)
            spans = self.__text_processor.process_sentences(annotated_text, doc, storeTag, text_id)
            nes = self.__text_processor.process_entities(spans, text_id)
            coref = self.__text_processor.process_coreference(doc, text_id)
            self.__text_processor.build_entities_inferred_graph(text_id)
            rules = [
                {
                    'type': 'RECEIVE_PRICE',
                    'verbs': ['receive'],
                    'subjectTypes': ['PERSON', 'NP'],
                    'objectTypes': ['WORK_OF_ART']
                }
            ]
            self.__text_processor.extract_relationships(text_id, rules)
            self.__text_processor.build_relationships_inferred_graph(text_id)


if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    basic_nlp = GraphBasedNLP(language="en", uri=uri, user="neo4j", password="pippo1")
    basic_nlp.tokenize_and_store(
        "Marie Curie received the Nobel Prize in Physics in 1903. "
        "She became the first woman to win the prize and the first person — man or woman — to win the award twice.",
        3,
        False)

    basic_nlp.close()
