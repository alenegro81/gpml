import spacy
import pytextrank
import pandas as pd
import sys,os

from ch12.text_processors import TextProcessor
from util.graphdb_base import GraphDBBase


class GraphBasedNLP(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        spacy.prefer_gpu()
        self.nlp = spacy.load('en_core_web_sm')
        #coref = neuralcoref.NeuralCoref(self.nlp.vocab)
        #self.nlp.add_pipe(coref, name='neuralcoref');
        tr = pytextrank.TextRank()
        self.nlp.add_pipe(tr.PipelineComponent, name='textrank', last=True)
        self.__text_processor = TextProcessor(self.nlp, self._driver)
        self.create_constraints()

    def close(self):
        self.close()

    def create_constraints(self):
        self.execute_without_exception("CREATE CONSTRAINT ON (u:Tag) ASSERT (u.id) IS NODE KEY")
        self.execute_without_exception("CREATE CONSTRAINT ON (i:TagOccurrence) ASSERT (i.id) IS NODE KEY")
        self.execute_without_exception("CREATE CONSTRAINT ON (t:Sentence) ASSERT (t.id) IS NODE KEY")
        self.execute_without_exception("CREATE CONSTRAINT ON (l:AnnotatedText) ASSERT (l.id) IS NODE KEY")
        self.execute_without_exception("CREATE CONSTRAINT ON (l:NamedEntity) ASSERT (l.id) IS NODE KEY")
        self.execute_without_exception("CREATE CONSTRAINT ON (l:Keyword) ASSERT (l.id) IS NODE KEY")

    def import_data(self, file):
        j = 0
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
    basic_nlp = GraphBasedNLP(sys.argv[1:])
    base_path = basic_nlp.source_dataset_path
    if not base_path:
        base_path = "../../../dataset/movieplot"
    basic_nlp.import_data(file=os.path.abspath(os.path.join(base_path, "wiki_movie_plots_deduped.csv")))
    basic_nlp.close()
