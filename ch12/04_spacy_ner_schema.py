import spacy
from neo4j import GraphDatabase
from spacy.lang.en.stop_words import STOP_WORDS


class GraphBasedNLP(object):

    def __init__(self, language, uri, user, password):
        spacy.prefer_gpu()
        self.nlp = spacy.load("en_core_web_sm")
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.create_constraints()

    def close(self):
        self._driver.close()

    def create_constraints(self):
        with self._driver.session() as session:
            session.run("CREATE CONSTRAINT ON (u:Tag) ASSERT (u.id) IS NODE KEY")
            session.run("CREATE CONSTRAINT ON (i:TagOccurrence) ASSERT (i.id) IS NODE KEY")
            session.run("CREATE CONSTRAINT ON (t:Sentence) ASSERT (t.id) IS NODE KEY")
            session.run("CREATE CONSTRAINT ON (l:AnnotatedText) ASSERT (l.id) IS NODE KEY")
            session.run("CREATE CONSTRAINT ON (l:NamedEntity) ASSERT (l.id) IS NODE KEY")

    def tokenize_and_store(self, text, text_id, storeTag):
        docs = self.nlp.pipe([text])
        for doc in docs:
            annotated_text = self.create_annotated_text(doc, text_id)
            i = 1
            for sentence in doc.sents:
                print("-------- Sentence ", i, "-----------")
                sentence_id = self.store_sentence(sentence, annotated_text, text_id, i, storeTag)
                spans = list(doc.ents) + list(doc.noun_chunks)
                spans = filter_spans(spans)
                i += 1
            for entity in spans:
                print("-------- Entity: ", entity.text, entity.label_, entity.start_char, entity.end_char,
                      "-----------")
                ne = {'value': entity.text, 'type': entity.label_, 'start_index': entity.start_char,
                      'end_index': entity.end_char}
                self.process_ne(text_id, ne)

    def create_annotated_text(self, doc, id):
        query = """MERGE (ann:AnnotatedText {id: {id}})
            RETURN id(ann) as result
        """
        params = {"id": id}
        results = self.execute_query(query, params)
        return results[0]

    def store_sentence(self, sentence, annotated_text, text_id, sentence_id, storeTag):
        sentence_query = """MATCH (ann:AnnotatedText) WHERE id(ann) = {ann_id}
            MERGE (sentence:Sentence {id: {sentence_unique_id}})
            SET sentence.text = {text}
            MERGE (ann)-[:CONTAINS_SENTENCE]->(sentence)
            RETURN id(sentence) as result
        """

        tag_occurrence_query = """MATCH (sentence:Sentence) WHERE id(sentence) = {sentence_id}
            WITH sentence, {tag_occurrences} as tags
            FOREACH ( idx IN range(0,size(tags)-2) |
            MERGE (tagOccurrence1:TagOccurrence {id: tags[idx].id})
            SET tagOccurrence1 = tags[idx]
            MERGE (sentence)-[:HAS_TOKEN]->(tagOccurrence1)
            MERGE (tagOccurrence2:TagOccurrence {id: tags[idx + 1].id})
            SET tagOccurrence2 = tags[idx + 1]
            MERGE (sentence)-[:HAS_TOKEN]->(tagOccurrence2)
            MERGE (tagOccurrence1)-[r:HAS_NEXT {sentence: sentence.id}]->(tagOccurrence2))
            RETURN id(sentence) as result
        """

        tag_occurrence_with_tag_query = """MATCH (sentence:Sentence) WHERE id(sentence) = {sentence_id}
            WITH sentence, {tag_occurrences} as tags
            FOREACH ( idx IN range(0,size(tags)-2) |
            MERGE (tagOccurrence1:TagOccurrence {id: tags[idx].id})
            SET tagOccurrence1 = tags[idx]
            MERGE (sentence)-[:HAS_TOKEN]->(tagOccurrence1)
            MERGE (tagOccurrence2:TagOccurrence {id: tags[idx + 1].id})
            SET tagOccurrence2 = tags[idx + 1]
            MERGE (sentence)-[:HAS_TOKEN]->(tagOccurrence2)
            MERGE (tagOccurrence1)-[r:HAS_NEXT {sentence: sentence.id}]->(tagOccurrence2))
            FOREACH (tagItem in [tag_occurrence IN {tag_occurrences} WHERE tag_occurrence.is_stop = False] | 
            MERGE (tag:Tag {id: tagItem.lemma}) MERGE (tagOccurrence:TagOccurrence {id: tagItem.id}) MERGE (tag)<-[:REFERS_TO]-(tagOccurrence))
            RETURN id(sentence) as result
        """

        params = {"ann_id": annotated_text, "text": sentence.text,
                  "sentence_unique_id": str(text_id) + "_" + str(sentence_id)}
        results = self.execute_query(sentence_query, params)
        node_sentence_id = results[0]
        tag_occurrences = []
        tag_occurrence_dependencies = []
        for token in sentence:
            lexeme = self.nlp.vocab[token.text]
            if not lexeme.is_punct and not lexeme.is_space:
                tag_occurrence_id = str(text_id) + "_" + str(sentence_id) + "_" + str(token.idx)
                tag_occurrence = {"id": tag_occurrence_id,
                                  "index": token.idx,
                                  "text": token.text,
                                  "lemma": token.lemma_,
                                  "pos": token.tag_,
                                  "is_stop": (lexeme.is_stop or lexeme.is_punct or lexeme.is_space)}
                tag_occurrences.append(tag_occurrence)
                tag_occurrence_dependency_source = str(text_id) + "_" + str(sentence_id) + "_" + str(token.head.idx)
                dependency = {"source": tag_occurrence_dependency_source, "destination": tag_occurrence_id,
                              "type": token.dep_}
                tag_occurrence_dependencies.append(dependency)
        params = {"sentence_id": node_sentence_id, "tag_occurrences": tag_occurrences}
        if storeTag:
            results = self.execute_query(tag_occurrence_with_tag_query, params)
        else:
            results = self.execute_query(tag_occurrence_query, params)

        self.process_dependencies(tag_occurrence_dependencies)
        return results[0]

    def process_dependencies(self, tag_occurrence_dependencie):
        tag_occurrence_query = """UNWIND {dependencies} as dependency
            MATCH (source:TagOccurrence {id: dependency.source})
            MATCH (destination:TagOccurrence {id: dependency.destination})
            MERGE (source)-[:IS_DEPENDENT {type: dependency.type}]->(destination)
                """
        self.execute_query(tag_occurrence_query, {"dependencies": tag_occurrence_dependencie})

    def process_ne(self, document_id, ne):
        ne_query = """MERGE (ne:NamedEntity {id: {id}})
            SET ne.type = {ne}.type, ne.value = {ne}.value, ne.index = {ne}.start_index
            WITH ne, {ne} as neIndex
            MATCH (text:AnnotatedText)-[:CONTAINS_SENTENCE]->(sentence:Sentence)-[:HAS_TOKEN]->(tagOccurrence:TagOccurrence)
            WHERE text.id = {documentId} AND tagOccurrence.index >= neIndex.start_index AND tagOccurrence.index < neIndex.end_index
            MERGE (ne)<-[:PARTICIPATE_IN]-(tagOccurrence)
        """
        self.execute_query(ne_query, {"id": str(document_id) + "_" + str(ne['start_index']), "documentId":document_id, "ne": ne})

    def execute_query(self, query, params):
        results = []
        with self._driver.session() as session:
            for items in session.run(query, params):
                item = items["result"];
                results.append(item)
        return results


def filter_spans(spans):
    get_sort_key = lambda span: (span.end - span.start, -span.start)
    sorted_spans = sorted(spans, key=get_sort_key, reverse=True)
    result = []
    seen_tokens = set()
    for span in sorted_spans:
        # Check for end - 1 here because boundaries are inclusive
        if span.start not in seen_tokens and span.end - 1 not in seen_tokens:
            result.append(span)
        seen_tokens.update(range(span.start, span.end))
    result = sorted(result, key=lambda span: span.start)
    return result


if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    basic_nlp = GraphBasedNLP(language="en", uri=uri, user="neo4j", password="pippo1")
    basic_nlp.tokenize_and_store("President Barack Obama was born in Hawaii.  He was elected president in 2008.", 1,
                                 False)
    basic_nlp.close()
