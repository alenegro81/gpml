import spacy
from neo4j import GraphDatabase
import neuralcoref
import pytextrank
import pandas as pd
from typing import List
from collections import defaultdict

from spacy.matcher import Matcher, DependencyMatcher



class GraphBasedNLP(object):

    def __init__(self, language, uri, user, password):
        spacy.prefer_gpu()
        self.nlp = spacy.load('en_core_web_sm')
        coref = neuralcoref.NeuralCoref(self.nlp.vocab)
        self.nlp.add_pipe(coref, name='neuralcoref');
        tr = pytextrank.TextRank()
        self.nlp.add_pipe(tr.PipelineComponent, name='textrank', last=True)
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
            session.run("CREATE CONSTRAINT ON (l:Keyword) ASSERT (l.id) IS NODE KEY")

    def import_masc(self, file):
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
            print(j, "lines processed")
        print(j, "total lines")

    def tokenize_and_store(self, text, text_id, storeTag):
        docs = self.nlp.pipe([text])
        for doc in docs:
            annotated_text = self.create_annotated_text(doc, text_id)
            spans = self.process_sentences(annotated_text, doc, storeTag, text_id)
            self.process_entities(spans, text_id)
            self.process_coreference(doc, text_id)
            self.run_dependency_matcher(doc)

    def process_sentences(self, annotated_text, doc, storeTag, text_id):
        i = 1
        for sentence in doc.sents:
            #print("-------- Sentence ", i, "-----------")
            sentence_id = self.store_sentence(sentence, annotated_text, text_id, i, storeTag)
            spans = list(doc.ents) + list(doc.noun_chunks)
            spans = filter_spans(spans)
            i += 1
        return spans

    def process_entities(self, spans, text_id):
        nes = []
        for entity in spans:
            #print("-------- Entity: ", entity.text, entity.label_, entity.start_char, entity.end_char,
            #      "-----------")
            ne = {'value': entity.text, 'type': entity.label_, 'start_index': entity.start_char,
                  'end_index': entity.end_char}
            nes.append(ne)
        self.store_entities(text_id, nes)

    def process_coreference(self, doc, text_id):
        if doc._.has_coref:
            coref = []
            for cluster in doc._.coref_clusters:
                mention = {'from_index': cluster.mentions[-1].start_char, 'to_index': cluster.mentions[0].start_char}
                coref.append(mention)
            self.store_coref(text_id, coref)

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

    def run_dependency_matcher(self, doc):
        # example = [["founded", "nsubj", "START_ENTITY"], ["founded", "dobj", "END_ENTITY"]]
        example = [["found", "nsubj", "PERSON"], ["found", "dobj", "ORG"]]
        # example = [["works", "nsubj", "PERSON"], ["works", "pobj", "ORG"]]

        pattern = construct_pattern(example, match_on_lemmas=True)
        if pattern is None:
            print("No pattern constructed, aborting.")
            return
        print(pattern)

        matcher = DependencyMatcher(self.nlp.vocab)
        matcher.add("pattern1", None, pattern)

        match = matcher(doc)[0]
        if match is None or len(match) < 1 or len(match[1]) < 1:
            return
        print(match)
        subtree = match[1][0]
        print(f"{doc[subtree[1]]} -[{doc[subtree[0]]}]-> {doc[subtree[2]]}")
        # visualise_subtrees(doc, subtree)

    def store_entities(self, document_id, nes):
        ne_query = """
            UNWIND {nes} as item
            MERGE (ne:NamedEntity {id: toString({documentId}) + "_" + toString(item.start_index)})
            SET ne.type = item.type, ne.value = item.value, ne.index = item.start_index
            WITH ne, item as neIndex
            MATCH (text:AnnotatedText)-[:CONTAINS_SENTENCE]->(sentence:Sentence)-[:HAS_TOKEN]->(tagOccurrence:TagOccurrence)
            WHERE text.id = {documentId} AND tagOccurrence.index >= neIndex.start_index AND tagOccurrence.index < neIndex.end_index
            MERGE (ne)<-[:PARTICIPATE_IN]-(tagOccurrence)
        """
        self.execute_query(ne_query, {"documentId": document_id, "nes": nes})

    def store_coref(self, document_id, corefs):
        coref_query = """
                MATCH (document:AnnotatedText)
                WHERE document.id = {documentId} 
                WITH document
                UNWIND {corefs} as coref  
                MATCH (document)-[*3..3]->(start:NamedEntity), (document)-[*3..3]->(end:NamedEntity) 
                WHERE start.index = coref.from_index AND end.index = coref.to_index
                MERGE (start)-[:MENTIONS]->(end)
        """
        self.execute_query(coref_query,
                           {"documentId": document_id, "corefs": corefs})

    def execute_query(self, query, params):
        results = []
        with self._driver.session() as session:
            for items in session.run(query, params):
                item = items["result"];
                results.append(item)
        return results

def construct_pattern(dependency_triples: List[List[str]], match_on_lemmas: bool = False):
    """
    Idea: add patterns to a matcher designed to find a subtree in a spacy dependency tree.
    Rules are strictly of the form "Parent --rel--> Child". To build this up, we add rules
    in DFS order, so that the parent nodes have already been added to the dict for each child
    we encounter.

    # Parameters
    dependency_triples: List[List[str]]
        A list of [parent, relation, child] triples, which together
        form a tree that we would like to match on.

    # Returns
    pattern:
        A json structure defining the match for the given tree, which
        can be passed to the spacy DependencyMatcher.

    """
    # Step 1: Build up a dictionary mapping parents to their children
    # in the dependency subtree. Whilst we do this, we check that there is
    # a single node which has only outgoing edges.

    root, parent_to_children = check_for_non_trees(dependency_triples)
    if root is None:
        return None

    def add_node(parent: str, pattern: List):
        for (rel, child) in parent_to_children[parent]:
            # First, we add the specification that we are looking for
            # an edge which connects the child to the parent.
            node = {
                "SPEC": {
                    "NODE_NAME": child,
                    "NBOR_RELOP": ">",
                    "NBOR_NAME": parent}
            }
            # We want to match the relation exactly.
            token_pattern = {"DEP": rel}

            # Because we're working specifically with relation extraction in mind,
            # we'll use START_ENTITY and END_ENTITY as dummy placeholders in our
            # list of triples to indicate that we want to match a word which is contained
            # within an entity (or the entity itself if you have added the merge_entities pipe
            # to your pipeline before running the matcher).
            if child not in {"START_ENTITY", "END_ENTITY"}:
                # token_pattern["ORTH"] = child
                token_pattern["ENT_TYPE"] = child
            else:
                token_pattern["ENT_TYPE"] = {"NOT_IN": [""]}

            node["PATTERN"] = token_pattern

            pattern.append(node)
            add_node(child, pattern)

    match_on = "ORTH"
    if match_on_lemmas:
        print("Matching on lemmas.")
        match_on = "LEMMA"

    pattern = [{"SPEC": {"NODE_NAME": root}, "PATTERN": {match_on: root}}]
    add_node(root, pattern)

    return pattern

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


def filter_extended_spans(items):
    get_sort_key = lambda item: (item['span'].end - item['span'].start, -item['span'].start)
    sorted_spans = sorted(items, key=get_sort_key, reverse=True)
    result = []
    seen_tokens = set()
    for item in sorted_spans:
        # Check for end - 1 here because boundaries are inclusive
        if item['span'].start not in seen_tokens and item['span'].end - 1 not in seen_tokens:
            result.append(item)
        seen_tokens.update(range(item['span'].start, item['span'].end))
    result = sorted(result, key=lambda span: span['span'].start)
    return result

def check_for_non_trees(dependency_triples: List[List[str]]):
    """
    A utility function which checks:

    1. The dependency triples you pass in are not self referential
    2. The triples you pass in form a single tree, with one root.
    3. There are no loops in the triples you pass in.

    # Parameters
    dependency_triples: List[List[str]]
        A list of [parent, relation, child] triples, which together
        form a tree that we would like to match on.

    # Returns
    root: str
        The root of the subtree
    parent_to_children: Dict[str, List[Tuple[str, str]]]
        A dictionary mapping parents to a list of their children,
        where the child is represented as a (relation, child) tuple.
    """

    parent_to_children = defaultdict(list)
    seen = set()
    has_incoming_edges = set()
    for (parent, rel, child) in dependency_triples:
        seen.add(parent)
        seen.add(child)
        has_incoming_edges.add(child)
        if parent == child:
            return None, None
        parent_to_children[parent].append((rel, child))

    # Only accept strictly connected trees with a single root.
    roots = seen.difference(has_incoming_edges)
    if len(roots) != 1:
        return None, None

    root = roots.pop()
    seen = {root}

    # Step 2: check that the tree doesn't have a loop:
    def contains_loop(node):
        has_loop = False
        for (_, child) in parent_to_children[node]:
            if child in seen:
                return True
            else:
                seen.add(child)
                has_loop = contains_loop(child)
            if has_loop:
                break

        return has_loop

    if contains_loop(root):
        return None, None

    return root, parent_to_children

if __name__ == '__main__':
    uri = "bolt://localhost:7687"
    basic_nlp = GraphBasedNLP(language="en", uri=uri, user="neo4j", password="pippo1")
    basic_nlp.tokenize_and_store("Bill Gates, billionaire and philanthropist, founded Microsoft.", 3000000,
                                False)

    basic_nlp.close()



#https://github.com/explosion/spaCy/blob/master/examples/information_extraction/entity_relations.py
#import spacy
#nlp = spacy.load('en_core_web_sm')
#doc = nlp(u"Marie Curie received the Nobel Prize in Physic")
#options = {"collapse_phrases": True}
#spacy.displacy.serve(doc, style='dep', options=options)