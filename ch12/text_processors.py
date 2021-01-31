import spacy
from spacy.lang.en.stop_words import STOP_WORDS


class TextProcessor(object):
    def __init__(self, nlp, driver):
        self.nlp = nlp
        self._driver = driver

    def create_annotated_text(self, doc, id):
        query = """MERGE (ann:AnnotatedText {id: $id})
            RETURN id(ann) as result
        """
        params = {"id": id}
        results = self.execute_query(query, params)
        return results[0]

    def process_sentences(self, annotated_text, doc, storeTag, text_id):
        i = 1
        for sentence in doc.sents:
            sentence_id = self.store_sentence(sentence, annotated_text, text_id, i, storeTag)
            spans = list(doc.ents) + list(doc.noun_chunks)
            spans = filter_spans(spans)
            i += 1
        return spans

    def store_sentence(self, sentence, annotated_text, text_id, sentence_id, storeTag):
        sentence_query = """MATCH (ann:AnnotatedText) WHERE id(ann) = $ann_id
            MERGE (sentence:Sentence {id: $sentence_unique_id})
            SET sentence.text = $text
            MERGE (ann)-[:CONTAINS_SENTENCE]->(sentence)
            RETURN id(sentence) as result
        """

        tag_occurrence_query = """MATCH (sentence:Sentence) WHERE id(sentence) = $sentence_id
            WITH sentence, $tag_occurrences as tags
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

        tag_occurrence_with_tag_query = """MATCH (sentence:Sentence) WHERE id(sentence) = $sentence_id
            WITH sentence, $tag_occurrences as tags
            FOREACH ( idx IN range(0,size(tags)-2) |
            MERGE (tagOccurrence1:TagOccurrence {id: tags[idx].id})
            SET tagOccurrence1 = tags[idx]
            MERGE (sentence)-[:HAS_TOKEN]->(tagOccurrence1)
            MERGE (tagOccurrence2:TagOccurrence {id: tags[idx + 1].id})
            SET tagOccurrence2 = tags[idx + 1]
            MERGE (sentence)-[:HAS_TOKEN]->(tagOccurrence2)
            MERGE (tagOccurrence1)-[r:HAS_NEXT {sentence: sentence.id}]->(tagOccurrence2))
            FOREACH (tagItem in [tag_occurrence IN $tag_occurrences WHERE tag_occurrence.is_stop = False] | 
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

    def process_entities(self, spans, text_id):
        nes = []
        for entity in spans:
            ne = {'value': entity.text, 'type': entity.label_, 'start_index': entity.start_char,
                  'end_index': entity.end_char}
            nes.append(ne)
        self.store_entities(text_id, nes)
        return nes

    def store_entities(self, document_id, nes):
        ne_query = """
            UNWIND $nes as item
            MERGE (ne:NamedEntity {id: toString($documentId) + "_" + toString(item.start_index)})
            SET ne.type = item.type, ne.value = item.value, ne.index = item.start_index
            WITH ne, item as neIndex
            MATCH (text:AnnotatedText)-[:CONTAINS_SENTENCE]->(sentence:Sentence)-[:HAS_TOKEN]->(tagOccurrence:TagOccurrence)
            WHERE text.id = $documentId AND tagOccurrence.index >= neIndex.start_index AND tagOccurrence.index < neIndex.end_index
            MERGE (ne)<-[:PARTICIPATES_IN]-(tagOccurrence)
        """
        self.execute_query(ne_query, {"documentId": document_id, "nes": nes})

    def process_coreference(self, doc, text_id):
        coref = []
        if doc._.has_coref:
            for cluster in doc._.coref_clusters:
                mention = {'from_index': cluster.mentions[-1].start_char, 'to_index': cluster.mentions[0].start_char}
                coref.append(mention)
            self.store_coref(text_id, coref)
        return coref

    def store_coref(self, document_id, corefs):
        coref_query = """
                MATCH (document:AnnotatedText)
                WHERE document.id = $documentId 
                WITH document
                UNWIND $corefs as coref  
                MATCH (document)-[*3..3]->(start:NamedEntity), (document)-[*3..3]->(end:NamedEntity) 
                WHERE start.index = coref.from_index AND end.index = coref.to_index
                MERGE (start)-[:MENTIONS]->(end)
        """
        self.execute_query(coref_query,
                           {"documentId": document_id, "corefs": corefs})

    def process_textrank(self, doc, text_id):
        keywords = []
        spans = []
        for p in doc._.phrases:
            for span in p.chunks:
                item = {"span": span, "rank": p.rank}
                spans.append(item)
        spans = filter_extended_spans(spans)
        for item in spans:
            span = item['span']
            lexme = self.nlp.vocab[span.text]
            if lexme.is_stop or lexme.is_digit or lexme.is_bracket or "-PRON-" in span.lemma_:
                continue
            keyword = {"id": span.lemma_, "start_index": span.start_char, "end_index": span.end_char}
            if len(span.ents) > 0:
                keyword['NE'] = span.ents[0].label_
            keyword['rank'] = item['rank']
            keywords.append(keyword)
        self.store_keywords(text_id, keywords)

    def create_annotated_text(self, doc, id):
        query = """MERGE (ann:AnnotatedText {id: $id})
            RETURN id(ann) as result
        """
        params = {"id": id}
        results = self.execute_query(query, params)
        return results[0]

    def process_dependencies(self, tag_occurrence_dependencies):
        tag_occurrence_query = """UNWIND $dependencies as dependency
            MATCH (source:TagOccurrence {id: dependency.source})
            MATCH (destination:TagOccurrence {id: dependency.destination})
            MERGE (source)-[:IS_DEPENDENT {type: dependency.type}]->(destination)
                """
        self.execute_query(tag_occurrence_query, {"dependencies": tag_occurrence_dependencies})

    def store_keywords(self, document_id, keywords):
        ne_query = """
            UNWIND $keywords as keyword
            MERGE (kw:Keyword {id: keyword.id})
            SET kw.NE = keyword.NE, kw.index = keyword.start_index, kw.endIndex = keyword.end_index
            WITH kw, keyword
            MATCH (text:AnnotatedText)
            WHERE text.id = $documentId
            MERGE (text)<-[:DESCRIBES {rank: keyword.rank}]-(kw)
        """
        self.execute_query(ne_query, {"documentId": document_id, "keywords": keywords})

    def build_entities_inferred_graph(self, document_id):
        extract_direct_entities_query = """
            MATCH (document:AnnotatedText)
            WHERE document.id = $documentId
            WITH document
            MATCH (document)-[*3..3]->(ne:NamedEntity)
            WHERE NOT ne.type IN ['NP', 'NUMBER', 'DATE']
            WITH ne
            MERGE (entity:Entity {type: ne.type, id:ne.value})
            MERGE (ne)-[:REFERS_TO {type: "evoke"}]->(entity)
        """

        extract_indirect_entities_query = """
            MATCH (document:AnnotatedText)
            WHERE document.id = $documentId
            WITH document
            MATCH (document)-[*3..3]->(ne:NamedEntity)<-[:MENTIONS]-(mention)
            WHERE NOT ne.type IN ['NP', 'NUMBER', 'DATE']
            WITH ne, mention
            MERGE (entity:Entity {type: ne.type, id:ne.value})
            MERGE (mention)-[:REFERS_TO {type: "access"}]->(entity)
        """
        self.execute_query(extract_direct_entities_query, {"documentId": document_id})
        self.execute_query(extract_indirect_entities_query, {"documentId": document_id})

    def extract_relationships(self, document_id, rules):
        extract_relationships_query = """
            MATCH (document:AnnotatedText)
            WHERE document.id = $documentId
            WITH document
            UNWIND $rules as rule
            MATCH (document)-[*2..2]->(verb:TagOccurrence {pos: "VBD"})
            MATCH (verb:TagOccurrence {pos: "VBD"})
            WHERE verb.lemma IN rule.verbs
            WITH verb, rule
            MATCH (verb)-[:IS_DEPENDENT {type:"nsubj"}]->(subject)-[:PARTICIPATE_IN]->(subjectNe:NamedEntity)
            WHERE subjectNe.type IN rule.subjectTypes
            MATCH (verb)-[:IS_DEPENDENT {type:"dobj"}]->(object)-[:PARTICIPATE_IN]->(objectNe:NamedEntity {type: "WORK_OF_ART"})
            WHERE objectNe.type IN rule.objectTypes
            WITH verb, subjectNe, objectNe, rule
            MERGE (subjectNe)-[:IS_RELATED_TO {root: verb.lemma, type: rule.type}]->(objectNe)
        """
        self.execute_query(extract_relationships_query, {"documentId": document_id, "rules":rules})

    def build_relationships_inferred_graph(self, document_id):
        extract_relationships_query = """
            MATCH (document:AnnotatedText)
            WHERE document.id = $documentId
            WITH document
            MATCH (document)-[*2..3]->(ne1:NamedEntity)
            MATCH (entity1:Entity)<-[:REFERS_TO]-(ne1:NamedEntity)-[r:IS_RELATED_TO]->(ne2:NamedEntity)-[:REFERS_TO]->(entity2:Entity)
            MERGE (evidence:Evidence {id: id(r), type:r.type})
            MERGE (rel:Relationship {id: id(r), type:r.type})
            MERGE (ne1)<-[:SOURCE]-(evidence)
            MERGE (ne2)<-[:DESTINATION]-(evidence)
            MERGE (rel)-[:HAS_EVIDENCE]->(evidence)
            MERGE (entity1)<-[:FROM]-(rel)
            MERGE (entity2)<-[:TO]-(rel)
        """
        self.execute_query(extract_relationships_query, {"documentId": document_id})

    def execute_query(self, query, params):
        results = []
        with self._driver.session() as session:
            for items in session.run(query, params):
                item = items["result"]
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
