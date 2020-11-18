import spacy
import sys

class BasicNLP(object):

    def __init__(self, language):
        spacy.prefer_gpu()

    def tokenize(self, text):
        nlp = spacy.load("en_core_web_sm")
        doc = nlp(text)
        i = 1
        for sentence in doc.sents:
            print("-------- Sentence ", i,  "-----------")
            i += 1
            for token in sentence:
                print(token.idx, "-", token.text, "-", token.lemma_, "-", token.tag_)

if __name__ == '__main__':
    basic_nlp = BasicNLP(language="en")
    if len(sys.argv) > 1:
        sentence = sys.argv[1]
    else:
        sentence = "Marie Curie received the Nobel Prize in Physic in 1903. She became the first woman to win the prize."
    basic_nlp.tokenize(sentence)
