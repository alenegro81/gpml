import stanfordnlp

class BasicNLP(object):

    def __init__(self, language):
        stanfordnlp.download(download_label=language, resource_dir='stanfordnlp_resources', confirm_if_exists=False)

    def tokenize(self, text):
        nlp = stanfordnlp.Pipeline()  # This sets up a default neural pipeline in English
        doc = nlp(text)
        i = 1
        for sentence in doc.sentences:
            print("--------Sentence ", i,  "-----------")
            i += 1
            for token in sentence.tokens:
                print(token.index, "-", token.text, "-", token.words[0].lemma)

if __name__ == '__main__':
    basic_nlp = BasicNLP(language="en")
    basic_nlp.tokenize("Barack Obama was born in Hawaii.  He was elected president in 2008.")
