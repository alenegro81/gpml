import spacy
nlp = spacy.load('en_core_web_sm')
doc = nlp(u"Marie Curie received the Nobel Prize in Physics") #A
options = {"collapse_phrases": True} #B
spacy.displacy.serve(doc, style='dep', options=options) #C
