init:
	pip3 install -U -r requirements.txt
	python3 -m spacy download en_core_web_sm
test:
	nosetests tests

download:
	mkdir -p ../dataset/masc/
	if [ ! -f ../dataset/masc/masc_word_sense_sentence_corpus.V1.0.tar.gz ]; then curl -L -o '../dataset/masc/masc_word_sense_sentence_corpus.V1.0.tar.gz' 'http://academiccommons.columbia.edu/download/fedora_content/download/ac:175064/CONTENT/masc_word_sense_sentence_corpus.V1.0.tar.gz'; fi
	if [ -f ../dataset/masc/masc_word_sense_sentence_corpus.V1.0.tar.gz ] && [ ! -d masc_word_sense_sentence_corpus.V1.0 ]; then cd ../dataset/masc/; tar zxf masc_word_sense_sentence_corpus.V1.0.tar.gz; cd -; fi
