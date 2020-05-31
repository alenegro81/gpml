init:
	python -m pip install -U pip
	pip3 install -r requirements.txt
	export PYTHONPATH=`pwd`
	git clone "https://github.com/explosion/spaCy"
	cd spaCy
	git checkout -b v2.1.8
	pip install -r requirements.txt
	python setup.py build_ext --inplace
	cd ..
	git clone "https://github.com/huggingface/neuralcoref.git"
	cd neuralcoref
	pip install -e .
test:
	nosetests tests


	737  git clone https://github.com/explosion/spaCy
  738  cd spaCy/
  739  git checkout -b v2.1.8
  740  virtualenv .env
  741  source .env/bin/activate
  742  export PYTHONPATH=`pwd`
  743  pip install -r requirements.txt
  744  python -m pip install -U pip
  745  pip install -r requirements.txt
  746  python setup.py build_ext --inplace
  pip install -e .
  747  cd ..
  748  ls
  749  git clone "https://github.com/huggingface/neuralcoref.git"
  750  cd neuralcoref/
  751  pip install -e .
  752  cd ..
  753  python 05_spacy_cofer_schema.py
  754  cat requirements.txt
  755  pip install -r requirements.txt
  756  python 05_spacy_cofer_schema.py
  757  python3 -m spacy download en_core_web_sm
  758  python 05_spacy_cofer_schema.py
  759  history
