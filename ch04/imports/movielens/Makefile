PIP=pip
PYTHON=python

init:
	$(PIP) install -r requirements.txt

download:
	mkdir -p ../../../dataset/movielens
	curl -f http://files.grouplens.org/datasets/movielens/ml-latest-small.zip -o ../../../dataset/movielens/ml-latest-small.zip
	unzip ../../../dataset/movielens/ml-latest-small.zip -d ../../../dataset/movielens/



test:
	nosetests tests
