init:
	pip3 install -r requirements.txt

test:
	nosetests tests

download:
	mkdir -p ../../../dataset/deeppaulmovie && \
	if [ ! -f ../../../dataset/deeppaulmovie/ratings.txt ]; then \
		curl -L -o ../../../dataset/deeppaulmovie/ratings.txt https://raw.githubusercontent.com/JDonini/depaulmovie-recommender-system/master/dataset/Movie_DePaulMovie/ratings.txt ; \
	fi
