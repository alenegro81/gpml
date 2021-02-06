init:
	pip install -U -r requirements.txt

download:
	mkdir -p ../../../dataset/yoochoose ;\
	if [ ! -f  yoochoose-data.7z ]; then \
		curl -L -o ../../../dataset/yoochoose/yoochoose-data.7z https://s3-eu-west-1.amazonaws.com/yc-rdata/yoochoose-data.7z ;\
	fi; \
	if [ ! -f yoochoose-buys.dat ]; then\
		cd ../../../dataset/yoochoose; \
		7z x yoochoose-data.7z ; \
		cd - ; \
	fi

test:
	nosetests tests
