init:
	pip install -U -r requirements.txt

get_data:
	if [ ! -f  yoochoose-data.7z ]; then \
		curl -L -o yoochoose-data.7z https://s3-eu-west-1.amazonaws.com/yc-rdata/yoochoose-data.7z ;\
	fi; \
	if [ ! -f yoochoose-buys.dat ]; then \
		7z x yoochoose-data.7z; \
	fi

test:
	nosetests tests
