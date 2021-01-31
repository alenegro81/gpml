This directory contains the code for importing of data that are used in the chapter 6 of the book.

## Install dependencies

To install all necessary dependencies just run the:

```sh
make
```

or 

```sh
pip install -r requirements.txt
```


## Download & import the dataset


For this chapter we're using the [Youchoose dataset](https://s3-eu-west-1.amazonaws.com/yc-rdata/yoochoose-data.7z). To download & unpack it just type (you need to have 7Zip binary installed):

```sh
make get_data
```

Importing of data is performed with following command (you may need to update Neo4j username & password in the file):

```sh
python import_yoochoose.py path_to_youchoose_dataset

```

If you used `make get_data`, use `.` for `path_to_youchoose_dataset`
