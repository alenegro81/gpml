This directory contains the code for importing of data that are used in the chapter 5 of the book.

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


For this chapter we're using the [Retailrocket recommender system dataset](https://www.kaggle.com/retailrocket/ecommerce-dataset) available from Kaggle.  From this dataset we need only the `events.csv` file (you can download the whole dataset), download it and put it into directory `../../../dataset/retailrocket/`.

Importing of data is performed with following command:

```sh
python import_retail_rocket_ui.py

```
