# Import Movielens data

## Install required libraries
The import scripts requires some specific library such as imdbpy which allows to access to ImDB (https://www.imdb.com/) api.
To install what is necessary run:

```sh
make
```

## Download the data source
The Makefile contains also the command to download the necessary data sources.
Run:

```sh
make source
```

You can also download it manually from [project's site](http://files.grouplens.org/datasets/movielens/ml-latest-small.zip)

The default location is in the home of this code repository in the directory datasets
(eventually in the `movielens` subdirectory). 


## Run the import

```sh
python import_movielens.py -u <neo4j username> -p <password>  -b <bolt uri> -s <source directory>
```
 
If you used the makefile for downloading the directory you don't need to specify the datasource. 
The simple version takes a while to be completed. I recommend to run the parallel version as follows:

```sh
python import_movielens_parallel.py -u <neo4j username> -p <password>  -b <bolt uri> -s <source directory>
```

## Note during the import

Note that IMDB imposes some constraints for the access to its API. Due to this, if the machine is powerful enough 
it can happen that it will start rejecting the requests. It is perfectly normal. 

After the chapter has been released the full version of the IMDB has been released [here](https://www.imdb.com/interfaces/).

In the future I'll make some changes in order to load from files instead.