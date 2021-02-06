# Code Repository for Graph-Powered Machine Learning book

# Introduction

This repository contains the code of the [Graph-Powered Machine Learning](https://www.manning.com/books/graph-powered-machine-learning) book. 
Chapters contain only necessary code snippets, and here is the full code of examples, and much more. 
Whenever possible the basic code has been extended suggesting more complex implementations, for instance proposing a parallel version or different queries. 
In this way the code offers multiple levels of complexity. 
According to his or her own experience the reader can select the complexity he or she prefers. 

# Running the code examples

In order to run the examples in this code repository you need to have Neo4j installed on your machine. 
Refer to the section [Neo4j Installation](#neo4j-installation) for some suggestion on how to find the guides for properly install the right version.

# Create your python environment

All code examples were tested with Python 3 (although they may work with Python 2 as well) - in all examples `python` refers to the Python 3 binary - use `python3` if you have both Python 2 & 3 installed. 

As best practice in Python, it is better to use a virtual environment where all the necessary dependencies will be installed
without affecting the system installation. So before starting the code review create and activate your virtual environment 
by running the following command in the project directory after the `git clone` (feel free to rename the virtual environment directory as you wish): 

```sh
python -m venv .venv
```

Once created it needs to be activated

```sh
source .venv/bin/activate
```

You also need to set the `PYTHONPATH` environment variable, so Python will able to find auxiliary libraries (in the top-level directory of the project):

```sh
export PYTHONPATH="$(pwd):$PYTHONPATH"
```

The commands above run on Linux/Unix sheels for more details and for other operating systems refers to the Python documentation available [here](https://docs.python.org/3/library/venv.html).
 
As reminder the environment must be activated everytime a new shel is opened. 

# Code organization and generic guidelines

The code is organized in chapters, so that would be easy to find the code related to the chapter you are reading. 
Wherever possible, it has been split in "import" and "analysis". The first provides the scripts for importing the data converting it in the desired graph data structure.
The second contains the scripts necessary to create the models and use them. 

Each directory contains a `requirements.txt` file and a `Makefile`. In order to install all the dependencies just run the following command:

```sh
make
```

All the necessary dependencies will be installed. If you don't have the `make` command just import the dependencies manually. 
For further details on how to install modules on python refers to the [related documentation](https://docs.python.org/3/installing/index.html).

Each script (the python scripts at least, for the Java code of chapter 5 refers to the specific `README.md`)
requires to specify the username (default: `neo4j`) and the password (default: `password`), and URI (default: `bolt://localhost:7687`) to connect to Neo4j.  These & other parameters could be different ways (in order of priority, if some parameter is missing, then the next method will be tried, or default value will be used)

* by specifying them in the command line.  In this case invocation will look like this:

```sh
python name_of_the_script.py [INSERT ARGUMENTS HERE]
```

Currently following parameters are supported (execute script with `-h` option):
  * `-u` is used to specify Neo4j username
  * `-p` is used to specify Neo4j password
  * `-b` is used to specify Neo4j URI
  * `-s` is used to specify location of the source dataset (if it's used)

* by specifying them as environment variables:
  * `NEO4J_USER` is used to specify Neo4j username
  * `NEO4J_PASSWORD` is used to specify Neo4j password
  * `NEO4J_URI` is used to specify Neo4j URI

* by specifying parameters in the `neo4j` section of the `config.ini` file at the top of the project.  Besides username, password, URI, it may also contain other parameters that will be passed as `config` parameter of the [GraphDatabase.driver](https://github.com/neo4j/neo4j-python-driver/blob/4.3/neo4j/__init__.py#L123) call.


Some scripts, in particular the importing scripts, require also the path where the source dataset resides - this could be done by specifying the path via `-s` command-line parameter.
Whenever possible the `make` command will also download the datasets, in other cases it is necessary to download the dataset manually and then specify the path during the run. 
There are some defaults that make this not necessary but in any case it is possible to specify the path in the following way:

```sh
python name_of_the_script.py [INSERT ARGUMENTS HERE]
```


## Disclaimer

It is worth mentioning that, as for the book, the code has not meant to provide only basic examples.
In most of the cases, the datasets used in the code are real. Which means that the volume of data is sinigficant enough. 
On one side this allows the reader to test real use cases instead of toy examples (that would be hardlymigrated in production without significant changes).
On the other side this causes the reader to face with real problem during the execution of the code: not enough space on disk, 
issues with thr RAM and hours to execute the code.

I firmly believe that this is also an important part of the learning process. 
In real scenarios, practitioners have to figure out how to import multiple GB of data and how to process large graphs. 
If you encounter problems in running some of the examples you can decide to either using a different machine or reduce the dataset size (in some cases you need to shuffle the dataset's rows). 


# Neo4j Installation

All the scripts work perfectly with the community and the enterprise edition of Neo4j. 
Moreover, it is possible to use the [Neo4j desktop](https://neo4j.com/download/) to manage the Neo4j instances. 
The book has been written during the transition from 3.5.x to 4.x so the code has been all updated to run on the version 4.x.
It introduced some changes, like the variable binding and the multi relationships syntax in Cypher that make the code incompatible with the previous version 3.5.x. 

You can find all the instruction for downloading and installing Neo4j in the way you prefer here:
* https://neo4j.com/download/
* https://neo4j.com/docs/

## Using Docker Compose

You can get completely working setup in the matter of seconds by using the Docker & [docker-compose](https://docs.docker.com/compose/).  After it's installed, change to the `docker-compose` folder, and execute (add `-d` if you want to start it in background):

```sh
docker-compose up
```

this will bring you fully functional Neo4j setup, with all plugins installed.  After that you can point browser to the http://localhost:7474 and login into Neo4j browser using the name `neo4j` and password: `password`.


# Useful tricks when working with Neo4j

Sometimes you may need to completely cleanup database.  You can do it using the functions available in the [APOC library](https://neo4j.com/developer/neo4j-apoc/) that you can install into your database (via UI, or other way): 

* Delete everything:

```
CALL apoc.periodic.iterate('MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000})
```

* Drop all constraints:

```
CALL apoc.schema.assert({}, {})
```
