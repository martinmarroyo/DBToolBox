# DBToolBox
A toolbox of wrapper functions for common database tasks

*Note: Only PostgreSQL is supported at this time.* 

## Installation
``` 
pip install git+https://github.com/martinmarroyo/DBToolBox.git 
```
## Usage

To use the DataConnectors module, ensure that you have an environment file called `.env` in the same directory as the code that calls it. For example, if you have a file called `main.py` that uses the module, ensure that there is a `.env` file in that same directory. 

Here is the minimum required structure for `.env`:
```
USER=username
PORT=5432
DB=databasename
DRIVER={PostgreSQL ANSI}
SERVER=database.server.address
PWD=password
```

### Examples

#### Querying
Querying can be as simple as the following:
``` python
from DBToolBox.DataConnectors import query_pgdb

# Run your query using SQL and get the results as a Pandas DataFrame
my_query = query_pgdb("SELECT * FROM mytable;")

# Perform operations on results using standard Pandas functions
```

#### Inserting
Inserting into a database table from a DataFrame (default schema is "public"):
``` python
from DBToolBox.DataConnectors import insert_pgdb

insert_pgdb(some_dataframe, "table_name")
```

These examples above are the simplest available operations with the convenience functions. However, if you want to work with a connection,
you can use the `get_connection` convenience function to get a database connection to work with:
``` python
from DBToolBox.DataConnectors import connect_pgdb

with connect_pgdb().cursor() as cur:
    # Do some operations with the database connection/cursor
```
