import psycopg2
from sqlalchemy import create_engine
import datetime
import os
import pandas as pd
from dotenv import dotenv_values

# Load db configuration
config = dotenv_values(".env")


def db_connection(user: str, password: str, host: str, port: int, dbname: str):
    """
    Returns a Connection object for the
    specified server(Nursery,Greenhouse,Garden)
    """
    try:
        connection = psycopg2.connect(
            user=user, password=password, host=host, port=port, dbname=dbname
        )
        return connection
    except KeyError:
        print("One of your input parameters is incorrect.")
        print("Please try again.")
        raise
    except:
        print("Something went wrong. Please try again.")
        raise


def get_alchemy_engine(server_name: str):
    """
    Returns a SQLAlchemy engine that
    is connected to the provided @server_name
    """
    try:
        address = (
            f"postgresql+psycopg2://{config['USER']}"
            + f":{config['PWD']}@{server_name}"
            + f":{config['PORT']}/{config['DB']}"
        )
        engine = create_engine(address, echo=False)
        return engine
    except Exception as err:
        print(f"Error occurred during engine creation: {str(err)}")
        raise


def get_alchemy_connection(
    engine=None, server_name=None, stream=False, max_row_buffer=100
):
    """
    Returns a SQLAlchemy connection for the given engine. If one
    is not provided, an engine is generated to return a connection.

    If @stream is set to True, the connection will establish a server-side
    cursor and stream the results of the query.

    @max_row_buffer sets the number of pre-fetched rows that are saved into
    the buffer as the query is processed (max is 1000)
    """
    try:
        if engine is not None:
            conn = engine.connect()
        else:
            try:
                conn = get_alchemy_engine(server_name).connect()
            except Exception as e:
                print(f"Error occurred while getting engine: {str(e)}")
                raise
        if stream:
            conn = conn.execution_options(
                stream_results=stream, max_row_buffer=max_row_buffer
            )
        return conn
    except Exception as err:
        print(f"Error occurred while getting a connection: {str(err)}")
        raise


def run_query(
    query,
    connection,
    params=None,
    parse_dates=None,
    chunksize=None,
    index_col=None,
    columns=None,
):
    """
    Runs a given query against a given connection and
    returns the results in a DataFrame. Allows for all
    of the parameters that the Pandas read_sql function
    takes (see
    https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html)
    """
    # Check for valid query input
    if (
        len(query) < len("SELECT * FROM")
        or not isinstance(query, str)
        or query.isspace()
    ):
        raise ValueError("Invalid Query")
    try:
        # Run query and put results in DataFrame
        df = pd.read_sql(
            query,
            connection,
            params=params,
            parse_dates=parse_dates,
            chunksize=chunksize,
            index_col=index_col,
            columns=columns,
        )
        return df
    except ValueError:
        print("Please use a valid query")
        raise
    except Exception as e:
        print(f"Something went wrong. Please try again. Error message {str(e)}")
        raise


def db_insertion(
    data: pd.DataFrame,
    server_name: str,
    table_name: str,
    schema_name: str = "public",
    engine=None,
    chunksize=1000,
    method="multi",
    index: bool = False,
    if_exists: str = "append",
    dtype: dict = None,
):
    """
    Base function for inserting/appending
    data to a specified @server_name
    """
    if engine is None:
        engine = get_alchemy_engine(server_name)
    data.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        index=index,
        if_exists=if_exists,
        dtype=dtype
        # Adding chunksize and multi method to speed up inserts
        ,
        chunksize=chunksize,
        method=method,
    )


def connect_pgdb():
    """
    Returns a Connection to the Postgres database
    """
    connection = db_connection(
        config["USER"], config["PWD"], config["SERVER"], config["PORT"], config["DB"]
    )
    return connection


def get_alchemy_engine_pgdb():
    """
    Returns a SQLAlchemy engine that
    is connected to the Postgres database
    """
    try:
        address = (
            f"postgresql+psycopg2://{config['USER']}"
            + f":{config['PWD']}@{config['SERVER']}"
            + f":{config['PORT']}/{config['DB']}"
        )
        engine = create_engine(address, echo=False)
        return engine
    except Exception as err:
        print(f"Error occurred during engine creation: {str(err)}")
        raise


def get_alchemy_connection_pgdb(stream=False, max_row_buffer=100):
    """
    Returns a SQLAlchemy connection to the Postgres for the given engine. If one
    is not provided, an engine is generated to return a connection.

    If @stream is set to True, the connection will establish a server-side
    cursor and stream the results of the query.

    @max_row_buffer sets the number of pre-fetched rows that are saved into
    the buffer as the query is processed (max is 1000)
    """
    try:
        conn = get_alchemy_engine(config["SERVER"]).connect()
        if stream:
            conn = conn.execution_options(
                stream_results=stream, max_row_buffer=max_row_buffer
            )
        return conn
    except Exception as err:
        print(f"Error occurred while getting a connection: {str(err)}")
        raise


def query_pgdb(
    query,
    connection=None,
    stream=False,
    max_row_buffer=100,
    params=None,
    parse_dates=None,
    chunksize=None,
    index_col=None,
    columns=None,
):
    """
    Runs a query in the Postgres database and returns the
    results in a DataFrame. If no connection is
    given, then a connection to the database is
    opened up.

    @stream: If set to true, establishes a server-side
             cursor to run the query on the server and
             stream the results (good for reducing memory consumption)
    @max_row_buffer: If streaming, this sets the number of rows to hold in
                     the buffer. The max is 1000.
    @params: Takes a dictionary of parameters to pass into the query

    All other params are part of the read_sql Pandas function.
    See https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
    """
    try:
        # Get connection
        conn = connection
        if conn is None:
            if stream:
                # Get connection with a buffered stream
                conn = get_alchemy_connection(
                    server_name=config["SERVER"],
                    stream=stream,
                    max_row_buffer=max_row_buffer,
                )
            else:
                conn = get_alchemy_connection(server_name=config["SERVER"])
        # Run the query
        df = run_query(query, conn, params=params)
        # Close our connection
        conn.close()
        # Return the DataFrame
        return df
    except Exception as e:
        print(f"Error occurred while querying the database: {str(e)}")


def insert_pgdb(
    data: pd.DataFrame,
    table: str,
    schema: str = "public",
    engine=None,
    chunksize=1000,
    method="multi",
    index=False,
    if_exists: str = "append",
    dtype: dict = None,
):
    """
    Inserts/Appends data into the given
    table in the given @schema. @schema
    must already exist in database.
    """
    try:
        db_insertion(
            data=data,
            server_name=config["SERVER"],
            table_name=table,
            schema_name=schema,
            engine=engine,
            chunksize=chunksize,
            method=method,
            index=index,
            if_exists=if_exists,
            dtype=dtype,
        )
    except Exception as e:
        print(f"Error occurred while trying to insert to Nursery: {str(e)}")
        raise
