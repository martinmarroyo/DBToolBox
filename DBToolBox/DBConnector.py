import psycopg2
import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine

def _validate_config(config: dict) -> bool:
        """Tests that a given configuration is valid"""
        if "URL" in config:
            return True
        REQUIRED = set(['SERVER', 'PWD', 'USER', 'PORT', 'DB', 'DRIVER', 'DIALECT'])
        return REQUIRED.issubset(set(config))


class DataConnector:
    
    default_config = dotenv_values(".env")
    
    def __init__(self, config: dict = default_config):
        """ 
        Establishes the database connection based on given configuration.
        The configuration is a dictionary that must provide a connection string
        using "URL" as the key, or guarantee the following key-value pairs:
            SERVER: database.server.address
            PWD: password
            USER: username
            PORT: port #
            DB: Database name
            DIALECT: The name of the RDBMS (e.g. Postgresql, MySQL, SQLServer, etc.)
            DRIVER: The database driver being used (e.g. psycopg2)
        It is assumed that there will be a file called ".env" placed in the root
        directory (relative to where the DataConnector is initialized.) You can either
        have the file setup there or pass in the path to a configuration file that has
        the required key-value pairs
        """
        valid_config = _validate_config(config) 
        if not valid_config:
            print("Please ensure that your configuration file has all required fields")
            raise(KeyError)
        self.config = config
    

    def set_engine(self, connection_string:str=None):
        """Initializes a SQLAlchemy Engine to the DataConnector based on the current configuration"""
        try:
            address = connection_string 
            if connection_string is None:
                address = self.generate_connection_string()
            print("Engine address", address)   
            engine = create_engine(address, echo=False)
            self.engine = engine
        except Exception as err:
            print(f"Error occurred during engine creation: {str(err)}")
            raise
        finally:
            return None


    def initialize(self):
        """A helper function to initialize the Database engine based on the current configuration"""
        self.set_engine()
        return self


    def generate_connection_string(self, config:dict = None):
        """ 
        Returns a connection string to use for SQLAlchemy engine based on the 
        current config for the DataConnector or a given config dictionary
        """
        conf = self.config if config is None else config
        # Return the connection string given in the URL variable (if present)
        try:
            if "URL" in conf:
                print(conf["URL"])
                return conf["URL"]
            connection_string = (
                f"{conf['DIALECT']}+{conf['DRIVER']}://{conf['USER']}"
                f":{conf['PWD']}@{conf['SERVER']}"
                f":{conf['PORT']}/{conf['DB']}"
            )
            return connection_string
        except KeyError as err:
            print(
                "A valid connection string could not be formed."
                "Please check your configuration and try again."
            )
            raise
    

    def update_config(self, config: dict) -> None:
        """
        Updates the existing configuration for the DataConnector instance
        """
        valid = _validate_config(config)
        if valid:
            self.config = config
            return None
        print("The provided configuration is invalid. Please try again.")
        raise KeyError


    def query(
        self, 
        query: str, 
        params = None,
        parse_dates: str = None,
        chunksize: int = None,
        index_col: str = None,
        columns: list = None
    ) -> pd.DataFrame:
        """
        Runs the given SQL query with optional parameters and returns
        the result as a Pandas DataFrame
        """
        try:
            # Run query and put results in DataFrame
            df = pd.read_sql(
                sql = query,
                con = self.engine,
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
            print(f"Something went wrong. Please try again. Error message: {str(e)}")
            raise


    def insert(
        self, 
        data: pd.DataFrame, 
        table: str, 
        schema: str = None,
        index: bool = False,
        if_exists: str = "replace",
        dtype = None,
        chunksize: int = None,
        method: str = "multi"
    ) -> pd.DataFrame:
        """
        Runs the given SQL query with optional parameters and returns
        the result as a Pandas DataFrame
        """
        if self.engine:
            data.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                index=index,
                if_exists=if_exists,
                dtype=dtype,
                # Adding chunksize and multi method to speed up inserts
                chunksize=chunksize,
                method=method,
            )
            return None
        print("Missing engine: please set the engine and try again")
        raise KeyError


    @property
    def get_engine(self):
        """Returns the engine that was configured to the DataConnector"""
        if self.engine:
            return self.engine
        print("No engine has been configured")
        return None
    