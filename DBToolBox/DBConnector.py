"""A wrapper class around SQLAlchemy to make general database operations easier to write"""
import os
import psycopg2
import pandas as pd
from dotenv import dotenv_values
from sqlalchemy import create_engine


def _validate_config(config: dict) -> bool:
    """Tests that a given configuration is valid"""
    if "DBC_URL" in config:
        return True
    REQUIRED = set(["DBC_SERVER", "DBC_PWD", "DBC_USER", "DBC_PORT", "DBC_DB", "DBC_DRIVER", "DBC_DIALECT"])
    return REQUIRED.issubset(set(config))


class DataConnector:

    default_config = dotenv_values(".env")

    def __init__(
        self, 
        config: dict = default_config, 
        use_env = False, 
        no_eng = False
        ):
        """
        Description:

        Establishes a database engine based on a given configuration.
        The configuration can be defined as a dictionary, using system
        environment variables, or a combination of the two.
        There are two ways that an engine configuration can be defined:
        1. By providing a connection string via the DBC_URL parameter in
           your configuration, or
        2. Ensuring the following variables are specific in your configuration:
            DBC_SERVER: database.server.address
            DBC_PWD: password
            DBC_USER: username
            DBC_PORT: port #
            DBC_DB: Database name
            DBC_DIALECT: The name of the RDBMS (e.g. Postgresql, MySQL, SQLServer, etc.)
            DBC_DRIVER: The database driver being used (e.g. psycopg2)
        
        By default, the initialization method checks for a file in the root directory
        called ".env". If it finds that file, then it will validate it to ensure the 
        required parameters are supplied and will use it to start the database engine.

        To use system environment variables, set `use_env=True` (it is False by default).
        If both a configuration and environment variables are set, then all of those
        variables will be combined into the DataConnector's configuration.

        You can also specify whether the initialization process starts an engine
        or not using the `no_eng` flag should you desire to do so.
        
        Usage:
        
        # Using defaults (user ensures that .env file is in root directory)
        dc = DataConnector() # Initializes engine based on configuration defined in .env file
        
        # Using a supplied dictionary
        dc = DataConnector(config=my_dict) # Initializes engine based on configuration defined in my_dict
        
        # Using environment variables (user ensures that system environment variables are set prior to call)
        dc = DataConnector(use_env=True)
        
        # Using both a dictionary config and system variables
        dc = DataConnector(config=my_dict, use_env=True) # Combines all variables into a single config
        
        # Initializing without starting the database engine
        dc = DataConnector(no_eng=True)
        """
        conf = config
        # Throw an error if there is no configuration either in .env or the environment
        if not conf:
            if not use_env:
                print("No configuration found! Please provide a configuration and try again")
                raise (ValueError)
            conf = dict(os.environ)
        # If we have a configuration file and defined environment variables, then combine them
        elif conf and use_env:
            conf.update(dict(os.environ))
        # 
        valid_config = _validate_config(conf)
        if not valid_config:
            print("Please ensure that your configuration file has all required fields")
            raise (KeyError)
        # Set the configuration
        self.config = conf
        # Initialize the Database engine
        if not no_eng:
            try:
                self.set_engine()
            except Exception as e:
                print(
                    f"{self.set_engine()} is an invalid connection string." 
                    "Please check your configuration and try again."
                    f"Error Message: {e}"
                )
                raise


    def set_engine(self, connection_string: str = None):
        """Initializes a SQLAlchemy Engine to the DataConnector based on the current configuration"""
        try:
            url = connection_string
            if url is None:
                url = self.generate_connection_string()
            # If there is already an engine, dispose of it before creating a new one
            self.dispose_engine()
            engine = create_engine(url, echo=False)
            self.engine = engine
            print("Engine is set! Engine URL:", url)
        except Exception as err:
            print(f"Error occurred during engine creation: {str(err)}")
            raise
        finally:
            return url

    def dispose_engine(self):
        """
        Disposes the current engine. 
        Returns 0 if successful; 1 if no engine was found
        """
        try:
            self.engine.dispose()
            print("Engine successfully disposed")
            return 0
        except AttributeError as ae:
            print(f"No engine found: {ae}")
            return 1


    def generate_connection_string(self, config: dict = None):
        """
        Returns a connection string to use for SQLAlchemy engine based on the
        current config for the DataConnector or a given config dictionary
        """
        conf = self.config if config is None else config
        # Return the connection string given in the URL variable (if present)
        try:
            if "DBC_URL" in conf:
                return conf["DBC_URL"]
            connection_string = (
                f"{conf['DBC_DIALECT']}+{conf['DBC_DRIVER']}://{conf['DBC_USER']}"
                f":{conf['DBC_PWD']}@{conf['DBC_SERVER']}"
                f":{conf['DBC_PORT']}/{conf['DBC_DB']}"
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
        params=None,
        parse_dates: str = None,
        chunksize: int = None,
        index_col: str = None,
        columns: list = None,
    ) -> pd.DataFrame:
        """
        Runs the given SQL query with optional parameters and returns
        the result as a Pandas DataFrame
        """
        try:
            # Run query and put results in DataFrame
            df = pd.read_sql(
                sql=query,
                con=self.engine,
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
        dtype=None,
        chunksize: int = None,
        method: str = "multi",
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


    @property
    def get_config(self):
        """Returns the configuration for the DataConnector"""
        if self.config:
            return self.config
        print("No configuration found")
        return None
