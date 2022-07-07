import psycopg2
import pandas as pd
from psycopg2 import OperationalError
from dotenv import dotenv_values
from sqlalchemy import create_engine


class DataConnector:
    def __init__(self, config: dict):
        """ 
        Establishes the database connection based on given configuration.
        The configuration is a dictionary that must guarantee the following
        key-value pairs:
            SERVER: database.server.address
            PWD: password
            USER: username
            PORT: port #
            DB: Database name
        """
        if ('SERVER', 'PWD', 'USER', 'PORT', 'DB') not in config:
            print("Please ensure that your configuration file has all required fields")
            raise(KeyError)
        self.config = config
    
    
    def pg_connection(self):
        """ 
            Returns a psycopg2 connection to the database defined 
            in the configuration file 
        """
        try:
            connection = psycopg2.connect(
                user = self.config['USER'], 
                password = self.config['PWD'], 
                host = self.config['SERVER'],
                port = self.config['PORT'],
                dbname = self.config['DB']
            )
        except KeyError as ke:
            print(
                "One of your input parameters is incorrect.",
                "Please try again."
            )
            connection = None
            raise(ke)
        except OperationalError as oe:
            print(oe)
            connection = None
        finally:
            return connection