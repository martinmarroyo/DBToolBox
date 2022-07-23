"""A collection of mock objects for unit testing"""
import pandas as pd

COLUMN_INFO_MOCK = pd.DataFrame({
    'column_name': ['testcol1','testcol2','testcol3'],
    'cardinality': [4, 4, 20],
    'dtype': ['int64', 'object','int64'],
    'cardinality_rating': ['LOW','LOW', 'HIGH'],
    'variable_type': ['Numeric', 'Categorical', 'Numeric'],
    'cardinality_pct': [0.2, 0.2, 1.0],
    'nulls': [0, 2, 0],
    'null_pct': [0.0, 0.1, 0.0]
})

CONFIG_MOCK = {
    "SERVER": "database.server.address",
    "PWD": "password",
    "USER": "username",
    "PORT": "port",
    "DB": "Database.name",
    "DRIVER": "Driver.name",
    "DIALECT": "Dialect"
}
CONFIG_MOCK_URL = {
    "URL": "database:connection.string"
}
CONNECTION_STRING_STANDARD_MOCK = "Dialect+Driver.name://username:password@database.server.address:port/Database.name"
CONNECTION_STRING_URL_MOCK = "database:connection.string"
MOCK_DF = pd.DataFrame({
    "test1": [1,2,3,4],
    "test2": ["a", "b", "c", "d"]
})
CONFIG_INMEMORY_ENGINE = {"URL": "sqlite://"}