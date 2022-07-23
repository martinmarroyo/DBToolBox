"""A collection of mock objects for unit testing"""
import pandas as pd

COLUMN_INFO_MOCK = pd.DataFrame(
    {
        "column_name": ["testcol1", "testcol2", "testcol3"],
        "cardinality": [4, 4, 20],
        "dtype": ["int64", "object", "int64"],
        "cardinality_rating": ["LOW", "LOW", "HIGH"],
        "variable_type": ["Numeric", "Categorical", "Numeric"],
        "cardinality_pct": [0.2, 0.2, 1.0],
        "nulls": [0, 2, 0],
        "null_pct": [0.0, 0.1, 0.0],
    }
)

CONFIG_MOCK = {
    "DBC_SERVER": "database.server.address",
    "DBC_PWD": "password",
    "DBC_USER": "username",
    "DBC_PORT": "port",
    "DBC_DB": "Database.name",
    "DBC_DRIVER": "Driver.name",
    "DBC_DIALECT": "Dialect",
}
CONFIG_MOCK_DBC_URL = {"DBC_URL": "database:connection.string"}
CONNECTION_STRING_STANDARD_MOCK = (
    "Dialect+Driver.name://username:password@database.server.address:port/Database.name"
)
CONNECTION_STRING_DBC_URL_MOCK = "database:connection.string"
MOCK_DF = pd.DataFrame({"test1": [1, 2, 3, 4], "test2": ["a", "b", "c", "d"]})
CONFIG_INMEMORY_ENGINE = {"DBC_URL": "sqlite://"}
