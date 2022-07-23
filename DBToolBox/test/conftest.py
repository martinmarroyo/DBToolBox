import pandas as pd
import pytest

@pytest.fixture(scope="function")
def mock_config():
    return {
        "SERVER": "database.server.address",
        "PWD": "password",
        "USER": "username",
        "PORT": "port",
        "DB": "Database.name",
        "DRIVER": "Driver.name",
        "DIALECT": "Dialect"
    }


@pytest.fixture(scope="function")
def mock_config_missing_user():
    return {
        "SERVER": "database.server.address",
        "PWD": "password",
        "PORT": "port",
        "DB": "Database.name",
        "DRIVER": "Driver.name"
    }


@pytest.fixture(scope="function")
def mock_config_missing_server():
    return {
        "PWD": "password",
        "USER": "username",
        "PORT": "port",
        "DB": "Database.name",
        "DRIVER": "Driver.name"
    }


@pytest.fixture(scope="function")
def mock_config_missing_pwd():
    return {
        "SERVER": "database.server.address",
        "USER": "username",
        "PORT": "port #",
        "DB": "Database.name",
        "DRIVER": "Driver.name"
    }


@pytest.fixture(scope="function")
def mock_config_missing_port():
    return {
        "SERVER": "database.server.address",
        "PWD": "password",
        "USER": "username",
        "DB": "Database.name",
        "DRIVER": "Driver.name"
    } 


@pytest.fixture(scope="function")
def mock_config_url():
    return {
        "URL": "database:connection.string"
    }