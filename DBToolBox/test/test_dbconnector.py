import pytest
from DBToolBox.DBConnector import DataConnector

@pytest.fixture
def mock_config():
    return {
        "SERVER": "database.server.address",
        "PWD": "password",
        "USER": "username",
        "PORT": "port #",
        "DB": "Database name",
        "DRIVER": "Driver name"
    }

@pytest.fixture
def mock_config_missing_user():
    return {
        "SERVER": "database.server.address",
        "PWD": "password",
        "PORT": "port #",
        "DB": "Database name",
        "DRIVER": "Driver name"
    }

@pytest.fixture
def mock_config_missing_server():
    return {
        "PWD": "password",
        "USER": "username",
        "PORT": "port #",
        "DB": "Database name",
        "DRIVER": "Driver name"
    }

@pytest.fixture
def mock_config_missing_pwd():
    return {
        "SERVER": "database.server.address",
        "USER": "username",
        "PORT": "port #",
        "DB": "Database name",
        "DRIVER": "Driver name"
    }

@pytest.fixture
def mock_config_missing_port():
    return {
        "SERVER": "database.server.address",
        "PWD": "password",
        "USER": "username",
        "DB": "Database name",
        "DRIVER": "Driver name"
    } 

# Testing DataConnector initialization

def test_config_missing_user(mock_config_missing_user):
    with pytest.raises(KeyError):
        _ = DataConnector(mock_config_missing_user)


def test_config_missing_server(mock_config_missing_server):
    with pytest.raises(KeyError):
        _ = DataConnector(mock_config_missing_server)


def test_config_missing_pwd(mock_config_missing_pwd):
    with pytest.raises(KeyError):
        _ = DataConnector(mock_config_missing_pwd)


def test_config_missing_port(mock_config_missing_port):
    with pytest.raises(KeyError):
        _ = DataConnector(mock_config_missing_port)

# Testing Postgres-specific connection
def test_pg_connection(mock_config):
    with pytest.raises(KeyError):
        connection = DataConnector(mock_config).pg_connection()