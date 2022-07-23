import pandas as pd
import pytest

@pytest.fixture(scope="function")
def mock_config():
    return {
        "DBC_SERVER": "database.server.address",
        "DBC_PWD": "password",
        "DBC_USER": "username",
        "DBC_PORT": "port",
        "DBC_DB": "Database.name",
        "DBC_DRIVER": "Driver.name",
        "DBC_DIALECT": "Dialect",
    }


@pytest.fixture(scope="function")
def mock_config_missing_user():
    return {
        "DBC_SERVER": "database.server.address",
        "DBC_PWD": "password",
        "DBC_PORT": "port",
        "DBC_DB": "Database.name",
        "DBC_DRIVER": "Driver.name",
    }


@pytest.fixture(scope="function")
def mock_config_missing_server():
    return {
        "DBC_PWD": "password",
        "DBC_USER": "username",
        "DBC_PORT": "port",
        "DBC_DB": "Database.name",
        "DBC_DRIVER": "Driver.name",
    }


@pytest.fixture(scope="function")
def mock_config_missing_pwd():
    return {
        "DBC_SERVER": "database.server.address",
        "DBC_USER": "username",
        "DBC_PORT": "port #",
        "DBC_DB": "Database.name",
        "DBC_DRIVER": "Driver.name",
    }


@pytest.fixture(scope="function")
def mock_config_missing_port():
    return {
        "DBC_SERVER": "database.server.address",
        "DBC_PWD": "password",
        "DBC_USER": "username",
        "DBC_DB": "Database.name",
        "DBC_DRIVER": "Driver.name",
    }


@pytest.fixture(scope="function")
def mock_config_url():
    return {"DBC_URL": "database:connection.string"}


@pytest.fixture
def mock_env_bash_url(monkeypatch):
    monkeypatch.setenv("DBC_URL", "sqlite://")


@pytest.fixture
def mock_env_std_config(monkeypatch):
    monkeypatch.setenv("DBC_SERVER", "database.server.address")
    monkeypatch.setenv("DBC_PWD", "password")
    monkeypatch.setenv("DBC_USER", "username")
    monkeypatch.setenv("DBC_PORT", "port")
    monkeypatch.setenv("DBC_DB", "Database.name")
    monkeypatch.setenv("DBC_DRIVER", "Driver.name")
    monkeypatch.setenv("DBC_DIALECT", "Dialect")