import pytest
import os
import pandas as pd
from DBToolBox.DBConnector import DataConnector, _validate_config
from DBToolBox.test import mocks
from unittest.mock import patch, Mock

##----- Testing DataConnector initialization
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


def test_config_01():
    expected = {"DBC_URL": "my.url"}
    dc = DataConnector(expected)
    assert dc.config == expected


def test_dbconnector_env_url_config(mock_env_bash_url):
    """
    Tests that DataConnector uses environment variables when 
    the use_env flag is set to True for a DBC_URL-based configuration
    Pass Condition: The configuration is successfully set, 
            which is verified by the returned config object
    Fail Condition: Error or the configuration is None
    """
    dc = DataConnector(use_env=True)
    assert dc.config == os.environ


def test_dbconnector_env_std_config(mock_env_std_config):
    """
    Tests that DataConnector uses environment variables when 
    the use_env flag is set to True for a standard configuration
    Pass Condition: The configuration is successfully set, 
            which is verified by the returned connection string
    Fail Condition: Error or the expected connection string is not returned
    """
    # Initialize a DataConnector without an engine since we don't want to test this
    # configuration with a live connection string
    dc = DataConnector(use_env=True, no_eng=True)
    # Get connection string generated by environment variables
    result = dc.generate_connection_string()
    assert result == mocks.CONNECTION_STRING_STANDARD_MOCK


def test_db_connector_no_config():
    """
    Tests that DataConnector throws a ValueError when no configuration is found
    Pass Condition: ValueError is raised
    Fail Condition: No error is raised or ValueError is not raised
    """
    with pytest.raises(ValueError) as ve:
        dc = DataConnector()
        

def test_db_connector_env_and_sys_config():
    """
    Tests that DataConnector appends system environment variables to
    environment variables defined in a file for the configuration
    Pass Condition: Returned config includes variables from system and file environment
    Fail Condition: Error or config does not include all variables from system and file environment 
    """
    dc = DataConnector(config=mocks.CONFIG_MOCK_DBC_URL, use_env=True, no_eng=True)
    sys_env = dict(os.environ)
    expected = mocks.CONFIG_MOCK_DBC_URL
    expected.update(sys_env)
    result = dc.config
    assert result == expected


##----- _validate_config tests
def test_validate_config_01():
    """
    Tests that _validate_config correctly validates a given configuration file
    Pass Condition: Function returns True for a valid configuration file
    Fail Condition: Function returns False for a valid configuration file
    """
    result = _validate_config(mocks.CONFIG_MOCK)
    assert result == True


def test_validate_config_02():
    """
    Tests that _validate_config correctly evaluates an invalid configuration file
    Pass Condition: Function returns False to indicate invalid configuration file
    Fail Condition: Error or function returns True
    """
    result = _validate_config({"TEST": "invalid_entry"})
    assert result == False


def test_validate_config_03():
    """
    Tests that _validate_config correctly evaluates a configuration file with a defined DBC_URL
    Pass Condition: Function returns True
    Fail Condition: Error or function returns False
    """
    result = _validate_config(mocks.CONFIG_MOCK_DBC_URL)
    assert result == True


##----- generate_connection_string tests
def test_generate_connection_string_01():
    """
    Tests that generate_connection_string returns the expected connection
    string given a configuration file with the DBC_URL parameter supplied
    Pass Condition: The expected connection string is returned
    Fail Condition: Error or incorrect string is returned
    """
    dc = DataConnector(mocks.CONFIG_MOCK_DBC_URL)
    result = dc.generate_connection_string()
    assert result == mocks.CONNECTION_STRING_DBC_URL_MOCK


def test_generate_connection_string_02():
    """
    Tests that generate_connection_string returns the expected connection
    string given a configuration file with standard database parameters
    Pass Condition: The expected connection string is returned
    Fail Condition: Error or incorrect string is returned
    """
    dc = DataConnector(mocks.CONFIG_MOCK)
    result = dc.generate_connection_string()
    assert result == mocks.CONNECTION_STRING_STANDARD_MOCK


##----- update_config tests
def test_update_config_01():
    """
    Tests that update_config correctly updates the configuration of the
    DataConnector if it is valid
    Pass Condition: The DataConnector configuration matches the expected output
    Fail Condition: Error or the incorrect configuration is returned
    """
    # Create DataConnector with original config
    dc = DataConnector(mocks.CONFIG_MOCK)
    # Update the config
    dc.update_config(mocks.CONFIG_MOCK_DBC_URL)
    # Check that DataConnector is using the updated config
    assert dc.config == mocks.CONFIG_MOCK_DBC_URL


def test_update_config_02():
    """
    Tests that update_config throws a KeyError if
    Pass Condition: The DataConnector configuration matches the expected output
    Fail Condition: Error or the incorrect configuration is returned
    """
    with pytest.raises(KeyError) as ke:
        dc = DataConnector(mocks.CONFIG_MOCK)
        dc.update_config({"TEST": "invalid_entry"})


##----- query tests
@patch("pandas.read_sql", return_value=mocks.MOCK_DF)
def test_query_01(read_sql_mock: Mock):
    """
    Tests that query function returns a DataFrame as expected
    Pass Condition: The expected DataFrame is returned
    Fail Condition: Error or does not return the expected DataFrame
    """
    # Set up DataConnector
    dc = DataConnector(mocks.CONFIG_INMEMORY_ENGINE)
    # Run query
    result = dc.query("SELECT * FROM test")
    # Compare result to expected value
    read_sql_mock.assert_called_once()
    pd.testing.assert_frame_equal(result, mocks.MOCK_DF)


##----- insert tests
def test_insert_01():
    """
    Tests that the insert function successfully
    inserts a given DataFrame to the configured Database
    Pass Condition: Expected data is returned from Database after insertion
    Fail Condition: Error or data returned is not what is expected
    """
    # Set up DataConnector
    dc = DataConnector(mocks.CONFIG_INMEMORY_ENGINE)
    # Insert the data
    dc.insert(mocks.MOCK_DF, table="test")
    result = dc.query("SELECT * FROM test")
    pd.testing.assert_frame_equal(result, mocks.MOCK_DF)


##----- set_engine tests
def test_set_engine():
    # Set up DataConnector without engine
    dc = DataConnector(mocks.CONFIG_INMEMORY_ENGINE, no_eng=True)
    # Set engine
    result = dc.set_engine() # Returns our connection DBC_URL
    assert result == mocks.CONFIG_INMEMORY_ENGINE["DBC_URL"]


##----- dispose_engine tests
def test_dispose_engine(mock_env_bash_url):
    """
    Tests that dispose_engine successfully disposes the
    current engine
    Pass Condition: Returns 0 as status code
    Fail Condition: Error or returns 1 as status code
    """
    # Initialize DataConnector with engine
    dc = DataConnector(use_env=True)
    # Check the return value of disposing the engine
    result = dc.dispose_engine()
    assert result == 0