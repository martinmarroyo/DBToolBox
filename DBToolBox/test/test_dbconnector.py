import pytest
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
    expected = {"URL": "my.url"}
    dc = DataConnector(expected)
    assert dc.config == expected


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
    result = _validate_config({"TEST":"invalid_entry"})
    assert result == False


def test_validate_config_03():
    """
    Tests that _validate_config correctly evaluates a configuration file with a defined URL
    Pass Condition: Function returns True
    Fail Condition: Error or function returns False 
    """
    result = _validate_config(mocks.CONFIG_MOCK_URL)
    assert result == True


##----- generate_connection_string tests
def test_generate_connection_string_01():
    """
    Tests that generate_connection_string returns the expected connection
    string given a configuration file with the URL parameter supplied
    Pass Condition: The expected connection string is returned
    Fail Condition: Error or incorrect string is returned
    """
    dc = DataConnector(mocks.CONFIG_MOCK_URL)
    result = dc.generate_connection_string()
    assert result == mocks.CONNECTION_STRING_URL_MOCK
    

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
    dc.update_config(mocks.CONFIG_MOCK_URL)
    # Check that DataConnector is using the updated config
    assert dc.config == mocks.CONFIG_MOCK_URL


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
    # Set engine
    dc.set_engine()
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
    # Set engine
    dc.set_engine()
    # Insert the data
    dc.insert(mocks.MOCK_DF, table="test")
    result = dc.query("SELECT * FROM test")
    pd.testing.assert_frame_equal(result, mocks.MOCK_DF)


##----- initialize tests
def test_initialize_01():
    """
    Tests that initialize function successfully returns a DataConnector with an engine
    Pass Condition: A DataConnector is returned and it has an engine
    Fail Condition: Error or return value is not a DataConnector or it has no engine
    """
    dc = DataConnector(mocks.CONFIG_INMEMORY_ENGINE).initialize()
    assert isinstance(dc, DataConnector) and dc.engine is not None