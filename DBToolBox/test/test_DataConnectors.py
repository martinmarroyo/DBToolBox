from psycopg2 import OperationalError
import pytest
import DBToolBox.DataConnectors as dc

@pytest.fixture
def mock_config():
    return {
        "SERVER": "database.host.name",
        "DB": "databasename",
        "PORT": 1234,
        "USER": "username",
        "PWD": "password"
    }

# Test db_connection
def test_db_connection_config(mock_config):
    """ Tests to ensure appropriate error handling 
        in case of incorrect configuration values
    """
    with pytest.raises(OperationalError):
        _ = dc.db_connection(
            user=mock_config['USER'],
            password=mock_config['PWD'],
            host=mock_config['SERVER'],
            port=mock_config['PORT'],
            dbname=mock_config['DB']
        )