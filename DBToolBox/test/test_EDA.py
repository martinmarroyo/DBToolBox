import pytest
import random
import pandas as pd
import DBToolBox.test.conftest as ct
from DBToolBox import EDA

# get_column_info tests
def test_get_column_info():
    """
    Tests that get_column_info returns the expected DataFrame (see conftest)
    Pass condition: Expected Dataframe is returned from get_column_info
    Fail condition: Expected Dataframe is not returned
    """
    random.seed(1)
    test_df = pd.DataFrame({
        "testcol1": random.choices([1,2,3,4],k=20),
        "testcol2": random.choices(["Test1","Test2","Test3","Test4","Test5", None],k=20),
        "testcol3": [i for i in range(1,21)]
    })
    result = EDA.get_column_info(test_df)
    pd.testing.assert_frame_equal(result, ct.COLUMN_INFO_MOCK)


# get_cardinality tests
def test_get_cardinality_01():
    """ 
    Tests that the expected cardinality is returned for a given Series/column
    Pass condition: Expected cardinality is returned
    Fail condition: Incorrect cardinality is returned
    """
    test_col = pd.Series([1,2,3,4,5,5,5])
    result = EDA.get_cardinality(test_col)
    assert result == 5


def test_get_cardinality_02():
    """ 
    Tests that the expected cardinality is returned for a Series/column of only null values
    Pass condition: Expected cardinality is returned
    Fail condition: Incorrect cardinality is returned
    """
    test_col = pd.Series([None, None, None])
    result = EDA.get_cardinality(test_col)
    assert result == 0


def test_get_cardinality_03():
    """ 
    Tests that the expected cardinality is returned for a given Series/column with a 
    mix of null and non-null values
    Pass condition: Expected cardinality is returned
    Fail condition: Incorrect cardinality is returned
    """
    test_col = pd.Series([1, 2, 2, 1, None, None, None])
    result = EDA.get_cardinality(test_col)
    assert result == 2


# get_size_mb tests
def test_get_size_mb():
    """
    Tests that the expected size in MB is returned for the given Dataframe
    Pass Condition: The expected size (in MB) is returned
    """
    result = EDA.get_size_mb(ct.COLUMN_INFO_MOCK)
    assert result == 0.001