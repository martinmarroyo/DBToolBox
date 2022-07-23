""" 
@module: EDA
@author: Martin Arroyo
@email: martinm.arroyo7@gmail.com
@description: A collection of functions to support 
              Exploratory Data Analysis (EDA) with 
              Pandas 
"""
import pandas as pd
import matplotlib
import sys


def get_column_info(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame with summary column info"""

    def cardinality_rating(row):
        """Assigns a cardinality rating based on the cardinality"""
        return "HIGH" if row["cardinality"] > 19 else "LOW"

    def variable_type(row):
        """Determines if a given column is a Categorical or Numeric variable"""
        return "Categorical" if row["dtype"] == "object" else "Numeric"

    def cardinality_pct(row):
        """Determines the percentage of rows that the given cardinality represents"""
        return round(row["cardinality"] / df.shape[0], 4)

    def null_count(row):
        """Determines the number of null values for a column"""
        return df[row["column_name"]].isnull().sum()

    def null_pct(row):
        """Determines the pct of null values for a column"""
        return round(df[row["column_name"]].isnull().sum() / df.shape[0], 4)

    # Build the Dataframe
    cardinality_df = pd.DataFrame(
        {"column_name": df.columns, "cardinality": df.nunique(), "dtype": df.dtypes}
    )
    cardinality_df["cardinality_rating"] = cardinality_df.apply(
        cardinality_rating, axis=1
    )
    cardinality_df["variable_type"] = cardinality_df.apply(variable_type, axis=1)
    cardinality_df["cardinality_pct"] = cardinality_df.apply(cardinality_pct, axis=1)
    cardinality_df["nulls"] = cardinality_df.apply(null_count, axis=1)
    cardinality_df["null_pct"] = cardinality_df.apply(null_pct, axis=1)
    cardinality_df.reset_index(inplace=True)
    cardinality_df.drop(columns="index", inplace=True)
    return cardinality_df


def get_cardinality(column: pd.Series) -> int:
    """Returns the cardinality of the given Series"""
    return column.nunique()


def visualize_distribution(df: pd.DataFrame, col: str):
    """Visualize the distribution of values for the given column"""
    df[col].value_counts().plot(kind="bar")


def get_size_mb(df: pd.DataFrame) -> float:
    return round(sys.getsizeof(df) / 10**6, 4)
