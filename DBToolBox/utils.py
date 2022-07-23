"""A collection of utility functions to work with data"""
from datetime import datetime


def convert_milli_to_timestamp(time_milli: int) -> datetime:
    """Helper function to convert timestamps in milliseconds to datetime format"""
    return datetime.fromtimestamp(time_milli // 1000)


def convert_gmt_string_to_timestamp(time_str: str) -> datetime:
    """
    Helper function to convert string GMT timestamps to datetime format

    Used when dt strings are in the following format:
    '2022-06-01T21:34:00.0'
    """
    return datetime.fromisoformat(time_str.split(".")[0])
