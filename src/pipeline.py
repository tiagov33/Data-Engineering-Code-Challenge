"""
pipeline.py

This module contains functions for processing tables in the data pipeline.
It includes functions for loading data, checking for missing values and inconsistencies,
handling duplicates, and enforcing data types.
"""

from pyspark.sql import SparkSession, DataFrame
from src.data_preparation import (
    load_data, 
    handle_duplicates, 
    enforce_data_types, 
    check_missing_values, 
    check_format_inconsistencies
)
from src.utils import get_logger

logger = get_logger(__name__)

def process_table(spark: SparkSession, config: dict, table_name: str) -> DataFrame:
    """
    Process a table by loading data, checking for missing values and inconsistencies,
    handling duplicates, and enforcing data types.

    Args:
        spark (SparkSession): The Spark session.
        config (dict): The configuration dictionary.
        table_name (str): The name of the table to process.

    Returns:
        DataFrame: The processed DataFrame.
    """
    try:
        table_config = config[table_name]
        logger.info("Processing table: %s", table_name)

        # Loading data
        df = load_data(spark, config[table_name])
        
        # Check for missing values and inconsistencies
        check_missing_values(df, table_config)
        check_format_inconsistencies(df, table_config)
        
        # Handle duplicates and enforce data types
        df = enforce_data_types(df, table_config)
        df = handle_duplicates(df, table_config)

        return df
    except Exception as e:
        logger.error("Error processing table %s: %s", table_name, e)
        raise