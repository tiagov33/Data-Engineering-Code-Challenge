"""
data_preparation.py

This module is responsible for preparing the data for the challenge.
It includes functions to load data, check for missing values, check for format inconsistencies,
handle duplicates, and enforce data types and constraints.

Functions:
- load_data: Load data from the specified path in the configuration file.
- check_missing_values: Check for missing values in the DataFrame.
- check_format_inconsistencies: Check for inconsistencies in the data format.
- handle_duplicates: Handle duplicates in the DataFrame by dropping rows 
with duplicate primary keys.
- enforce_constraints: Enforce constraints for a specific column in the DataFrame.
- enforce_data_types: Enforce data types and constraints for each column in the DataFrame.
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from src.utils import get_logger

logger = get_logger(__name__)

def load_data(spark: SparkSession, config: dict) -> DataFrame:
    """
    Load data from the specified path in the configuration file.

    Args:
        spark (SparkSession): The Spark session.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: The loaded DataFrame.
    """
    try:
        logger.info("Loading data from %s", config['path'])
        df = spark.read.format(config['format']).option("header", "true").load(config['path'])
        return df
    except FileNotFoundError as e:
        logger.error("Input data file not found: %s - %s", config['path'], e)
        raise
    except Exception as e:
        logger.error("Unexpected error loading data from %s: %s", config['path'], e)
        raise

def check_missing_values(df: DataFrame, config: dict) -> DataFrame:
    """
    Check for missing values in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to check.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: A DataFrame showing the count of missing values for each column.
    """
    try:
        logger.info("Checking for missing values in table %s", config['table_name'])

        missing_values = df.select(
            [
                F.count(
                    F.when(F.col(c).isNull(), F.lit(1))
                ).alias(c)
                for c in df.columns
            ]
        )

        logger.info("Missing values:\n%s", missing_values.collect())
        return missing_values
    except Exception as e:
        logger.error("Error checking missing values in table %s: %s", config['table_name'], e)
        raise

def check_format_inconsistencies(df: DataFrame, config: dict) -> DataFrame:
    """
    Check for inconsistencies in the data format.

    Args:
        df (DataFrame): The DataFrame to check.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: A DataFrame showing the count of format inconsistencies for each column.
    """
    try:
        logger.info("Checking for format inconsistencies in table %s", config['table_name'])
        format_inconsistencies = []
        for column, metadata in config['columns'].items():
            if metadata['type'] == 'string':
                logger.info("Checking for empty strings in column %s", column)
                format_inconsistencies.append(
                    F.count(
                        F.when(
                            F.length(F.trim(F.col(column))) == 0,
                            F.lit(1),
                        )
                    ).alias(column)
                )
            elif metadata['type'] == 'date':
                expected_format = metadata['output_format']
                logger.info(
                    "Checking raw dates in column %s against output format: %s",
                    column,
                    expected_format,
                )

                raw_value = F.col(column).cast("string")

                parsed_date = F.to_date(
                    F.try_to_timestamp(
                        F.trim(raw_value),
                        F.lit(expected_format),
                    )
                )

                formatted_date = F.date_format(parsed_date, expected_format)

                inconsistent = (
                    raw_value.isNotNull()
                    & (
                        parsed_date.isNull()
                        | (raw_value != formatted_date)
                    )
                )

                format_inconsistencies.append(
                    F.count(
                        F.when(inconsistent, F.lit(1))
                    ).alias(column)
                )
            elif metadata["type"] in ("integer", "float"):
                data_type = metadata["type"]

                logger.info(
                    "Checking for invalid %s values in column %s",
                    data_type,
                    column,
                )

                parsed_value = F.expr(
                    f"try_cast({column} AS {data_type})"
                )

                format_inconsistencies.append(
                    F.count(
                        F.when(
                            F.col(column).isNotNull() & parsed_value.isNull(),
                            F.lit(1),
                        )
                    ).alias(column)
                )
        inconsistencies_df = df.select(format_inconsistencies)
        logger.info("Format inconsistencies:\n%s", inconsistencies_df.collect())
        return inconsistencies_df
    except Exception as e:
        logger.error(
            "Error checking format inconsistencies in table %s: %s", 
            config['table_name'], e
        )
        raise

def handle_duplicates(df: DataFrame, config: dict) -> DataFrame:
    """
    Handle duplicates in the DataFrame by dropping rows with duplicate primary keys.

    Args:
        df (DataFrame): The DataFrame to process.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: The DataFrame with duplicates removed.
    """
    try:
        table_name = config["table_name"]
        primary_keys = config["primary_keys"]

        if not primary_keys:
            raise ValueError(f"No primary keys configured for {table_name}")

        missing_columns = set(primary_keys) - set(df.columns)
        if missing_columns:
            raise ValueError(
                f"Missing primary-key columns in {table_name}: "
                f"{sorted(missing_columns)}"
            )

        logger.info("Handling duplicates in table %s", table_name)

        initial_count = df.count()
        deduplicated_df = df.dropDuplicates()
        final_count = deduplicated_df.count()

        logger.info(
            "Removed %d exact duplicate rows from table %s",
            initial_count - final_count,
            table_name,
        )

        conflicting_keys = (
            deduplicated_df.groupBy(*primary_keys)
            .count()
            .filter(F.col("count") > 1)
        )

        if conflicting_keys.limit(1).count() > 0:
            logger.error(
                "Conflicting records found for primary keys %s in table %s",
                primary_keys,
                table_name,
            )
            raise ValueError(
                f"Conflicting records share primary keys in {table_name}"
            )

        return deduplicated_df
    except Exception as e:
        logger.error("Error handling duplicates in table %s: %s", config['table_name'], e)
        raise

def enforce_constraints(df: DataFrame, column: str, constraints: dict) -> DataFrame:
    """
    Enforces constraints for a specific column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to process.
        column (str): The column to enforce constraints on.
        constraints (dict): The constraints dictionary for the column.

    Returns:
        DataFrame: The DataFrame with enforced constraints.
    """
    try:
        if 'not_null' in constraints and constraints['not_null']:
            logger.info("Enforcing not_null constraint on column %s", column)
            df = df.filter(F.col(column).isNotNull())

        if 'not_empty' in constraints and constraints['not_empty']:
            logger.info("Enforcing not_empty constraint on column %s", column)
            df = df.filter(F.length(F.trim(F.col(column))) > 0)

        if 'min' in constraints:
            logger.info("Enforcing min constraint on column %s: %s", column, constraints['min'])
            df = df.filter(F.col(column) >= constraints['min'])
        return df
    except Exception as e:
        logger.error("Error enforcing constraints on column %s: %s", column, e)
        raise

def enforce_data_types(df: DataFrame, config: dict) -> DataFrame:
    """
    Enforces data types and constraints for each column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame to process.
        config (dict): The configuration dictionary for the table.

    Returns:
        DataFrame: The DataFrame with enforced data types and constraints.
    """
    try:
        logger.info("Enforcing data types for table %s", config['table_name'])
        for column, metadata in config['columns'].items():
            data_type = metadata['type']
            logger.info("Enforcing data type for column %s: %s", column, data_type)

            if data_type == 'date':
                input_formats = metadata["input_formats"]
                logger.info("Parsing date column %s using formats: %s", column, input_formats)

                parsed_dates = [
                    F.to_date(
                        F.try_to_timestamp(
                            F.trim(F.col(column)),
                            F.lit(date_format),
                        )
                    )
                    for date_format in input_formats
                ]

                df = df.withColumn(column, F.coalesce(*parsed_dates))
                logger.debug(
                    "Schema after enforcing data type for column %s: %s", 
                    column, df.schema
                )
            elif data_type in ("integer", "float"):
                df = df.withColumn(
                    column,
                    F.expr(f"try_cast({column} AS {data_type})"),
                )
            else:
                df = df.withColumn(column, F.col(column).cast(data_type))
            if 'constraints' in metadata:
                df = enforce_constraints(df, column, metadata['constraints'])
        return df
    except Exception as e:
        logger.error("Error enforcing data types for table %s: %s", config['table_name'], e)
        raise
