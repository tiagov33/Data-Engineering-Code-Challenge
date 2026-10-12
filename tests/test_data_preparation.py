"""Tests for input loading, validation, and preparation."""  
  
from datetime import date 
  
import pytest  
from chispa.dataframe_comparer import assert_df_equality  
  
from src.data_preparation import (  
    check_format_inconsistencies,  
    check_missing_values,  
    enforce_constraints,  
    enforce_data_types,  
    handle_duplicates,  
    load_data,  
)  
  
  
def test_load_data_reads_csv_with_header_as_strings(spark, tmp_path):  
    input_path = tmp_path / "sales.csv"  
    input_path.write_text(  
        "transaction_id,quantity\nT1,2\nT2,3\n",  
        encoding="utf-8",  
    )  
  
    actual = load_data(  
        spark,  
        {"path": str(input_path), "format": "csv"},  
    )  
    expected = spark.createDataFrame(  
        [("T1", "2"), ("T2", "3")],  
        "transaction_id string, quantity string",  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
def test_check_missing_values_counts_nulls_not_blank_strings(spark):  
    df = spark.createDataFrame(  
        [  
            ("1", None, ""),  
            (None, "2", " "),  
            ("3", None, None),  
        ],  
        "col1 string, col2 string, col3 string",  
    )  
    config = {"table_name": "test_table"}  
  
    actual = check_missing_values(df, config)  
    expected = spark.createDataFrame(  
        [(1, 2, 1)],  
        "col1 bigint, col2 bigint, col3 bigint",  
    )  
  
    assert_df_equality(actual, expected, ignore_nullable=True)  
  
  
def test_check_format_inconsistencies_reports_invalid_values(spark):  
    df = spark.createDataFrame(  
        [  
            ("Product", "2024-07-04", "2", "10.5"),  
            ("  ", "2024/07/04", "invalid", "invalid"),  
            ("", "2024-02-30", "", ""),  
            (None, None, None, None),  
            ("Other", "", "3", "0.0"),  
        ],  
        "name string, event_date string, quantity string, price string",  
    )  
    config = {  
        "table_name": "test_table",  
        "columns": {  
            "name": {"type": "string"},  
            "event_date": {  
                "type": "date",  
                "output_format": "yyyy-MM-dd",  
            },  
            "quantity": {"type": "integer"},  
            "price": {"type": "float"},  
        },  
    }  
  
    actual = check_format_inconsistencies(df, config)  
    expected = spark.createDataFrame(  
        [(2, 3, 2, 2)],  
        "name bigint, event_date bigint, quantity bigint, price bigint",  
    )  
  
    assert_df_equality(actual, expected, ignore_nullable=True)  
  
  
def test_handle_duplicates_removes_repeated_primary_keys(spark):  
    df = spark.createDataFrame(  
        [("T1", "P1"), ("T1", "P1"), ("T2", "P2")],  
        "transaction_id string, product_id string",  
    )  
    config = {  
        "table_name": "sales",  
        "primary_keys": ["transaction_id"],  
    }  
  
    actual = handle_duplicates(df, config)  
    expected = spark.createDataFrame(  
        [("T1", "P1"), ("T2", "P2")],  
        df.schema,  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
def test_handle_duplicates_supports_composite_primary_keys(spark):  
    df = spark.createDataFrame(  
        [("S1", "P1"), ("S1", "P1"), ("S1", "P2"), ("S2", "P1")],  
        "store_id string, product_id string",  
    )  
    config = {  
        "table_name": "test_table",  
        "primary_keys": ["store_id", "product_id"],  
    }  
  
    actual = handle_duplicates(df, config)  
    expected = spark.createDataFrame(  
        [("S1", "P1"), ("S1", "P2"), ("S2", "P1")],  
        df.schema,  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
@pytest.mark.parametrize(  
    "schema,values,constraints,expected_values",  
    [  
        (  
            "value string",  
            [(None,), ("",), (" ",), ("valid",)],  
            {"not_null": True},  
            [("",), (" ",), ("valid",)],  
        ),  
        (  
            "value string",  
            [(None,), ("",), (" ",), ("valid",)],  
            {"not_empty": True},  
            [("valid",)],  
        ),  
        (  
            "value int",  
            [(None,), (-1,), (0,), (1,), (2,)],  
            {"min": 1},  
            [(1,), (2,)],  
        ),  
    ],  
    ids=["not-null", "not-empty", "minimum"],  
)  
def test_enforce_constraints(  
    spark, schema, values, constraints, expected_values  
):  
    df = spark.createDataFrame(values, schema)  
  
    actual = enforce_constraints(df, "value", constraints)  
    expected = spark.createDataFrame(expected_values, schema)  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
def test_enforce_data_types_parses_values_and_filters_invalid_rows(spark):  
    df = spark.createDataFrame(  
        [  
            ("T1", "2", "2024-07-04", "10.5"),  
            ("T2", "3", " 2024/07/05 ", "20"),  
            ("T3", "invalid", "2024-07-04", "10"),  
            ("T4", "2", "not-a-date", "10"),  
            ("T5", "2", "2024-07-04", "invalid"),  
            ("T6", "0", "2024-07-04", "10"),  
            ("T7", "2", "2024-07-04", "-1"),  
            (" ", "2", "2024-07-04", "10"),  
            ("T8", "2", None, "10"),  
        ],  
        (  
            "transaction_id string, quantity string, "  
            "transaction_date string, price string"  
        ),  
    )  
    config = {  
        "table_name": "sales",  
        "columns": {  
            "transaction_id": {  
                "type": "string",  
                "constraints": {"not_null": True, "not_empty": True},  
            },  
            "quantity": {  
                "type": "integer",  
                "constraints": {"not_null": True, "min": 1},  
            },  
            "transaction_date": {  
                "type": "date",  
                "input_formats": ["yyyy-MM-dd", "yyyy/MM/dd"],  
                "output_format": "yyyy-MM-dd",  
                "constraints": {"not_null": True},  
            },  
            "price": {  
                "type": "float",  
                "constraints": {"not_null": True, "min": 0.0},  
            },  
        },  
    }  
  
    actual = enforce_data_types(df, config)  
    expected = spark.createDataFrame(  
        [  
            ("T1", 2, date(2024, 7, 4), 10.5),  
            ("T2", 3, date(2024, 7, 5), 20.0),  
        ],  
        (  
            "transaction_id string, quantity int, "  
            "transaction_date date, price float"  
        ),  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  