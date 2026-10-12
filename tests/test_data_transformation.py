"""Tests for sales insights, enrichment, and price classification."""  
  
from datetime import date  
  
import pytest  
from chispa.dataframe_comparer import assert_df_equality  
  
from src.data_transformation import (  
    add_price_category,  
    calculate_monthly_sales,  
    calculate_total_revenue,  
    categorized_price,  
    enrich_data,  
)  
  
  
@pytest.fixture  
def prepared_tables(spark):  
    sales = spark.createDataFrame(  
        [  
            ("T1", "S1", "P1", 2, date(2024, 7, 4), 10.0),  
            ("T2", "S1", "P1", 3, date(2024, 7, 5), 10.0),  
            ("T3", "S1", "P2", 1, date(2024, 7, 6), 20.0),  
            ("T4", "S2", "P1", 4, date(2025, 7, 4), 10.0),  
            ("T5", "S1", "P3", 2, date(2024, 8, 4), 5.0),  
            ("T6", "S1", "P3", 1, date(2024, 7, 4), 5.0),  
        ],  
        (  
            "transaction_id string, store_id string, product_id string, "  
            "quantity int, transaction_date date, price float"  
        ),  
    )  
    products = spark.createDataFrame(  
        [  
            ("P1", "Product1", "CategoryA"),  
            ("P2", "Product2", "CategoryA"),  
            ("P3", "Product3", "CategoryB"),  
        ],  
        "product_id string, product_name string, category string",  
    )  
    stores = spark.createDataFrame(  
        [  
            ("S1", "Store1", "Location1"),  
            ("S2", "Store2", "Location2"),  
        ],  
        "store_id string, store_name string, location string",  
    )  
  
    return sales, products, stores  
  
  
def test_calculate_total_revenue_groups_by_store_and_category(  
    spark, prepared_tables  
):  
    sales, products, _ = prepared_tables  
  
    actual = calculate_total_revenue(sales, products)  
    expected = spark.createDataFrame(  
        [  
            ("S1", "CategoryA", 70.0),  
            ("S2", "CategoryA", 40.0),  
            ("S1", "CategoryB", 15.0),  
        ],  
        "store_id string, category string, total_revenue double",  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
def test_calculate_monthly_sales_separates_year_month_and_category(  
    spark, prepared_tables  
):  
    sales, products, _ = prepared_tables  
  
    actual = calculate_monthly_sales(sales, products)  
    expected = spark.createDataFrame(  
        [  
            (2024, 7, "CategoryA", 6),  
            (2025, 7, "CategoryA", 4),  
            (2024, 8, "CategoryB", 2),  
            (2024, 7, "CategoryB", 1),  
        ],  
        (  
            "year int, month int, category string, "  
            "total_quantity_sold bigint"  
        ),  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
def test_enrich_data_returns_expected_attributes_and_columns(  
    spark, prepared_tables  
):  
    sales, products, stores = prepared_tables  
  
    actual = enrich_data(sales, products, stores)  
    expected = spark.createDataFrame(  
        [  
            (  
                "T1", "Store1", "Location1", "Product1",  
                "CategoryA", 2, date(2024, 7, 4), 10.0,  
            ),  
            (  
                "T2", "Store1", "Location1", "Product1",  
                "CategoryA", 3, date(2024, 7, 5), 10.0,  
            ),  
            (  
                "T3", "Store1", "Location1", "Product2",  
                "CategoryA", 1, date(2024, 7, 6), 20.0,  
            ),  
            (  
                "T4", "Store2", "Location2", "Product1",  
                "CategoryA", 4, date(2025, 7, 4), 10.0,  
            ),  
            (  
                "T5", "Store1", "Location1", "Product3",  
                "CategoryB", 2, date(2024, 8, 4), 5.0,  
            ),  
            (  
                "T6", "Store1", "Location1", "Product3",  
                "CategoryB", 1, date(2024, 7, 4), 5.0,  
            ),  
        ],  
        (  
            "transaction_id string, store_name string, location string, "  
            "product_name string, category string, quantity int, "  
            "transaction_date date, price float"  
        ),  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
@pytest.mark.parametrize(  
    "price,expected",  
    [  
        (0.0, "Low"),  
        (19.99, "Low"),  
        (20.0, "Medium"),  
        (50.0, "Medium"),  
        (100.0, "Medium"),  
        (100.01, "High"),  
    ],  
)  
def test_categorized_price_boundaries(price, expected):  
    assert categorized_price(price) == expected  
  
  
def test_add_price_category_applies_udf_and_preserves_input(spark):  
    original = spark.createDataFrame(  
        [  
            ("T1", 19.99),  
            ("T2", 20.0),  
            ("T3", 100.0),  
            ("T4", 100.01),  
        ],  
        "transaction_id string, price double",  
    )  
  
    actual = add_price_category(original)  
    expected = spark.createDataFrame(  
        [  
            ("T1", 19.99, "Low"),  
            ("T2", 20.0, "Medium"),  
            ("T3", 100.0, "Medium"),  
            ("T4", 100.01, "High"),  
        ],  
        "transaction_id string, price double, price_category string",  
    )  
  
    assert_df_equality(  
        actual,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
    assert original.columns == ["transaction_id", "price"]  