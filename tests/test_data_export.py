"""Filesystem integration tests for dataset exports."""  
  
from datetime import date  
  
import pytest  
from chispa.dataframe_comparer import assert_df_equality  
from pyspark.sql import functions as F  
  
from src.data_export import save_enriched_data, save_revenue_insights  
  
  
@pytest.mark.parametrize(  
    "with_price_category",  
    [False, True],  
    ids=["enriched", "enriched-with-price-category"],  
)  
def test_save_enriched_data_round_trip_partitioning_and_overwrite(  
    spark, tmp_path, with_price_category  
):  
    df = spark.createDataFrame(  
        [  
            (  
                "T1", "Store1", "Location1", "Product1",  
                "Category1", 2, date(2024, 7, 4), 10.0,  
            ),  
            (  
                "T2", "Store2", "Location2", "Product2",  
                "Category2", 3, date(2024, 7, 5), 50.0,  
            ),  
        ],  
        (  
            "transaction_id string, store_name string, location string, "  
            "product_name string, category string, quantity int, "  
            "transaction_date date, price float"  
        ),  
    )  
  
    if with_price_category:  
        df = df.withColumn(  
            "price_category",  
            F.when(F.col("transaction_id") == "T1", F.lit("Low"))  
            .otherwise(F.lit("Medium")),  
        )  
  
    output_path = tmp_path / "enriched_data"  
    save_enriched_data(df, str(output_path))  
  
    assert (  
        output_path / "category=Category1" / "transaction_date=2024-07-04"  
    ).is_dir()  
    assert (  
        output_path / "category=Category2" / "transaction_date=2024-07-05"  
    ).is_dir()  
    assert list(output_path.rglob("*.parquet"))  
  
    saved = spark.read.parquet(str(output_path))  
  
    # Partition columns may be placed last when Spark reads Parquet.  
    assert set(saved.columns) == set(df.columns)  
    assert_df_equality(  
        saved.select(*df.columns),  
        df,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
    replacement = df.filter(F.col("transaction_id") == "T2")  
    save_enriched_data(replacement, str(output_path))  
  
    overwritten = spark.read.parquet(str(output_path))  
    assert_df_equality(  
        overwritten.select(*replacement.columns),  
        replacement,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
  
def test_save_revenue_insights_csv_header_values_and_overwrite(  
    spark, tmp_path  
):  
    df = spark.createDataFrame(  
        [("S1", "Category1", 100.0), ("S2", "Category2", 200.0)],  
        "store_id string, category string, total_revenue double",  
    )  
    output_path = tmp_path / "revenue_insights"  
  
    save_revenue_insights(df, str(output_path))  
  
    assert output_path.is_dir()  
    assert list(output_path.glob("*.csv"))  
  
    saved = spark.read.option("header", True).csv(str(output_path))  
    expected = spark.createDataFrame(  
        [("S1", "Category1", "100.0"), ("S2", "Category2", "200.0")],  
        "store_id string, category string, total_revenue string",  
    )  
  
    # CSV has no stored Spark schema, so the default reader returns strings.  
    assert_df_equality(  
        saved,  
        expected,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  
  
    replacement = spark.createDataFrame(  
        [("S3", "Category3", 300.0)],  
        df.schema,  
    )  
    save_revenue_insights(replacement, str(output_path))  
  
    overwritten = spark.read.option("header", True).csv(str(output_path))  
    expected_replacement = spark.createDataFrame(  
        [("S3", "Category3", "300.0")],  
        expected.schema,  
    )  
  
    assert_df_equality(  
        overwritten,  
        expected_replacement,  
        ignore_nullable=True,  
        ignore_row_order=True,  
    )  