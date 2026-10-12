"""
data_transformation.py

This module is responsible for transforming the data for the challenge.
It includes functions to calculate total revenue, calculate monthly sales insights,
enrich data by combining multiple datasets, and categorize products based on price ranges.

Functions:
- calculate_total_revenue: Calculate the total revenue for each store and 
each product category.
- calculate_monthly_sales: Calculate the total quantity sold for each 
product category, grouped by month.
- enrich_data: Combine the sales, products, and stores datasets into a single enriched dataset.
- categorized_price: Categorize products based on price ranges.
- add_price_category: Add a price category column to the enriched dataset.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as pyspark_sum, year, month, round as pyspark_round, udf
from pyspark.sql.types import StringType
from src.utils import get_logger

logger = get_logger(__name__)

def calculate_total_revenue(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Calculate the total revenue for each store and each product category.

    Args:
        sales_df (DataFrame): The sales DataFrame.
        products_df (DataFrame): The products DataFrame.

    Returns:
        DataFrame: DataFrame with store_id, category, and total_revenue.
    """
    try:
        logger.info("Calculating total revenue for each store and product category")

        sales_with_category_df = sales_df.join(
            products_df, on="product_id", how="inner"
        )

        total_revenue_df = sales_with_category_df.groupBy("store_id", "category").agg(
            pyspark_round(pyspark_sum(col("price") * col("quantity")), 2).alias("total_revenue")
        ).orderBy("total_revenue", ascending=False)
        return total_revenue_df
    except Exception as e:
        logger.error("Error calculating total revenue: %s", e)
        raise

def calculate_monthly_sales(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Calculate the total quantity sold for each product category, grouped by month.

    Args:
        sales_df (DataFrame): The sales DataFrame.
        products_df (DataFrame): The products DataFrame.

    Returns:
        DataFrame: DataFrame with year, month, category, and total_quantity_sold.
    """
    try:
        logger.info("Calculating total quantity sold for each product category, grouped by month")

        sales_with_category_df = sales_df.join(
            products_df, on="product_id", how="inner"
        )
        monthly_sales_df = sales_with_category_df.groupBy(
            year("transaction_date").alias("year"),
            month("transaction_date").alias("month"),
            "category"
        ).agg(
            pyspark_sum("quantity").alias("total_quantity_sold")
        ).orderBy(
            "total_quantity_sold", ascending=False
        )
        return monthly_sales_df
    except Exception as e:
        logger.error("Error calculating monthly sales: %s", e)
        raise

def enrich_data(sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame) -> DataFrame:
    """
    Combine the sales, products, and stores datasets into a single enriched dataset.

    Args:
        sales_df (DataFrame): The sales DataFrame.
        products_df (DataFrame): The products DataFrame.
        stores_df (DataFrame): The stores DataFrame.

    Returns:
        DataFrame: Enriched DataFrame with transaction_id, store_name, location, product_name, 
        category, quantity, transaction_date, and price.
    """
    try:
        logger.info("Enriching data by combining sales, products, and stores datasets")

        enriched_df = (
            sales_df
            .join(products_df, on="product_id", how="inner")
            .join(stores_df, on="store_id", how="inner")
        )

        enriched_df = enriched_df.select(
            "transaction_id",
            "store_name",
            "location",
            "product_name",
            "category",
            "quantity",
            "transaction_date",
            "price"
        ).orderBy("transaction_date", ascending=False)

        return enriched_df
    except Exception as e:
        logger.error("Error enriching data: %s", e)
        raise

def categorized_price(price: float) -> str:
    """
    Categorize products based on price ranges.

    Args:
        price (float): The price of the product.

    Returns:
        str: The price category (Low, Medium, High).
    """
    try:
        if price < 20:
            return "Low"
        if 20 <= price <= 100:
            return "Medium"
        return "High"
    except Exception as e:
        logger.error("Error categorizing price: %s", e)
        raise


categorized_price_udf = udf(categorized_price, StringType())

def add_price_category(enriched_df: DataFrame) -> DataFrame:
    """
    Add a price category column to the enriched dataset.

    Args:
        enriched_df (DataFrame): The enriched DataFrame.

    Returns:
        DataFrame: Enriched DataFrame with an additional price_category column.
    """
    try:
        logger.info("Adding price category to the enriched dataset")
        enriched_df = enriched_df.withColumn("price_category", categorized_price_udf(col("price")))
        return enriched_df
    except Exception as e:
        logger.error("Error adding price category: %s", e)
        raise
