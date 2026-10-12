# Source Code — Data Engineering Challenge  
  
This directory contains the modules used by the PySpark retail sales pipeline.  
  
The application separates data preparation, business transformations, exports, and shared utilities. The project-level [`main.py`](../main.py) orchestrates these modules.  
  
## Module Structure  
  
```text  
src/  
├── __init__.py  
├── data_preparation.py  
├── data_transformation.py  
├── data_export.py  
├── pipeline.py  
├── utils.py  
└── README.md  
```  
  
| Module | Responsibility |  
| --- | --- |  
| `data_preparation.py` | Load and validate input data, handle duplicates, and enforce types and constraints. |  
| `data_transformation.py` | Calculate sales insights and enrich transaction data. |  
| `data_export.py` | Write transformed datasets to Parquet and CSV. |  
| `pipeline.py` | Coordinate the preparation of each input table. |  
| `utils.py` | Load JSON configuration and provide application loggers. |  
  
## Pipeline Overview  
  
The application follows this workflow:  
  
```text  
Input CSV files + table configuration  
                  │  
                  ▼  
           process_table()  
                  │  
                  ▼  
     Prepared sales, products, and stores  
                  │  
                  ├── Revenue by store and category  
                  │       └── CSV output  
                  │  
                  ├── Monthly sales by category  
                  │       └── DataFrame only; not currently exported  
                  │  
                  └── Enriched sales data  
                          ├── Partitioned Parquet output  
                          └── Add price_category  
                                  └── Partitioned Parquet output  
```  
  
Input settings are loaded from `config/tables_config.json`. Output destinations are loaded from `config/output_config.json`.  
  
## `data_preparation.py`  
  
Provides reusable functions for preparing input datasets before business transformations.  
  
### Functions  
  
#### `load_data(spark: SparkSession, config: dict) -> DataFrame`  
  
Loads a dataset from the path specified in its configuration and returns a Spark DataFrame.  
  
#### `check_missing_values(df: DataFrame, config: dict) -> DataFrame`  
  
Checks missing values according to the table configuration and returns the resulting DataFrame.  
  
#### `check_format_inconsistencies(df: DataFrame, config: dict) -> DataFrame`  
  
Checks configured columns for data-format inconsistencies.  
  
For example, sales transaction dates are expected in `YYYY-MM-DD` format.  
  
#### `handle_duplicates(df: DataFrame, config: dict) -> DataFrame`  
  
Removes exact duplicate rows, then validates uniqueness using the configured  
primary-key columns.

Duplicate handling follows these rules:  
  
- **Exact duplicates:** rows with identical values across all columns are  
  reduced to one row.  
- **Conflicting records:** rows sharing the same primary key but containing  
  different values raise `ValueError`. The function does not arbitrarily  
  choose a record to keep.
- **Composite primary keys:** uniqueness is checked using the complete  
  combination of configured key columns.  
- **Invalid key configuration:** an empty primary-key list or primary-key  
  columns missing from the DataFrame raises `ValueError`. 
  
#### `enforce_constraints(df: DataFrame, column: str, constraints: dict) -> DataFrame`  
  
Applies the supplied constraints to a specific column.  
  
#### `enforce_data_types(df: DataFrame, config: dict) -> DataFrame`  
  
Applies the configured data types and column constraints.  
  
### Configuration Dependency  
  
These functions rely on the table configuration to define processing rules. Refer to [`config/README.md`](../config/README.md) for the configuration structure.  
  
## `pipeline.py`  
  
Coordinates table preparation using the functions in `data_preparation.py`.  
  
### Functions  
  
#### `process_table(spark: SparkSession, config: dict, table_name: str) -> DataFrame`  
  
Processes the requested table using its configuration.  
  
Its responsibilities include:  
  
- Loading the dataset.  
- Checking missing values.  
- Checking format inconsistencies.  
- Removing exact duplicates and rejecting conflicting primary-key records. 
- Enforcing data types and constraints.  
  
The returned DataFrame is passed to the transformation stage.  
  
### Usage  
  
```python  
sales_df = process_table(spark, config, "sales")  
products_df = process_table(spark, config, "products")  
stores_df = process_table(spark, config, "stores")  
```  
  
## `data_transformation.py`  
  
Contains the business transformations applied to prepared datasets.  
  
### Functions  
  
#### `calculate_total_revenue(sales_df: DataFrame, products_df: DataFrame) -> DataFrame`  
  
Combines sales with product category information and calculates total revenue for each store and category.  
  
Revenue is calculated as:  
  
```text  
revenue = quantity × price  
```  
  
**Output columns:**  
  
- `store_id`  
- `category`  
- `total_revenue`  
  
#### `calculate_monthly_sales(sales_df: DataFrame, products_df: DataFrame) -> DataFrame`  
  
Calculates the total quantity sold for each product category, grouped by transaction year and month.  
  
**Output columns:**  
  
- `year`  
- `month`  
- `category`  
- `total_quantity_sold`  
  
Grouping by both year and month keeps sales from different years separate.  
  
**Current integration:** `main.py` creates this DataFrame but does not export it.  
  
#### `enrich_data(sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame) -> DataFrame`  
  
Combines sales transactions with product and store information.  
  
**Output columns:**  
  
- `transaction_id`  
- `store_name`  
- `location`  
- `product_name`  
- `category`  
- `quantity`  
- `transaction_date`  
- `price`  
  
#### `categorized_price(price: float) -> str`  
  
Classifies a price using the following ranges:  
  
| Result | Condition |  
| --- | --- |  
| `Low` | `price < 20` |  
| `Medium` | `20 <= price <= 100` |  
| `High` | `price > 100` |  
  
Both boundary values, `20` and `100`, are classified as `Medium`.  
  
#### `add_price_category(enriched_df: DataFrame) -> DataFrame`  
  
Returns the enriched dataset with an additional `price_category` column.  
  
The pipeline exports both the enriched dataset without this column and the price-classified version.  
  
## `data_export.py`  
  
Writes transformed datasets to the configured output destinations.  
  
### Functions  
  
#### `save_enriched_data(enriched_df: DataFrame, output_path: str) -> None`  
  
Writes the supplied DataFrame in Parquet format, partitioned by:  
  
- `category`  
- `transaction_date`  
  
This function is used for both enriched outputs:  
  
```python  
save_enriched_data(  
    enriched_df,  
    output_config["enriched_data"]["path"],  
)  
  
save_enriched_data(  
    enriched_df_with_price_categorized,  
    output_config["enriched_data_with_price_category"]["path"],  
)  
```  
  
#### `save_revenue_insights(revenue_df: DataFrame, output_path: str) -> None`  
  
Writes revenue insights by store and product category in CSV format.  
  
```python  
save_revenue_insights(  
    total_revenue_df,  
    output_config["revenue_insights"]["path"],  
)  
```  

## `utils.py`  
  
Provides shared configuration and logging utilities.  
  
### Functions  
  
#### `load_config(config_path: str) -> dict`  
  
Reads a JSON configuration file and returns its contents as a dictionary.  
  
The application uses this function to load both table settings and output destinations.  
  
#### `get_logger(name: str) -> logging.Logger`  
  
Returns a logger for the supplied name.  
  
Modules can obtain a logger using:  
  
```python  
logger = get_logger(__name__)  
```  
  
Application messages should use logging rather than `print` statements.  
  
## Orchestration and Error Handling  
  
[`main.py`](../main.py) is responsible for:  
  
- Loading configuration.  
- Creating the Spark session.  
- Calling table preparation, transformation, and export functions.  
- Logging successful completion or pipeline failure.  
- Stopping the Spark session in a `finally` block.  
  
If an exception occurs, it is logged with its traceback and propagated. The application then exits with a non-zero status code.