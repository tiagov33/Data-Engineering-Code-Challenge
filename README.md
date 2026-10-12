# Data-Engineering-Code-Challenge
  
A modular PySpark application for processing retail sales data, generating sales insights, and exporting datasets for further analysis.  
  
The pipeline combines sales transactions with product and store information, prepares the input data, performs transformations, and writes the results to Parquet and CSV.  
  
## Input Datasets  
  
The application uses three CSV datasets:  
  
| Dataset | Columns |  
| --- | --- |  
| `sales.csv` | `transaction_id`, `store_id`, `product_id`, `quantity`, `transaction_date`, `price` |  
| `products.csv` | `product_id`, `product_name`, `category` |  
| `stores.csv` | `store_id`, `store_name`, `location` |  
  
Transaction dates are expected in `YYYY-MM-DD` format.  
  
## Project Structure  
  
```text  
.  
├── .github/  
│   └── workflows/  
│       └── ci.yml  
├── config/  
│   ├── tables_config.json  
│   ├── output_config.json  
│   └── README.md  
├── data/  
│   ├── sales_uuid.csv  
│   ├── products_uuid.csv  
│   └── stores_uuid.csv  
├── src/  
│   ├── __init__.py  
│   ├── data_preparation.py  
│   ├── data_transformation.py  
│   ├── data_export.py  
│   ├── pipeline.py  
│   ├── utils.py  
│   └── README.md  
├── tests/  
│   ├── __init__.py  
│   ├── conftest.py  
│   ├── README.md  
│   ├── test_data_export.py  
│   ├── test_data_preparation.py  
│   └── test_data_transformation.py
├── logs/                       # Generated application logs  
├── output/                     # Generated output datasets  
├── .gitignore  
├── requirements.txt  
├── main.py  
└── README.md  
```  
  
Input and output locations are controlled by the configuration files. The `logs/` and `output/` directories represent runtime artifacts rather than source code.  
  
## Approach  
  
The application separates orchestration, data preparation, transformations, and exports into dedicated modules.  
  
`main.py` runs the following steps:  
  
1. Load the table and output configurations.  
2. Initialize a Spark session.  
3. Process the sales, products, and stores datasets through `process_table`.  
4. Calculate revenue by store and product category.  
5. Create a monthly sales insights DataFrame.  
6. Combine the datasets into an enriched sales DataFrame.  
7. Add a price classification to the enriched data.  
8. Export the enriched datasets and revenue insights.  
9. Stop the Spark session, including when an error occurs.  
  
The data preparation stage covers loading, missing-value and format validation, duplicate handling, and data type enforcement. See [`src/README.md`](src/README.md) for the detailed processing rules.  
  
## Transformations  
  
### Revenue by Store and Category  
  
Revenue is calculated as:  
  
```text  
revenue = quantity × price  
```  
  
The resulting DataFrame contains:  
  
- `store_id`  
- `category`  
- `total_revenue`  
  
### Monthly Sales Insights  
  
Quantities are aggregated by transaction year, month, and product category.  
  
The resulting DataFrame contains:  
  
- `year`  
- `month`  
- `category`  
- `total_quantity_sold`  
  
**Current behavior:** `main.py` creates this DataFrame but does not export it.  
  
### Enriched Sales Data  
  
Sales transactions are combined with product and store information to produce:  
  
- `transaction_id`  
- `store_name`  
- `location`  
- `product_name`  
- `category`  
- `quantity`  
- `transaction_date`  
- `price`  
  
### Price Classification  
  
An additional enriched dataset includes a `price_category` column:  
  
| Category | Price range |  
| --- | --- |  
| Low | `price < 20` |  
| Medium | `20 <= price <= 100` |  
| High | `price > 100` |  
  
Prices of exactly `20` and `100` belong to the `Medium` category.  
  
## Configuration  
  
The application reads:  
  
- `config/tables_config.json`: input table configuration.  
- `config/output_config.json`: output destinations.  
  
Before running the pipeline:  
  
1. Ensure the input files exist at the configured locations.  
2. Review the table configuration.  
3. Ensure the configured output locations are writable.  
  
See [`config/README.md`](config/README.md) for configuration details.  
  
**Run commands from the project root**, because `main.py` loads the configuration files using relative paths.  
  
## Run the Pipeline  
  
With the virtual environment activated:  
  
```bash  
python main.py  
```  

## Outputs  
  
Output destinations are defined in `config/output_config.json`.  
  
| Configuration key | Dataset | Format |  
| --- | --- | --- |  
| `enriched_data` | Enriched sales data | Parquet, partitioned by `category` and `transaction_date` |  
| `revenue_insights` | Revenue by store and category | CSV |  
| `enriched_data_with_price_category` | Enriched sales data including `price_category` | Parquet, using the same export function as `enriched_data` |  
  
Spark writes datasets to directories containing part files and potentially metadata files. A CSV output path therefore represents a directory, not necessarily a single CSV file.  
  
Monthly sales insights are not currently written to disk.  
  
## Testing  
  
From the project root, with the virtual environment activated:  
  
```bash  
python -m pytest  
```  
  
The challenge requires [`chispa`](https://github.com/MrPowers/chispa) for PySpark tests. Java and a working Spark environment are required for tests that create Spark sessions.  
  
See [`tests/README.md`](tests/README.md) for detailed test documentation.  
  
## Logging and Error Handling  
  
The application uses logging rather than print statements for operational messages.  
  
Logging is configured in `src/utils.py`; the configured log location is `logs/app.log`.  
  
If the pipeline fails:  
  
- The exception is logged with its traceback.  
- The Spark session is stopped.  
- The application exits with a non-zero status code.  
  
## Continuous Integration  
  
The GitHub Actions workflow is defined in:  
  
```text  
.github/workflows/ci.yml  
```  
  
Refer to that file for the configured automation, including test commands and any linting or formatting checks.  
  
## Assumptions and Decisions  
  
- Revenue is based on the transaction's unit price multiplied by its quantity.  
- Monthly sales quantities are grouped by both year and month to avoid combining the same month across different years.  
- Price classification includes both boundary values, `20` and `100`, in the `Medium` category.  
- Table preparation is kept separate from business transformations.  
- Input and output settings are stored in configuration files rather than personal, machine-specific paths.  
- The original enriched dataset and the price-classified dataset are exported separately.  
- The current pipeline creates monthly sales insights without exporting them.  
  
Detailed policies for invalid records, duplicate keys, and unmatched joins are documented alongside the relevant modules.  
  
## Additional Documentation  
  
- [`src/README.md`](src/README.md): processing modules and transformation rules.  
- [`config/README.md`](config/README.md): configuration structure and settings.  
- [`tests/README.md`](tests/README.md): test organization and execution.  