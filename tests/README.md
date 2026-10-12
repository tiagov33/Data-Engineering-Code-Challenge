# Tests  
  
The test suite uses pytest and chispa to validate the PySpark pipeline.  
  
## Files  
  
- `conftest.py`: Shared local Spark session with teardown.  
- `test_data_preparation.py`: CSV loading, validation summaries,  
  duplicate handling, type conversion, and constraints.  
- `test_data_transformation.py`: Revenue aggregation, monthly sales,  
  enrichment, and price classification.  
- `test_data_export.py`: Parquet and CSV round trips, partitioning,  
  and overwrite behavior. 
  
## Requirements  
  
Install the project dependencies and ensure a compatible Java runtime  
is available. The application uses `try_to_timestamp`, which requires  
PySpark 3.5 or newer.  
  
## Running Tests  
  
Run from the project root:  
  
```bash  
python -m pytest  
```  
  
Run one test module:  
  
```bash  
python -m pytest tests/test_data_transformation.py -v  
```  
  
## Test Design  
  
Tests use small synthetic datasets rather than repository input files.  
  
Export tests write to pytest-managed temporary directories and do not  
modify application output directories.  
  
DataFrame comparisons use chispa. Row order is ignored unless ordering  
is explicitly part of the behavior being tested. Data types and column  
names are checked; schema nullability differences are ignored.  