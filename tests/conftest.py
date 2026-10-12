"""Shared pytest fixtures."""  
  
import pytest  
from pyspark.sql import SparkSession  
  
  
@pytest.fixture(scope="session")  
def spark():  
    """Create one local Spark session and stop it after the test session."""  
    session = (  
        SparkSession.builder  
        .master("local[2]")  
        .appName("retail-pipeline-tests")  
        .config("spark.ui.enabled", "false")  
        .config("spark.sql.shuffle.partitions", "2")  
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  
        .getOrCreate()  
    )  
    session.sparkContext.setLogLevel("WARN")  
  
    try:  
        yield session  
    finally:  
        session.stop()