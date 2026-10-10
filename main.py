from pyspark.sql import SparkSession
from src.utils import load_config, get_logger
from src.data_preparation import load_data

# Initialize Spark session and logger
spark = (  
    SparkSession.builder  
    .appName("DataPipeline")  
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  
    .getOrCreate()  
)  

logger = get_logger(__name__)

# Load configuration files
tables_config = load_config("config/tables_config.json")
output_config = load_config("config/output_config.json")

sales_df = load_data(spark, tables_config["sales"])

sales_df.show(truncate=False)


