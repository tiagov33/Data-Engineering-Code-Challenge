from src.utils import load_config, get_logger

logger = get_logger(__name__)

# Load configuration files
tables_config = load_config("config/tables_config.json")
output_config = load_config("config/output_config.json")

logger.info("Config files loaded successfully.")


