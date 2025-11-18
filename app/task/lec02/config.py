import os
from dotenv import load_dotenv

# Load environment variables
config_path = os.path.join(os.path.dirname(__file__), 'Access', 'config.env')
load_dotenv(config_path)

# Configuration constants
AUTH_TOKEN = os.getenv('AUTH_TOKEN')
API_URL = os.getenv('API_URL')
BASE_DIR = os.getenv('BASE_DIR')

# Avro schema
AVRO_SCHEMA = {
    "type": "record",
    "name": "SalesRecord",
    "fields": [
        {"name": "client", "type": ["null", "string"], "default": None},
        {"name": "purchase_date", "type": ["null", "string"], "default": None},
        {"name": "product", "type": ["null", "string"], "default": None},
        {"name": "price", "type": ["null", "double"], "default": None},
    ]
}