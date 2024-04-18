import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
APP_CONFIG = {
    "PORT": os.getenv("PIPELINE_PORT", 5000),
    "HOST": os.getenv("PIPELINE_HOST", "0.0.0.0"),
}
