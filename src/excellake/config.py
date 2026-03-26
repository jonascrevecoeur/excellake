from dotenv import load_dotenv
import os

load_dotenv()


class Config:
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "dev")
        self.data_directory = os.getenv(f"DATA_DIRECTORY_{self.environment.upper()}")


config = Config()
