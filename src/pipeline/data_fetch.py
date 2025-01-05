import os
import pandas as pd
from astrapy import DataAPIClient
import yaml
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("logs/data_fetch.log", mode="a")]
)

class AstraDBIngestor:
    """Handles fetching data from AstraDB and saving it locally."""
    
    def __init__(self, config_path="config/db_config.yaml"):
        self.config = self._load_config(config_path)
        self.client = None
        self.db = None

    def _load_config(self, path):
        """Load configuration from a YAML file."""
        with open(path, "r") as file:
            logging.info("Configuration loaded.")
            return yaml.safe_load(file)

    def _initialize_client(self):
        """Initialize Astra client and database."""
        try:
            self.client = DataAPIClient(self.config["astra"]["client_id"])
            self.db = self.client.get_database_by_api_endpoint(self.config["astra"]["api_endpoint"])
            logging.info("Connected to Astra DB.")
        except Exception as e:
            logging.error("Failed to connect to Astra DB.")
            raise e

    def fetch_and_save_data(self):
        """Fetch data from AstraDB and save it locally."""
        if not self.client or not self.db:
            self._initialize_client()

        collection_name = self.config["astra"]["collection_name"]
        try:
            logging.info(f"Fetching data from collection: {collection_name}.")
            collection = self.db.get_collection(collection_name)
            documents = collection.find({})
            df = pd.DataFrame(documents)

            if df.empty:
                logging.warning("Fetched data is empty.")
            else:
                logging.info(f"Fetched {len(df)} rows.")

            self._save_to_csv(df, collection_name)
            return df
        except Exception as e:
            logging.error("Error fetching data from Astra DB.")
            raise e

    def _save_to_csv(self, df, collection_name):
        """Save DataFrame to a CSV file."""
        folder_path = os.path.join(os.getcwd(), "data/raw")
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"{collection_name}.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"Data saved to {file_path}")

if __name__ == "__main__":
    ingestor = AstraDBIngestor("config/db/db_config.yaml")
    try:
        df = ingestor.fetch_and_save_data()
        logging.info("Data ingestion completed.")
        print(df)
    except Exception as e:
        logging.error(f"Process failed: {e}")




