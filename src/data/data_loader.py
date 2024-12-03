from abc import ABC, abstractmethod
import pandas as pd
from astrapy import DataAPIClient
from astrapy.database import Database
import os

# Abstract Base Class
class DataIngestor(ABC):
    @abstractmethod
    def ingest(self, client: DataAPIClient, db: Database, collection_name: str) -> pd.DataFrame:
        """Abstract method to ingest data from the database."""
        pass

# AstraDB Ingestor Implementation
class AstraDBIngestor(DataIngestor):
    def ingest(self, client: DataAPIClient, db: Database, collection_name: str) -> pd.DataFrame:
        """Fetch data from AstraDB and save it as a CSV."""
        self._validate_inputs(client, db)
        root_path = os.path.abspath(os.getcwd())  # Always points to the project root
        folder_path = os.path.join(root_path, "Extracted_data")
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"{collection_name}.csv")


        try:
            collection = db.get_collection(collection_name)
            documents = collection.find({})
            df = pd.DataFrame(documents)
            df.to_csv(file_path, index=False)
            print(f"Data saved successfully at: {file_path}")
            return df
        except Exception as e:
            print(f"Error during data ingestion: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error

    @staticmethod
    def _validate_inputs(client, db):
        """Validate that client and db are properly initialized."""
        if not isinstance(client, DataAPIClient):
            raise TypeError("Invalid client: must be an instance of DataAPIClient")
        if not isinstance(db, Database):
            raise TypeError("Invalid database: must be an instance of Database")

# Factory Class
class DataIngestorFactory:
    @staticmethod
    def get_ingestor(source: str) -> DataIngestor:
        """Return the appropriate DataIngestor based on the source."""
        if source == "AstraDB":
            return AstraDBIngestor()
        else:
            raise ValueError(f"Unsupported data source: {source}")

# Main Execution
if __name__ == "__main__":
    # Replace with actual credentials and configurations
    client = DataAPIClient("AstraCS:sbeWsJFERlmIzFzvFdPYbGHb:d1ca01f2af290a02c33960b0a3044099033ed0cada776a51989f9379329f0f3a")
    db = client.get_database_by_api_endpoint(
        "https://84f4acd1-8998-472f-9808-f8e64f9bb435-us-east1.apps.astra.datastax.com"
    )
    collection_name = "adult_census_income_prediction"

    # Use Factory to get the appropriate ingestor
    try:
        ingestor = DataIngestorFactory.get_ingestor("AstraDB")
        df = ingestor.ingest(client, db, collection_name)
        print("Ingested DataFrame:")
        print(df)
    except ValueError as e:
        print(f"Factory error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")