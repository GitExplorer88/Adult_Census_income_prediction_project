import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File paths
RAW_DATA = Path(r"C:\Users\DELL\OneDrive\Desktop\pandasapp\Adult_Census_income_prediction\data\raw\adult_census_income_prediction.csv")
PROCESSED_DIR = Path(r"C:\Users\DELL\OneDrive\Desktop\pandasapp\Adult_Census_income_prediction\data\Processed_data")
PROCESSED_FILE = PROCESSED_DIR / "Processed_data.csv"
CATEGORICAL_COLS = ['workclass', 'education', 'marital-status', 'occupation', 
                    'relationship', 'race', 'sex', 'salary', 'country']

def preprocess_data(file_path, categorical_cols, save_path):
    """Load, clean, and process the dataset."""
    logging.info(f"Loading data from {file_path}")
    df = pd.read_csv(file_path)
    
    logging.info("Cleaning data...")
    df = df.apply(lambda col: col.str.strip() if col.dtypes == 'object' else col)
    df.replace('?', np.nan, inplace=True)
    df.dropna(inplace=True)
    
    logging.info(f"Saving processed data to {save_path}")
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path, index=False)
    logging.info("Processing complete.")

if __name__ == "__main__":
    preprocess_data(RAW_DATA, CATEGORICAL_COLS, PROCESSED_FILE)

