from abc import ABC, abstractmethod 
import pandas as pd


class DataInspectionStrategy(ABC):
    @abstractmethod
    def inspect(self,df:pd.DataFrame):
        """
        Perform a specific type of data inspection.
        
        Parameters:
        df(pd.DataFrame): The dataframe on which the inspection
        
        Returns:
        None: This method prints the inspection results directly
        """
        pass
    

class DataTypesInspectionStrategy(DataInspectionStrategy):
    def inspect(self, df:pd.DataFrame):
        """
        Inspects and prints the data types and non-null counts of dataframe column
        
        Parameters:
        df(pd.DataFrame): The dataFrame to inspected.
        
        Returns:
        None: Prints the data types and non-null counts to the consol
        """
        print("\nData Types and Non-null Counts")
        print(df.info())
        
class SummaryStatisticsInspectionStrategy(DataInspectionStrategy):
    def inspect(self, df: pd.DataFrame):
        """
        Prints summary statistics for numerical and categorical columns
        
        Parameters:
        df (pd.DataFrame): The dataframe to be inspected.
        
        Returns:
        None: Prints summary statistic to the console.
        """
        print("\nSummary Statistics(Numerical Features):")
        print(df.describe())
        print("\nSummary Statistics (Categorical Features):")
        print(df.describe(include=["object","category"]))
        

class DataInspector:
    def __init__(self, strategy: DataInspectionStrategy):
        """
        Initializes the DataInspector with a specific inspection
        
        Parameters:
        strategy(DataInspectionStrategy):The strategy to be used for data inspection
        
        Returns:
        None
        """
        self._strategy = strategy
    
    def set_strategy(self, strategy:DataInspectionStrategy):
        """
        Sets a new strategy for the DataInspector.
        
        Parameters:
        strategy (DataInspectionStrategy): The new strategy
        
        Returns:
        None:
        """
        self._strategy = strategy
        
    def execute_inspection(self, df:pd.DataFrame):
        """
        Execute the inspection using the current strategy.
        
        Parameters:
        df (pd.DataFrame): The DataFrame to be inspected.
        
        Returns:
        None: Executes the strategy's inspection method.
        """
        self._strategy.inspect(df)
        
        
