import pandas as pd
from handler import Handler
""" from metadata_upload_handler import DataFrame
from process_data_upload_handler import DataFrame """
from pandas import DataFrame

class QueryHandler(Handler): 
    def __init__(self, dbPathOrUrl: str = ""):  
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str) -> DataFrame: # Retrieve data by its ID. This method does nothing here and should be implemented in subclasses to provide specific query logic.
        pass