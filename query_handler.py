import pandas as pd
from handler import Handler

class QueryHandler(Handler): 
    def __init__(self, dbPathOrUrl: str = ""):  
        self.dbPathOrUrl = dbPathOrUrl 

    def getById(self, id: str):
        pass  