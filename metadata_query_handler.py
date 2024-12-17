import pandas as pd
from SPARQLWrapper import SPARQLWrapper
import numpy as np

class MetadataQueryHandler:
    def __init__(self, grp_endpoint):
        """
        Initialize the MetadataQueryHandler with a SPARQL endpoint URL.
        """
        self.dbPathOrUrl = grp_endpoint

    def getAllPeople(self):
        query = """
            SELECT DISTINCT ?author_id ?author_name
            WHERE {
                ?object <https://schema.org/author> ?author .
                ?author <https://schema.org/identifier> ?author_id .
                ?author <https://schema.org/name> ?author_name .
            }
        """
        sparql = SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(SPARQLWrapper.JSON)
        sparql.setQuery(query)
        
        try:
            ret = sparql.queryAndConvert()
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error
        
        # Convert query results into a DataFrame
        df_columns = ret["head"]["vars"]
        rows = [
            {column: row[column]["value"] for column in df_columns if column in row}
            for row in ret["results"]["bindings"]
        ]
        df = pd.DataFrame(rows, columns=df_columns).replace(np.nan, " ")
        return df

    def getAllCulturalHeritageObjects(self):
        query = """
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE { 
                ?object <https://schema.org/identifier> ?id. 
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. 
                ?object <https://schema.org/name> ?title. 
                ?object <https://schema.org/copyrightHolder> ?owner. 
                ?object <https://schema.org/spatial> ?place. 
                OPTIONAL{ ?object <https://schema.org/dateCreated> ?date. } 
                OPTIONAL{ 
                    ?object <https://schema.org/author> ?author. 
                    ?author <https://schema.org/name> ?author_name. 
                    ?author <https://schema.org/identifier> ?author_id.
                }
            }
        """
        sparql = SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(SPARQLWrapper.JSON)
        sparql.setQuery(query)
        
        try:
            ret = sparql.queryAndConvert()
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error
        
        # Convert query results into a DataFrame
        df_columns = ret["head"]["vars"]
        rows = [
            {column: row[column]["value"] for column in df_columns if column in row}
            for row in ret["results"]["bindings"]
        ]
        df = pd.DataFrame(rows, columns=df_columns).replace(np.nan, " ")
        return df

    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:
        query = f"""
            SELECT DISTINCT ?author ?author_name ?author_id 
            WHERE {{ 
                ?object <https://schema.org/identifier> '{object_id}'. 
                ?object <https://schema.org/author> ?author. 
                ?author <https://schema.org/name> ?author_name. 
                ?author <https://schema.org/identifier> ?author_id. 
            }}
        """
        sparql = SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(SPARQLWrapper.JSON)
        sparql.setQuery(query)
        
        try:
            ret = sparql.queryAndConvert()
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error
        
        # Convert query results into a DataFrame
        df_columns = ret["head"]["vars"]
        rows = [
            {column: row[column]["value"] for column in df_columns if column in row}
            for row in ret["results"]["bindings"]
        ]
        df = pd.DataFrame(rows, columns=df_columns).replace(np.nan, " ")
        return df

    def getCulturalHeritageObjectsAuthoredBy(self, personId: str) -> pd.DataFrame:
        query = f"""
            SELECT DISTINCT ?object ?id ?type ?title ?date ?owner ?place ?author ?author_name ?author_id 
            WHERE {{ 
                ?object <https://schema.org/identifier> ?id. 
                ?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type. 
                ?object <https://schema.org/name> ?title. 
                ?object <https://schema.org/copyrightHolder> ?owner. 
                ?object <https://schema.org/spatial> ?place. 
                ?object <https://schema.org/author> ?author. 
                ?author <https://schema.org/name> ?author_name.
                ?author <https://schema.org/identifier> ?author_id.
                OPTIONAL{{ ?object <https://schema.org/dateCreated> ?date. }}
                FILTER CONTAINS(?author_id, '{personId}')
            }}
        """
        sparql = SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(SPARQLWrapper.JSON)
        sparql.setQuery(query)
        
        try:
            ret = sparql.queryAndConvert()
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on error
        
        # Convert query results into a DataFrame
        df_columns = ret["head"]["vars"]
        rows = [
            {column: row[column]["value"] for column in df_columns if column in row}
            for row in ret["results"]["bindings"]
        ]
        df = pd.DataFrame(rows, columns=df_columns).replace(np.nan, " ")
        return df