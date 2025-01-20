from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import json
from rdflib import Literal
from rdflib.namespace import XSD

from query_handler import QueryHandler
from metadata_upload_handler import MetadataUploadHandler

class MetadataQueryHandler:
    def __init__(self, db_url=""):
        """Initialize a class with the URL of the database."""
        self.db_url = db_url

    def execute_query(self, query):
        """Execute a query SPARQL and return the result as a DataFrame."""
        try:
            sparql = SPARQLWrapper(self.db_url)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            # Extract data directly as a DataFrame
            columns = results["head"]["vars"]
            rows = [
                [binding.get(col, {}).get("value", None) for col in columns]
                for binding in results["results"]["bindings"]
            ]

            df = pd.DataFrame(rows, columns=columns)

            # After the DataFrame is created, fix the date format if necessary
            self.apply_date_fix(df)

            return df
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()

    def apply_date_fix(self, df):
        """Applica la correzione del formato alle date nel DataFrame."""
    if "date" in df.columns:
        df["date"] = df["date"].apply(self.fix_date_format)


    def fix_date_format(self, date_str):
        """Fix the format of the date to ensure it's in YYYY-MM-DD."""
        if isinstance(date_str, str):
            if len(date_str) == 8 and date_str.isdigit():  # If the date is in YYYYMMDD format
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"  # Correct format to YYYY-MM-DD
            else:
                return date_str  # Return it as is if already in a correct format
        return date_str  # If it's not a string, return it unchanged (for NaNs or None values)

    def getAllPeople(self):
        """Fetch all people from the database."""
        query = """
        SELECT DISTINCT ?personName ?personID
        WHERE {
            ?object dcterms:creator ?creator .
            BIND (STRAFTER(?creator, "(") AS ?personID) .
            BIND (STRBEFORE(?creator, " (") AS ?personName) .
        }
        ORDER BY ?personName
        """
        return self.execute_query(query)

    def getAllCulturalHeritageObjects(self):
        """Fetch all cultural heritage objects from the database."""
        query = """
        SELECT ?id ?title ?date ?owner ?place 
        WHERE {
            ?object a dbo:CulturalHeritageObject .
            ?object a DCTERMS.identifier ?id .
            ?object DCTERMS.title ?title .
            ?object schema.dateCreated ?date .
            ?object FOAF.maker ?owner .
            ?object DCTERMS.spatial ?place .
            FILTER (lang(?title) = "en")
        }
        """  
        return self.execute_query(query)

    def getAuthorsOfCulturalHeritageObject(self, objectID):
        """Fetch all authors (people) of a specific cultural heritage object."""
        query = """
        SELECT DISTINCT ?personName ?personID
        WHERE {
            ?object dcterms:identifier ?objectID ;
                    dcterms:creator ?creator .
            ?creator foaf:name ?personName .
            BIND(STR(?creator) AS ?personID)
        }
        ORDER BY ?personName
        """
        # Use parameterized query to pass `objectID`
        return self.execute_query(query, {'objectID': objectID})

    def getCulturalHeritageObjectsAuthoredBy(self, id_type, id_value):
        """Fetch all cultural heritage objects authored by a specific person."""
        personID = f"http://example.org/person/{id_type}_{id_value}"
        
        query = """
        SELECT DISTINCT ?objectID ?title
        WHERE {
            ?object dcterms:creator <{personID}> ;
                    dcterms:identifier ?objectID ;
                    dcterms:title ?title .
        }
        ORDER BY ?objectID
        """
        query = query.format(personID=personID)
        
        return self.execute_query(query)
