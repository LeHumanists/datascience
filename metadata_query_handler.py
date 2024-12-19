from SPARQLWrapper import SPARQLWrapper, JSON # Assicurati che JSON sia importato
import pandas as pd
import json  # Importazione del modulo standard json
import pandas as pd
from query_handler import QueryHandler
from metadata_upload_handler import MetadataUploadHandler

class MetadataQueryHandler:
    def __init__(self, db_url=""):
        """Inicialize a class with the URL of the database."""
        self.db_url = db_url

    def execute_query(self, query):
        """Execute a query SPARQL and retorn the result as a DataFrame."""
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

            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Error executing SPARQL query: {e}")
            return pd.DataFrame()

    def getAllPeople(self):
        """
        Fetch all people from the database.
        Expected result includes person names and their identifiers.
        """
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
    
    def getAllCulturalHeritageObjects (self):
        query = """
        SELECT ?id ?title ?date ?owner ?place 
        WHERE (
            ?object a dbo:CulturalHeritageObject .
            ?object a DCTERMS.identifier ?id .
            ?object DCTERMS.title ?title .
            ?object schema.dateCreated ?date .
            ?object FOAF.maker ?owner .
            ?object DCTERMS.spatial ?place .
            FILTER (lang(?title) = "en")
        ) 
        """
        return self.execute_query(query)
    
    def getAuthorsOfCulturalHeritageObject(self, objectID):
        """
        Fetch all authors (people) of a specific cultural heritage object.
        
        Parameters:
        - objectID: The identifier of the cultural heritage object.
        """
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
        """
        Fetch all cultural heritage objects authored by a specific person.

        Parameters:
        - id_type: Type of the ID, e.g., "VIAF" or "ULAN".
        - id_value: The numeric value of the ID, e.g., "123456".
        """
        # Construct the person URI based on the given type and value
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
        # Format the query with the person URI
        query = query.format(personID=personID)
        
        return self.execute_query(query)