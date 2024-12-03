import pandas as pd
import SPARQLWrapper
from query_handler import QueryHandler
from handler import Handler, UploadHandler
from metadata_upload_handler import MetadataUploadHandler

class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        
    
    def getAllPeople(self):
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        sparql.setQuery("""
            PREFIX ex: <http://example.org/> #cambiare per qualcosa com VIAF or ULAN
            SELECT ?personId ?name #person id bisogna essere Viaf number or Ulan number
            WHERE {
                ?personId a ex:Person ; 
                ex:name ?name .
            }
        """)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return pd.DataFrame([{"id": result["personId"]["value"], "name": result["name"]["value"]} for result in results["results"]["bindings"]])
    
