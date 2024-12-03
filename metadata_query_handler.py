from SPARQLWrapper import SPARQLWrapper, JSON  # Assicurati che JSON sia importato
import pandas as pd
from query_handler import QueryHandler

class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
        
    def getAllPeople(self):
        sparql = SPARQLWrapper(self.getDbPathOrUrl())
        sparql.setQuery("""
            PREFIX ulan: <http://vocab.getty.edu/ulan/>  # Prefisso ULAN
            PREFIX viaf: <http://viaf.org/viaf/>        # Prefisso VIAF

            SELECT ?personId ?name  # personId può essere un numero ULAN o VIAF
            WHERE {
                {
                    ?personId a ulan:Person ;  # Dati da ULAN
                    ulan:name ?name .
                }
                UNION
                {
                    ?personId a viaf:Person ;  # Dati da VIAF
                    viaf:name ?name .
                }
            }
        """)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return pd.DataFrame([{"id": result["personId"]["value"], "name": result["name"]["value"]} for result in results["results"]["bindings"]])
