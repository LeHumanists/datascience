import pandas as pd
from pandas import DataFrame
import re
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.namespace import FOAF, DCTERMS, XSD
from rdflib import Namespace
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graph_class import CulturalHeritageObject, Author 
from handler import UploadHandler

class ResourceURIs:
    NauticalChart = URIRef("https://dbpedia.org/resource/Nautical_chart")
    ManuscriptPlate = URIRef("http://example.org/ManuscriptPlate")
    ManuscriptVolume = URIRef("https://dbpedia.org/resource/Category:Manuscripts_by_collection")
    PrintedVolume = URIRef("https://schema.org/PublicationVolume")
    PrintedMaterial = URIRef("http://example.org/PrintedMaterial")
    Herbarium = URIRef("https://dbpedia.org/resource/Herbarium")
    Specimen = URIRef("https://dbpedia.org/resource/Specimen")
    Painting = URIRef("https://dbpedia.org/resource/Category:Painting")
    Model = URIRef("https://dbpedia.org/resource/Category:Prototypes")
    Map = URIRef("https://dbpedia.org/resource/Category:Maps")


class MetadataUploadHandler(UploadHandler): 
    def __init__(self):
        super().__init__()
        self.my_graph = Graph()  # Initialize the RDF graph
        self.dbpedia = Namespace("http://dbpedia.org/resource/") 
        self.example = Namespace("http://example.org/")
        self.schema = Namespace("http://schema.org/")
        
        # Bind namespaces to the graph 
        self.my_graph.bind("schema", self.schema)
        self.my_graph.bind("dbpedia", self.dbpedia)
    
    def pushDataToDb(self, file_path) -> bool:
        try:
            df = pd.read_csv(file_path)  # Read the CSV file into a DataFrame
            print("Loaded DataFrame:")
            print(df)  # Show the DataFrame
        except Exception as e:
            print(f"Error to read file CSV: {e}")  # Print error message if reading fails
            return False
        
        for idx, row in df.iterrows():  # Iterate over each row in the DataFrame
            # Use the existing 'Id' from the CSV file as the unique identifier
            cultural_object_id = str(row["Id"])
            subj = URIRef(self.example + cultural_object_id)  # Use the 'Id' as part of the URI
            
            # Assigning object id
            self.my_graph.add((subj, DCTERMS.identifier, Literal(cultural_object_id)))
            
            # Mapping the type based on the "Type" column in the CSV
            type_mapping = {
                "Nautical chart": ResourceURIs.NauticalChart,
                "Manuscript plate": ResourceURIs.ManuscriptPlate,
                "Manuscript volume": ResourceURIs.ManuscriptVolume,
                "Printed volume": ResourceURIs.PrintedVolume,
                "Printed material": ResourceURIs.PrintedMaterial,
                "Herbarium": ResourceURIs.Herbarium,
                "Specimen": ResourceURIs.Specimen,
                "Painting": ResourceURIs.Painting,
                "Model": ResourceURIs.Model,
                "Map": ResourceURIs.Map
            }
            
            # Check if the 'Type' exists in the type_mapping dictionary and assign the RDF type
            item_type = row.get("Type", "").strip()
            if item_type in type_mapping:
                self.my_graph.add((subj, RDF.type, type_mapping[item_type]))
            
            # Add title (or other properties) from the CSV
            self.my_graph.add((subj, DCTERMS.title, Literal(row["Title"].strip())))
            
            # Additional fields like date, owner, place, etc.
            if pd.notna(row.get("Date")):
                self.my_graph.add((subj, self.schema.dateCreated, Literal(row["Date"], datatype=XSD.date)))
            if pd.notna(row.get("Owner")):
                self.my_graph.add((subj, FOAF.maker, Literal(row["Owner"].strip())))
            if pd.notna(row.get("Place")):
                self.my_graph.add((subj, DCTERMS.spatial, Literal(row["Place"].strip())))
        
        # Uploading data to Blazegraph after processing all rows
        try:
            store = SPARQLUpdateStore()
            store.open((self.dbPathOrUrl, self.dbPathOrUrl))

            for triple in self.my_graph.triples((None, None, None)):
                store.add(triple)

            store.close()
            return True  # Return True if the upload is successful

        except Exception as e:
            print("The upload of data to Blazegraph failed: " + str(e))
            return False
        
# Example usage
base_url = "http://example.org/"
meta_file_path = "data/meta.csv"  # Replace with the actual path of your CSV file

handler = MetadataUploadHandler()  # Create an instance of MetadataUploadHandler
handler.dbPathOrUrl = "http://10.201.81.71:9999/blazegraph/sparql"  # Define Blazegraph endpoint
handler.pushDataToDb(meta_file_path)  # Push data from the CSV file to the database