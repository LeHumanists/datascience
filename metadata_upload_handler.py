import pandas as pd
from pandas import DataFrame
import re
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import FOAF, DCTERMS, XSD
from rdflib import Namespace
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graph_class import CulturalHeritageObject, Author 
from handler import UploadHandler

# Define a class to hold URIs for resources
class ResourceURIs:
    NauticalChart = URIRef("https://dbpedia.org/page/Nautical_chart")
    ManuscriptPlate = URIRef("http://example.org/ManuscriptPlate")
    ManuscriptVolume = URIRef("https://dbpedia.org/page/Category:Manuscripts_by_collection")
    PrintedVolume = URIRef("https://schema.org/PublicationVolume")
    PrintedMaterial = URIRef("http://example.org/PrintedMaterial")
    Herbarium = URIRef("https://dbpedia.org/page/Herbarium")
    Specimen = URIRef("https://dbpedia.org/page/Specimen")
    Painting = URIRef("https://dbpedia.org/page/Category:Painting")
    Model = URIRef("https://dbpedia.org/page/Category:Prototypes")
    Map = URIRef("https://dbpedia.org/page/Category:Maps")

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
            # Create a CulturalHeritageObject instance from the row data
            cultural_object = CulturalHeritageObject(
                id=str(row["Id"]),
                title=row["Title"].strip(),
                date=(row["Date"]),
                owner=row["Owner"],
                place=row["Place"]
            )

            # Add authors to the cultural object
            if isinstance(row["Author"], str) and row["Author"].strip():
                authors = row["Author"].strip('\"').strip().strip(";").split(";")  # Split authors by ";"
                for author_string in authors:
                    # Use regex to find author ID with either VIAF or ULAN
                    author_id_match = re.search(r'\((VIAF|ULAN):(\d+)\)', author_string)  # Match both VIAF and ULAN formats
                    if author_id_match:
                        identifier_type = author_id_match.group(1)  # Get either 'VIAF' or 'ULAN'
                        permanent_identifier = author_id_match.group(2).strip()  # Extract the number
                        author_name = author_string.split("(")[0].strip()  # Extract author name before parentheses
                        
                        # Create Author instance
                        author_instance = Author(name=author_name, identifier=permanent_identifier)

                        # Construct the URI based on whether it's VIAF or ULAN
                        if identifier_type == "VIAF":
                            author_uri = URIRef(f"http://viaf.org/viaf/{permanent_identifier}")  # Use VIAF URI format
                        elif identifier_type == "ULAN":
                            author_uri = URIRef(f"http://vocab.getty.edu/ulan/{permanent_identifier}")  # Use ULAN URI format

                        # Add to cultural object
                        cultural_object.addAuthor(author_instance)

                        # Add authors to the RDF graph using their permanent identifiers
                        self.my_graph.add((cultural_object.id, DCTERMS.creator, author_uri))
                        self.my_graph.add((author_uri, FOAF.name, Literal(author_instance.getName())))
                        self.my_graph.add((author_uri, DCTERMS.identifier, Literal(permanent_identifier)))

            # Now add this cultural object to the RDF graph
            self.add_cultural_object_to_graph(cultural_object)

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

    def add_cultural_object_to_graph(self, cultural_object):
        # Create a URI for the cultural object based on its ID
        cultural_object_uri = URIRef(self.example[cultural_object.id])
        
        # Add properties of the cultural object to the RDF graph
        self.my_graph.add((cultural_object_uri, DCTERMS.identifier, Literal(cultural_object.id, datatype=XSD.string)))
        self.my_graph.add((cultural_object_uri, DCTERMS.title, Literal(cultural_object.title, datatype=XSD.string)))
        
        if cultural_object.date != "":
            self.my_graph.add((cultural_object_uri, self.schema.dateCreated, Literal(cultural_object.date, datatype=XSD.date)))
        
        if cultural_object.place:
            self.my_graph.add((cultural_object_uri, DCTERMS.spatial, Literal(cultural_object.place, datatype=XSD.string)))

        if cultural_object.owner:
            self.my_graph.add((cultural_object_uri, FOAF.maker, Literal(cultural_object.owner)))

# Example usage
base_url = "http://example.org/"
meta_file_path = "data/meta.csv"  # Replace with the actual path of your CSV file

handler = MetadataUploadHandler()  # Create an instance of MetadataUploadHandler
handler.dbPathOrUrl = "http://10.201.81.71:9999/blazegraph/sparql"  # Define Blazegraph endpoint
handler.pushDataToDb(meta_file_path)  # Push data from the CSV file to the database