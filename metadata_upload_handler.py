import pandas as pd
from rdflib import RDF, Graph, URIRef, Literal
from rdflib.namespace import FOAF, DC, DCTERMS, XSD
from rdflib import Namespace
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from graph_class import CulturalHeritageObject, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map
from handler import Handler, UploadHandler

from urllib.error import URLError
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

class MetadataUploadHandler(UploadHandler): 
    def __init__(self):
        super().__init__()
        self.my_graph = Graph()


#URIs for classes of resources

NauticalChart = URIRef("https://dbpedia.org/page/Nautical_chart")
ManuscriptPlate = URIRef("http://example.org/ManuscriptPlate")
ManuscriptVolume = URIRef("https://dbpedia.org/page/Category:Manuscripts_by_collection")
PrintedVolume = URIRef("https://schema.org/PublicationVolume")
PrintedMaterial = URIRef("http://example.org/PrintedMaterial")
Herbarium = URIRef("https://dbpedia.org/page/Herbarium")
Specimen = URIRef("https://dbpedia.org/page/Specimen")
Painting = URIRef("https://dbpedia.org/page/Category:Painting")
Model = URIRef("https://dbpedia.org/page/Category:Prototypes")
Map = URIRef ("https://dbpedia.org/page/Category:Maps")


example = Namespace("http://example.org/")
schema = Namespace("http://schema.org/")
dbpedia = Namespace("http://dbpedia.org/resource/")


df = pd.read_csv("data/meta.csv")

graph = Graph()
graph.bind("schema", schema)
graph.bind("dbpedia", dbpedia)
people_counter = 0

for _, row in df.iterrows():
    CulturalHeritageObject_uri = URIRef(example[row["Id"]])
    
    graph.add((CulturalHeritageObject_uri, DCTERMS.identifier, Literal(row["Id"], datatype=XSD.string)))
    graph.add((CulturalHeritageObject_uri, schema.additionalType, Literal(row["Type"], datatype=XSD.string)))
    graph.add((CulturalHeritageObject_uri, DCTERMS.title, Literal(row["Title"], datatype=XSD.string)))
    graph.add((CulturalHeritageObject_uri, DCTERMS.created, Literal(row["Date"], datatype=XSD.string)))
    graph.add((CulturalHeritageObject_uri, DCTERMS.spatial, Literal(row["Place"], datatype=XSD.string)))
    graph.add((CulturalHeritageObject_uri, schema.Property, Literal(row["Owner"], datatype=XSD.string)))
    
    authors = row["Author"].split(",") if isinstance(row["Author"], str) else []
    for author in authors:
        author = author.strip()
        people_counter += 1
        person_id = URIRef(f"http://example.org/person/{people_counter}")
        graph.add((CulturalHeritageObject_uri, DCTERMS.creator, person_id))
        graph.add((person_id, FOAF.name, Literal(author, datatype=XSD.string)))



store = SPARQLUpdateStore()
endpoint_url = "http://10.201.81.16:9999/blazegraph/sparql"

try:
    store.open((endpoint_url, endpoint_url))
    for triple in graph.triples((None, None, None)):
        store.add(triple)
    store.close()
    print("Data uploaded successfully.")
except URLError as e:
    print(f"Failed to connect to SPARQL endpoint: {e}")
except Exception as ex:
    print(f"An error occurred: {ex}")

store.open((endpoint_url, endpoint_url))
for triple in graph.triples((None, None, None)):
    store.add(triple)
store.close()


