from impl import IdentifiableEntity, CulturalHeritageObject, Person, Author, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedMaterial, PrintedVolume, Herbarium, Specimen, Painting, Model, Map, Handler, UploadHandler, MetadataUploadHandler, MetadataQueryHandler, ProcessDataUploadHandler, ProcessDataQueryHandler
import pandas as pd
import json  # Importazione del modulo standard json
import pprint
from pandas import DataFrame
from pandas import Series
from pandas import concat
from pandas import read_sql
from pandas import merge
import re
import numpy as np
import os
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.namespace import FOAF, DCTERMS, XSD
from rdflib import Namespace
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pprint import pprint
from json import load
from sqlite3 import connect
from SPARQLWrapper import SPARQLWrapper, JSON 
from unittest.mock import MagicMock
import logging 
from typing import List, Optional
from sparql_dataframe import get



class MetadataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()
        self.my_graph = Graph()  # RDF graph instance
        self.example = Namespace("http://example.org/")
        self.schema = Namespace("http://schema.org/")
        self.my_graph.bind("example", self.example)
        self.my_graph.bind("schema", self.schema)

        # Mapping types to URIs
        self.type_mapping = {
            "Nautical chart": URIRef("https://dbpedia.org/resource/Nautical_chart"),
            "Manuscript plate": URIRef("http://example.org/ManuscriptPlate"),
            "Manuscript volume": URIRef("https://dbpedia.org/resource/Category:Manuscripts_by_collection"),
            "Printed volume": URIRef("https://schema.org/PublicationVolume"),
            "Printed material": URIRef("http://example.org/PrintedMaterial"),
            "Herbarium": URIRef("https://dbpedia.org/resource/Herbarium"),
            "Specimen": URIRef("https://dbpedia.org/resource/Specimen"),
            "Painting": URIRef("https://dbpedia.org/resource/Category:Painting"),
            "Model": URIRef("https://dbpedia.org/resource/Category:Prototypes"),
            "Map": URIRef("https://dbpedia.org/resource/Category:Maps"),
            }

    def pushDataToDb(self, file_path: str) -> bool:
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return False

        try:
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            if df.empty:
                print("Error: The CSV file is empty or improperly formatted.")
                return False

            # Process each row into RDF triples
            for _, row in df.iterrows():
                self._processRow(row)

            # Upload the graph to Blazegraph
            return self._uploadGraphToBlazegraph()

        except Exception as e:
            print(f"Error processing CSV file {file_path}: {e}")
            return False

    def _processRow(self, row: pd.Series):
        """Processes a single row and adds RDF triples to the graph."""
        try:
            # Criar o sujeito com base no ID da linha
            subj = URIRef(self.example + str(row["Id"]))
            self.my_graph.add((subj, DCTERMS.identifier, Literal(row["Id"])))
            print(f"Processing ID: {row['Id']}")

            # Adicionar o tipo de objeto cultural
            if row.get("Type", "").strip() in self.type_mapping:
                self.my_graph.add((subj, RDF.type, self.type_mapping[row["Type"].strip()]))
                print(f"Added type: {row['Type']}")

            # Adicionar título
            if pd.notna(row.get("Title")):
                self.my_graph.add((subj, DCTERMS.title, Literal(row["Title"].strip())))
                print(f"Added title: {row['Title']}")

            # Adicionar data de criação
            if pd.notna(row.get("Date")):
                self.my_graph.add((subj, self.schema.dateCreated, Literal(row["Date"], datatype=XSD.string)))
                print(f"Added date: {row['Date']}")

            # Adicionar proprietário
            if pd.notna(row.get("Owner")):
                self.my_graph.add((subj, FOAF.maker, Literal(row["Owner"].strip())))
                print(f"Added owner: {row['Owner']}")

            # Adicionar localização
            if pd.notna(row.get("Place")):
                self.my_graph.add((subj, DCTERMS.spatial, Literal(row["Place"].strip())))
                print(f"Added place: {row['Place']}")

            # Processar os autores da linha
            if "Author" in row and pd.notna(row["Author"]):
                authors = row["Author"].split(",") if isinstance(row["Author"], str) else []
                for author_string in authors:
                    author_string = author_string.strip()

                    # Verificar se há um identificador VIAF ou ULAN no autor
                    author_id_match = re.search(r'\((VIAF|ULAN):(\d+)\)', author_string)
                    if author_id_match:
                        id_type = author_id_match.group(1)  # VIAF ou ULAN
                        id_value = author_id_match.group(2)  # O ID numérico
                        person_id = URIRef(f"http://example.org/person/{id_type}_{id_value}")
                    else:
                        # Fallback para URI baseado no nome do autor
                        person_id = URIRef(f"http://example.org/person/{author_string.replace(' ', '_')}")

                    # Adicionar o autor ao grafo
                    self.my_graph.add((person_id, DCTERMS.creator, subj))
                    self.my_graph.add((person_id, FOAF.name, Literal(author_string)))
                    print(f"Added author: {author_string}, ID: {person_id}")

        except Exception as e:
            print(f"Error processing row: {row}. Exception: {e}")
        
    def _uploadGraphToBlazegraph(self) -> bool:
        """Uploads the RDF graph to Blazegraph."""
        if not self.dbPathOrUrl:
            print("Error: SPARQL endpoint is not set. Use setDbPathOrUrl to configure it.")
            return False

        if len(self.my_graph) == 0:
            print("Error: RDF graph is empty. No data to upload.")
            return False

        try:
            store = SPARQLUpdateStore()
            store.open((self.dbPathOrUrl, self.dbPathOrUrl))

            for triple in self.my_graph.triples((None, None, None)):
                store.add(triple)

            store.close()
            print("Data successfully uploaded to Blazegraph.")
            return True

        except Exception as e:
            print(f"Error uploading to Blazegraph: {e}")
            return False
    
    def printRDFTriples(file_path: str, handler: MetadataUploadHandler):
        """Prints all RDF triples as they are prepared for Blazegraph."""
        try:
            if not os.path.exists(file_path):
                print(f"Error: File not found at {file_path}")
                return

            # Carregar o DataFrame
            df = pd.read_csv(file_path)

            if df.empty:
                print("The CSV file is empty or improperly formatted.")
                return

            # Processar cada linha do DataFrame
            for _, row in df.iterrows():
                handler._processRow(row)  # Reaproveitar a lógica de processamento do handler

            # Iterar sobre os triples criados
            print("RDF Triples prepared for Blazegraph:")
            for subj, pred, obj in handler.my_graph:
                print(f"{subj} {pred} {obj}")

        except Exception as e:
            print(f"Error processing or printing RDF triples: {e}")

    # Exemplo de uso
    file_path = "data/meta.csv"
    handler = MetadataUploadHandler()  # Instância do seu handler
    printRDFTriples(file_path, handler)