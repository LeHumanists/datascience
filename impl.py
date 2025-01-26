import ast
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

class IdentifiableEntity(object):
    def __init__(self, identifier):
        self.id = identifier
    
    def getId(self):
        return self.id

class Person(IdentifiableEntity):
    def __init__(self, name):
        self.name = name
        
    def getName(self):
        return self.name

# A L I C E, C A R L A
class Author(Person):
    def __init__(self, name, identifier=None):
        super().__init__(name)
        self.identifier = identifier 
        
    def getIdentifier(self):
        return self.identifier

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id:str, title:str, date:str, owner:str, place:str):
        super().__init__(id)
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.authors = [] 

    def getTitle(self):
        return self.title
    
    def getDate(self):
        return self.date
    
    def getOwner(self):
        return self.owner
    
    def getPlace(self):
        return self.place
    
    def addAuthor(self, author):
        if isinstance(author, Author): 
            self.authors.append(author)
    
    def removeAuthor(self, author):
        if author in self.authors:
            self.authors.remove(author)

    def getAuthors(self):
        return [author.getName() for author in self.authors]


class NauticalChart(CulturalHeritageObject):
    pass

class ManuscriptPlate(CulturalHeritageObject):
    pass

class ManuscriptVolume(CulturalHeritageObject):
    pass

class PrintedVolume(CulturalHeritageObject):
    pass

class PrintedMaterial(CulturalHeritageObject):
    pass

class Herbarium(CulturalHeritageObject):
    pass

class Specimen(CulturalHeritageObject):
    pass

class Painting(CulturalHeritageObject):
    pass

class Model(CulturalHeritageObject):
    pass

class Map(CulturalHeritageObject):
    pass

# F R A N C E S C A, M A T I L D E
class Activity(object):
    def __init__(self, institute, person, tools, start, end, refers_to):
        self.institute = institute
        self.person = person
        self.tools = list()
        for t in tools:
            self.tools.append(t)

        self.start = start
        self.end = end
        self.refers_to = refers_to
    
    def getResponsibleInstitute(self):
        return self.institute

    def getResponsiblePerson(self):
        if self.person:
            return self.person
        else:
            return None

    def getTools(self):
        # it returns a list of strings
        return self.tool

    def getStartDate(self):
        if self.start:
            return self.start
        else:
            return None

    def getEndDate(self):
        if self.end:
            return self.end
        else:
            return None

    def refersTo(self):
        return CulturalHeritageObject

class Acquisition(Activity):
    def __init__(self, institute, person, tools, start, end, refersTo, technique):
        self.technique = technique

        super().__init__(institute, person, tools, start, end, refersTo)
    
    def getTechnique(self):
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass

class Handler(object):
    def __init__(self, dbPathOrUrl=""):
        self.dbPathOrUrl = dbPathOrUrl

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, url):
        self.dbPathOrUrl = url
        return True  # Indicate success


class UploadHandler(Handler):
    def __init__(self, dbPathOrUrl=""):
        super().__init__(dbPathOrUrl)
    def pushDataToDb(self, file_path):
        pass
    
# C A R L A
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
        subj = URIRef(self.example + str(row["Id"]))
        self.my_graph.add((subj, DCTERMS.identifier, Literal(row["Id"])))

        if row.get("Type", "").strip() in self.type_mapping:
            self.my_graph.add((subj, RDF.type, self.type_mapping[row["Type"].strip()]))

        if pd.notna(row.get("Title")):
            self.my_graph.add((subj, DCTERMS.title, Literal(row["Title"].strip())))
        if pd.notna(row.get("Date")):
            self.my_graph.add((subj, self.schema.dateCreated, Literal(row["Date"], datatype=XSD.string)))
        if pd.notna(row.get("Owner")):
            self.my_graph.add((subj, FOAF.maker, Literal(row["Owner"].strip())))
        if pd.notna(row.get("Place")):
            self.my_graph.add((subj, DCTERMS.spatial, Literal(row["Place"].strip())))
            
        # Process authors
            authors = row["Author"].split(",") if isinstance(row["Author"], str) else []
            for author_string in authors:
                author_string = author_string.strip()
                
                # Use regex to find author ID with either VIAF or ULAN
                author_id_match = re.search(r'\((VIAF|ULAN):(\d+)\)', author_string)  # Match both VIAF and ULAN formats
                if author_id_match:
                    id_type = author_id_match.group(1)  # Either 'VIAF' or 'ULAN'
                    id_value = author_id_match.group(2)  # The numeric ID
                    person_id = URIRef(f"http://example.org/person/{id_type}_{id_value}")
                else:
                    # Fallback to a simple URI based on the author's name if no ID is found
                    person_id = URIRef(f"http://example.org/person/{author_string.replace(' ', '_')}")
                
                # Add the author information to the graph
                self.my_graph.add((person_id, DCTERMS.creator, subj))
                self.my_graph.add((person_id, FOAF.name, Literal(author_string, datatype=XSD.string)))

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

# M A T I L D E
class ProcessDataUploadHandler(UploadHandler):
    pass

    # the method of ProcessDataUploadHandler that pushes data to the rel db
    def pushDataToDb(self, file_path):

        # open json file
        with open("data/process.json", "r", encoding="utf-8") as f:
            json_data = load(f)
    
        # function for extracting data from json
        def data_from_json(json_object, dict_key):
            # initialize list empty
            process = []
            # set counter
            count = 0
            # iterate over dictionaries in the input list
            for item in json_object:
                # add 1 to the counter at each iteration
                count += 1
                # iterate over key,value pairs for each dictionary
                for key, val in item.items():
                    # check if key is equal to the input key
                    if key == dict_key:
                        # if it is, append val (the inner dictionary) to the process list
                        process.append(val)
                        # and add the object id to the inner dictionary
                        val.update(refers_to="object_" + str(count))
            
            # return the list        
            return process

        # function calls
        acquisition = data_from_json(json_data, "acquisition")
        processing = data_from_json(json_data, "processing")
        modelling = data_from_json(json_data, "modelling")
        optimising = data_from_json(json_data, "optimising")
        exporting = data_from_json(json_data, "exporting")

        print("Acquisition list:\n", acquisition)

        # function for populating dataframes from lists
        def populateDf(process_list): 
        
            # assign datatype string to the entire dataframe
            df = pd.DataFrame(process_list, dtype="string")
            # then cast "tool" column to dtype object
            df["tool"] = df["tool"].astype("object")
            
            return df

        # function calls
        acquisition_df = populateDf(acquisition)
        processing_df = populateDf(processing)
        modelling_df = populateDf(modelling)
        optimising_df = populateDf(optimising)
        exporting_df = populateDf(exporting)

        print("Acquisition dataframe:\n", acquisition_df)
        print("Acquisition dataframe info:", acquisition_df.info())

        # create unique identifiers and append id column to df
        def createUniqueId(process_df, df_name):
            id = []
            # iterate over dataframe rows
            for idx, row in process_df.iterrows():
                # at each iteration, append to the list a string composed by dataframe name + underscore + the index of the row to be used as unique identifier
                id.append(str(df_name) + "_" + str(idx))

            # convert the list to a Series and insert it as first column in the df, with name "unique_id"
            process_df.insert(0, "unique_id", Series(id, dtype="string"))

            return process_df

        # function calls
        createUniqueId(acquisition_df, "acquisition")
        createUniqueId(processing_df, "processing")
        createUniqueId(modelling_df, "modelling")
        createUniqueId(optimising_df, "optimising")
        createUniqueId(exporting_df, "exporting")

        print("Acquisition df with unique ids:\n", acquisition_df)

        # remove multi-valued attributes from df
        def keep_single_valued(process_df):
            # dtypes stores a series where the first column lists the index for the Series (the column names) and the second one the datatype for each column
            dtypes = process_df.dtypes
            print(isinstance(dtypes, pd.Series))
            # iterate over the columns in the Series
            for column_name, datatype in dtypes.items():
                # if the column has datatype object...
                if datatype == object:
                    #... create a sub-dataframe containing the column and the unique_id, associate a dtype to the column and drop the multi-valued column from the dataframe
                    multi_valued = process_df[["unique_id", column_name]]
                    multi_valued[column_name].astype("object")
                    process_df.drop("tool", axis=1, inplace=True)

            # return the df and the popped column
            return process_df, multi_valued
    
        # function calls
        acquisition_df, acquisition_multi_valued = keep_single_valued(acquisition_df)
        processing_df, processing_multi_valued = keep_single_valued(processing_df)
        modelling_df, modelling_multi_valued = keep_single_valued(modelling_df)
        optimising_df, optimising_multi_valued = keep_single_valued(optimising_df)
        exporting_df, exporting_multi_valued = keep_single_valued(exporting_df)
        print("Acquisition df and multi-valued df:\n", acquisition_df, acquisition_multi_valued)
        print(acquisition_df.info())
        print(acquisition_multi_valued.info())
        
        # create multi-valued attributes tables
        def create_multi_valued_tables(multi_valued_df):
            tools_dict = dict()
            for idx, row in multi_valued_df.iterrows():
                # populate dictionary with unique identifiers as keys and lists of tools as values
                tools_dict[row.iloc[0]] = ast.literal_eval(row.iloc[1]) if isinstance(row.iloc[1], str) else row.iloc[1]

            print(tools_dict)

            tools_unpacked = []
            identifiers_unpacked = []

            # iterate over each tool in the inner lists
            for tool_list in tools_dict.values():
                # and append it to the pandas series
                if len(tool_list) < 1:
                    tools_unpacked.append("")
                else:
                    for t in tool_list:
                        tools_unpacked.append(t)

            print("list for tools:\n", tools_unpacked)

            # iterate over the list of identifiers
            for identifier in tools_dict.keys():
                # and append each identifier to the series as many times as the length of the list which is the value of the key corresponding to the identifier in the tools_dict
                list_length = len(tools_dict[identifier])
                if list_length < 1:
                    identifiers_unpacked.append(identifier)
                else:
                    for n in range(list_length):
                        identifiers_unpacked.append(identifier)

            print("list for identifiers:\n", identifiers_unpacked)

            # create a list that contains the two series and join them in a dataframe where each series is a column
            tools_series = pd.Series(tools_unpacked, dtype="string", name="tool")
            identifiers_series = pd.Series(identifiers_unpacked, dtype="string", name="unique_id")
            tools_df = pd.concat([identifiers_series, tools_series], axis=1)
            print("The dataframe for tools:\n", tools_df)
            
            return tools_df

        # function calls...
        ac_tools_df = create_multi_valued_tables(acquisition_multi_valued)
        pr_tools_df = create_multi_valued_tables(processing_multi_valued)
        md_tools_df = create_multi_valued_tables(modelling_multi_valued)
        op_tools_df = create_multi_valued_tables(optimising_multi_valued)
        ex_tools_df = create_multi_valued_tables(exporting_multi_valued)

        # function for merging tools-id tables
        def merge_mv_tables(table_1, table_2, table_3, table_4, table_5):
            merged = concat([table_1, table_2, table_3, table_4, table_5], ignore_index=True)

            return merged
        
        # function calls
        merged_tools_df = merge_mv_tables(ac_tools_df, pr_tools_df, md_tools_df, op_tools_df, ex_tools_df)
        print("The merged dataframe:\n", merged_tools_df)
        
        # F R A N C E S C A
        # pushing tables to db
        with connect("relational.db") as con:
            acquisition_df.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_df.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_df.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_df.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_df.to_sql("Exporting", con, if_exists="replace", index=False)
            merged_tools_df.to_sql("Tools", con, if_exists="replace", index=False)
        
            # check and return result
            rel_db_ac = pd.read_sql("SELECT * FROM Acquisition", con)
            rel_db_pr = pd.read_sql("SELECT * FROM Processing", con)
            rel_db_md = pd.read_sql("SELECT * FROM Modelling", con)
            rel_db_op = pd.read_sql("SELECT * FROM Optimising", con)
            rel_db_ex = pd.read_sql("SELECT * FROM Exporting", con)
            rel_db_tl = pd.read_sql("SELECT * FROM Tools", con)

            populated_tables = not any(df.empty for df in [rel_db_ac, rel_db_pr, rel_db_op, rel_db_md, rel_db_ex, rel_db_tl]) # add rel_db_tl
            print(populated_tables)
            return populated_tables


class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl: str = ""):
        super().__init__(dbPathOrUrl)
        
    def getById(self, id: str):
        pass 

# A L I C E, C A R L A
class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    # C A R L A
    def getById(self, id: str) -> pd.DataFrame:
        object_query = f"""
        PREFIX schema: <https://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT DISTINCT ?id ?type ?title ?dateCreated ?maker ?spatial
        WHERE {{
            ?object dcterms:identifier '{id}' .
            ?object dcterms:identifier ?id .
            ?object rdf:type ?type .
            OPTIONAL {{ ?object dcterms:title ?title . }}
            OPTIONAL {{ ?object schema:dateCreated ?dateCreated . }}
            OPTIONAL {{ ?object foaf:maker ?maker . }}
            OPTIONAL {{ ?object dcterms:spatial ?spatial . }}
        }}
        """

        person_query = f"""
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT DISTINCT ?id ?name ?createdFor
        WHERE {{
            ?person dcterms:identifier '{id}' .
            ?person dcterms:identifier ?id .
            ?person foaf:name ?name .
            OPTIONAL {{ ?person dcterms:creator ?createdFor . }}
        }}
        """

        # Execute both queries
        object_df = self.execute_query(object_query)
        if not object_df.empty:
            return object_df

        person_df = self.execute_query(person_query)
        if not person_df.empty:
            return person_df

        # Print and return empty DataFrame if no results
        print(f"No data found for ID: {id}")
        return pd.DataFrame(columns=["id", "type", "title", "dateCreated", "maker", "spatial", "name", "createdFor"])

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SPARQL query and return the result as a DataFrame.
        """
        if not self.dbPathOrUrl:
            print("ERROR: SPARQL endpoint URL is not set. Use setDbPathOrUrl to configure it.")
            return pd.DataFrame()

        try:
            sparql = SPARQLWrapper(self.dbPathOrUrl)  # Usa o endpoint configurado
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            # Extrai os dados para um DataFrame
            columns = results["head"]["vars"]
            rows = [
                [binding.get(col, {}).get("value", None) for col in columns]
                for binding in results["results"]["bindings"]
            ]

            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"ERROR: Error executing SPARQL query on {self.dbPathOrUrl}: {e}")
            return pd.DataFrame()

    def getAllPeople(self) -> pd.DataFrame:
        """
        Fetch all people from the database.
        """
        query = """
        PREFIX dcterms: <http://purl.org/dc/terms/>
        SELECT DISTINCT ?personName ?personID
        WHERE {
            ?object dcterms:creator ?creator .
            BIND (STRAFTER(?creator, "(") AS ?personID) .
            BIND (STRBEFORE(?creator, " (") AS ?personName) .
        }
        ORDER BY ?personName
        """
        return self.execute_query(query)

    # A L I C E
    def getAllCulturalHeritageObjects(self) -> pd.DataFrame:
        """
        Fetch all cultural heritage objects from the database.
        """
        query = """
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?id ?title ?date ?owner ?place
        WHERE {
            ?object a <http://dbpedia.org/ontology/CulturalHeritageObject> .
            ?object dcterms:identifier ?id .
            ?object dcterms:title ?title .
            ?object schema:dateCreated ?date .
            ?object foaf:maker ?owner .
            ?object dcterms:spatial ?place .
        }
        ORDER BY ?title
        """
        return self.execute_query(query)

    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame:
        """
        Retrieve all authors of a specific cultural heritage object by its ID.
        """
        query = f"""
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?personName ?personID
        WHERE {{
            ?object dcterms:identifier "{object_id}" ;
                    dcterms:creator ?creator .
            ?creator foaf:name ?personName .
            BIND(STR(?creator) AS ?personID)
        }}
        ORDER BY ?personName
        """
        return self.execute_query(query)

    def getCulturalHeritageObjectsAuthoredBy(self, id_value: str) -> pd.DataFrame:
        """
        Retrieve all cultural heritage objects authored by a specific person by their ID.
        Returns a DataFrame.
        """
        query = f"""
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?objectID ?title ?date ?owner ?place
        WHERE {{
            ?object dcterms:creator <http://example.org/person/{id_value}> ;
                    dcterms:identifier ?objectID ;
                    dcterms:title ?title ;
                    schema:dateCreated ?date ;
                    foaf:maker ?owner ;
                    dcterms:spatial ?place .
        }}
        ORDER BY ?title
        """
        return self.execute_query(query)
        
        
acquisition_sql_df = DataFrame()
tool_sql_df= DataFrame()

# F R A N C E S C A, M A T I L D E
def query_rel_db():
    activities = DataFrame()
    with connect("relational.db") as con:
        queries = {
            "Acquisition": "SELECT * FROM Acquisition",
            "Processing": "SELECT * FROM Processing",
            "Modelling": "SELECT * FROM Modelling",
            "Optimising": "SELECT * FROM Optimising",
            "Exporting": "SELECT * FROM Exporting",
            "Tools": "SELECT * FROM Tools",
        }
        df_dict = {}
        for key, query in queries.items():
            df_dict[key] = read_sql(query, con)
            if df_dict[key].empty:
                print(f"Warning: {key} table is empty.")

    activities = pd.concat([df_dict["Acquisition"], df_dict["Processing"], df_dict["Modelling"], df_dict["Optimising"], df_dict["Exporting"]], ignore_index=True)
    tool_sql_df = df_dict["Tools"]
    activities = pd.merge(activities, tool_sql_df, left_on="unique_id", right_on="unique_id")

    return activities

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self, dbPathOrUrl = ""):
        super().__init__(dbPathOrUrl)
    
    def getById(self, id: str) -> pd.DataFrame:
        return pd.DataFrame()

    def getAllActivities(self):
        
        return query_rel_db()

    # F R A N C E S C A
    def getActivitiesByResponsibleInstitution(self, partialName):
        activities = query_rel_db()
        institution_df = DataFrame()
        # handle empty input strings
        if not partialName:
           print("The input string is empty.")
           return institution_df
        else:
            # filter the df based on input string
            cleaned_input = partialName.lower().strip()
            institution_df = activities[activities["responsible institute"].str.lower().str.strip().str.contains(cleaned_input) | activities["responsible institute"].str.lower().str.strip().eq(cleaned_input)]

    # handle non matching inputs
            if institution_df.empty:
                print("No match found.")

        return institution_df
    
    # F R A N C E S C A 
    def getActivitiesByResponsiblePerson(self, partialName):
        activities = query_rel_db()
        person_df = DataFrame()
        #handle empty input strings
        if not partialName:
            print("The input string is empty.")
            return person_df
        else:
            #filter the df based on input string
            cleaned_input = partialName.lower().strip()
            person_df = activities[activities["responsible person"].str.lower().str.strip().str.contains(cleaned_input) | activities["responsible person"].str.lower().str.strip().eq(cleaned_input)]

            # handle non matching inputs
            if person_df.empty:
                print("No match found.")

        return person_df
    
    # M A T I L D E
    def getActivitiesStartedAfter(self, date):
        activities = query_rel_db()
        start_date_df = DataFrame()

        activities.columns = activities.columns.str.strip()
        print("Columns in the activities df:", activities.columns)

        start_date_df = activities[(activities["start date"] >= date) & (activities["start date"] != '')]
        
        if start_date_df.empty:
            print("No match found.")
        
        return start_date_df

    # M A T I L D E
    def getActivitiesEndedBefore(self, date):
        activities = query_rel_db()
        end_date_df = DataFrame()

        end_date_df = activities[(activities["end date"] <= date) & (activities["end date"] != '')]
        
        if end_date_df.empty:
            print("No match found.")
        
        return end_date_df

    # F R A N C E S C A
    def getAcquisitionsByTechnique(self, inputtechnique):
        # fetch Acquisition table
        with connect("relational.db") as con:
            query1 = "SELECT * FROM Acquisition"
            query2 = "SELECT * FROM Tools"

            acquisition_sql_df = read_sql(query1, con)
            tool_sql_df = read_sql(query2, con)

        merged_df = pd.merge(acquisition_sql_df, tool_sql_df, on="unique_id", how="inner")
        filtered_df = merged_df[merged_df["technique"].str.contains(inputtechnique, case=False, na=False)]

        if filtered_df.empty:
            print("No match found")  

        return filtered_df

    # F R A N C E S C A
    def getActivitiesUsingTool(self, tool):
        activities = query_rel_db()
        # Normalize the tool string for comparison
        tool_lower = tool.lower()
    
        # Filter rows where the tool column matches the exact or partial tool name
        activities_tool = activities[activities['tool'].str.lower().str.contains(tool_lower,  case=False, na=False)]

        if activities_tool.empty:
            print("No match found")

        return activities_tool

# C A R L A    
class BasicMashup(object):
    def __init__(self):
        self.metadataQuery = []  
        self.processQuery = []

        # Mapping object types to their corresponding subclasses
        self.type_mapping = {  # C A R L A
            "Nautical chart": NauticalChart,
            "Manuscript plate": ManuscriptPlate,
            "Manuscript volume": ManuscriptVolume,
            "Printed volume": PrintedVolume,
            "Printed material": PrintedMaterial,
            "Herbarium": Herbarium,
            "Specimen": Specimen,
            "Painting": Painting,
            "Model": Model,
            "Map": Map,
        }

    def cleanMetadataHandlers(self):
        self.metadataQuery = [] 
        return True

    def cleanProcessHandlers(self):
        self.processQuery = [] 
        return True

    def addMetadataHandler(self, handler: MetadataQueryHandler) -> bool:
        self.metadataQuery.append(handler)
        return True

    def addProcessHandler(self, handler: ProcessDataQueryHandler) -> bool:
        self.processQuery.append(handler)
        return True

    def _createEntityObject(self, entity_data: dict) -> IdentifiableEntity: # C A R L A
        entity_type = entity_data.get("type", None)
        entity_id = entity_data.get("id")
        if entity_type in self.type_mapping:  # Use the type mapping to determine the correct class
            cls = self.type_mapping[entity_type]
            return cls(**entity_data)  # Instantiate the appropriate class
        else:
            return IdentifiableEntity(entity_id)  # Fallback to a generic IdentifiableEntity

    def _createObjectList(self, df: pd.DataFrame) -> List[IdentifiableEntity]: # C A R L A
        object_list = []  # Initialize an empty list for storing objects
        for _, row in df.iterrows():  # Iterate over rows in the DataFrame
            entity_data = row.to_dict()  # Convert each row to a dictionary
            obj = self._createEntityObject(entity_data)  # Create an object using _createEntityObject
            object_list.append(obj)  # Append the created object to the list
        return object_list
    
    def combineAuthorsOfObjects(self, df, handler):  # A L I C E
        if "authors" in df.columns:
        # Iterate over all rows of the DataFrame
            for idx, row in df.iterrows():
                # Check that the "authors" column is not empty or None
                if row["authors"] not in [None, ""]:
                    object_id = row["id"]
                    
                    # Retrieve the authors for the object
                    authors_df = handler.getAuthorsOfCulturalHeritageObject(object_id)
                    
                    # If the authors DataFrame is not empty
                    if authors_df is not None and not authors_df.empty:
                        # Add the object ID and the combination of author name and ID
                        authors_df["auth"] = authors_df["authorName"].astype(str) + "-" + authors_df["authorId"].astype(str)
                        authors_df["id"] = str(object_id)
                        
                        # If there are multiple authors, join them with a semicolon
                        if authors_df.shape[0] > 1:
                            authors_combined = ";".join(authors_df["auth"])
                            df.at[idx, "authors"] = authors_combined
                        else:
                            # Otherwise, take the single author
                            df.at[idx, "authors"] = authors_df["auth"].iloc[0]
                    else:
                        # If there are no authors, leave the "authors" column empty
                        df.at[idx, "authors"] = ""
        
        # Remove duplicate rows and return the modified DataFrame
        return df.drop_duplicates()

    def getEntityById(self, entity_id: str) -> Optional[IdentifiableEntity]:  # C A R L A
        if not self.metadataQuery:  # Return None if no metadata handlers are available
            return None
        for handler in self.metadataQuery:  # Iterate over metadata handlers to query by ID
            try:
                df = handler.getById(entity_id)  # Query data using the handler and retrieve as a DataFrame
                if not df.empty:  # Check if the DataFrame is not empty
                    if 'type' in df.columns:  # Check for Cultural Heritage Objects based on 'type'
                        cho_list = self._createObjectList(df)
                        if cho_list:
                            return cho_list[0]  # Return the first matching object
                    elif 'name' in df.columns and 'id' in df.columns:  # Handle Person instances based on 'name' and 'id'
                        return Person(df.iloc[0]["id"], df.iloc[0]["name"])
            except Exception as e:  # Print an error message if querying fails
                print(f"Error retrieving entity by ID {entity_id} from handler {handler}: {e}")
        return None

    def getAllPeople(self):   # C A R L A
        person_list = []  # Initialize an empty list for storing people objects
        if self.metadataQuery:  # Check if there are any metadata handlers available
            new_person_df_list = [handler.getAllPeople() for handler in self.metadataQuery]  # Retrieve DataFrames from all handlers
            new_person_df_list = [df for df in new_person_df_list if not df.empty]  # Filter out empty DataFrames
            if new_person_df_list:  # Proceed only if there are valid DataFrames to merge
                merged_df = pd.concat(new_person_df_list, ignore_index=True).drop_duplicates(subset=["personID"], keep="first")  # Merge and deduplicate DataFrames
                # Safely check for None or empty strings
                person_list = [
                    Person(row["personID"], row["personName"])
                    for _, row in merged_df.iterrows()
                    if row["personID"] and row["personName"]  # Ensure values are not None or NaN
                    and str(row["personID"]).strip()  # Convert to string and strip
                    and str(row["personName"]).strip()
                ]
        return person_list

    def getAllCulturalHeritageObjects(self):  # A L I C E
        """
        Returns a list of objects of the class CulturalHeritageObject (or its subclasses).
        Integrates related person information into these objects.
        """
        # Ensure metadataQuery handlers are available
        if not self.metadataQuery:
            return []

        # List to collect DataFrames from handlers
        df_list = []

        # Iterate over metadata handlers to retrieve DataFrames
        for handler in self.metadataQuery:
            try:
                df_objects = handler.getAllCulturalHeritageObjects()
                if df_objects is not None and not df_objects.empty:
                    df_list.append(df_objects)
            except Exception as e:
                print(f"Error retrieving objects from handler {handler}: {e}")

        # If no data was retrieved, return an empty list
        if not df_list:
            return []

        # Merge all DataFrames, remove duplicates, and handle missing values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        # List to store the created objects
        cultural_heritage_objects = []

        # Iterate through each row of the DataFrame
        for _, row in df_union.iterrows():
            obj_type = row['type']  # Get the object type

            # Check if the type is mapped to a subclass
            if obj_type in self.type_mapping:
                # Retrieve the corresponding class
                object_class = self.type_mapping[obj_type]

                # Create an instance of the cultural heritage object
                obj = object_class(
                    id=str(row['id']),       # Object ID
                    title=row['title'],      # Object title
                    date=str(row['date']),   # Object date
                    owner=row['owner'],      # Object owner
                    place=row['place'],      # Object place
                )
                cultural_heritage_objects.append(obj)
            else:
                print(f"Warning: Object type {obj_type} not found in type mapping.")

        return cultural_heritage_objects
    
    def getAuthorsOfCulturalHeritageObject(self, id: str):  # A L I C E
        # Check if there are any available handlers
        if not self.metadataQuery:
            return []  # Return an empty list if there are no handlers available
    
        # List to store valid DataFrames retrieved from each handler
        df_list = []
    
        # Iterate through the handlers to collect author data
        for handler in self.metadataQuery:
            # Retrieve the authors' data for the given object ID from the handler
            df = handler.getAuthorsOfCulturalHeritageObject(id)
        
            # If the DataFrame is not empty, add it to the list
            if df is not None and not df.empty:  # Check for None as well
                df_list.append(df)
    
        # If there are no valid DataFrames, return an empty list
        if not df_list:
            return []
    
        # Concatenate the DataFrames, remove duplicates, and handle NaN values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates(subset=["authorId"]).fillna("")
    
        # List of authors (avoid empty or invalid authors)
        author_result_list = []
        for _, row in df_union.iterrows():
            # Strip leading and trailing whitespace from the author's name
            author = row['authorName'].strip()
            # If the author name is empty, return None
            if author == "":
                return None
            else:
                # Create a Person object for the author using their ID and name
                person = Person(id=str(row["authorId"]), name=author)
                # Append the Person object to the result list
                author_result_list.append(person)
            
        # Return the list of author objects
        return author_result_list

    def getCulturalHeritageObjectsAuthoredBy(self, authorId: str) -> List[CulturalHeritageObject]:  # A L I C E
        """
        Returns a list of CulturalHeritageObject instances authored by the person identified by the input ID.
        """
        if not self.metadataQuery:
            return []  # No metadata handlers available

        df_list = []
        for handler in self.metadataQuery:
            df = handler.getCulturalHeritageObjectsAuthoredBy(authorId)
            if not df.empty:
                df_list.append(df)

        if not df_list:
            return []  # No data retrieved

        # Combine all DataFrames
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates()

        # Convert DataFrame rows to CulturalHeritageObject instances
        object_list = []
        for _, row in df_union.iterrows():
            entity_data = row.to_dict()
            obj = self._createEntityObject(entity_data)  # Convert row to an appropriate object
            object_list.append(obj)

        return object_list

    # methods for relational db start here
    # F R A N C E S C A, M A T I L D E
    def getAllActivities(self):
        activities_df = pd.DataFrame()
        activities_df_list = []
        if self.processQuery:
            for process_qh in self.processQuery:
                activities_df = process_qh.getAllActivities()
                activities_df_list.append(activities_df)

            concat_df = pd.concat(activities_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates() # drop only identical rows
        
        else:
            print("No processQueryHandler found")
        
        updated_df = join_tools(concat_df_cleaned)
        print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
        

    def getActivitiesByResponsibleInstitution(self, partialName):
        institution_df = pd.DataFrame()
        institution_df_list = []
        if self.processQuery:
            for process_qh in self.processQuery:
                institution_df = process_qh.getActivitiesByResponsibleInstitution(partialName)
                institution_df_list.append(institution_df)

            concat_df = pd.concat(institution_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates()
        
        else:
            print("No processQueryHandler found")
        
        updated_df = join_tools(concat_df_cleaned)
        print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
    

    def getActivitiesByResponsiblePerson(self, partialName):
        person_df = pd.DataFrame()
        person_df_list = []
        if self.processQuery:
            for process_qh in self.processQuery:
                person_df = process_qh.getActivitiesByResponsiblePerson(partialName)
                person_df_list.append(person_df)

            concat_df = pd.concat(person_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates()
        
        else:
            print("No processQueryHandler found")
        
        updated_df = join_tools(concat_df_cleaned)
        print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
    

    def getActivitiesStartedAfter(self, date):
        started_after_df = pd.DataFrame()
        started_after_df_list = []
        if self.processQuery:
            for process_qh in self.processQuery:
                started_after_df = process_qh.getActivitiesStartedAfter(date)
                started_after_df_list.append(started_after_df)

            concat_df = concat(started_after_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates()

        else:
            print("No processQueryHandler found")
        
        updated_df = join_tools(concat_df_cleaned)
        print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
    

    def getActivitiesEndedBefore(self, date):
        ended_before_df = pd.DataFrame()
        ended_before_df_list = []
        if self.processQuery:
            for process_qh in self.processQuery:
                ended_before_df = process_qh.getActivitiesEndedBefore(date)
                ended_before_df_list.append(ended_before_df)

            concat_df = concat(ended_before_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates()

        else:
            print("No processQueryHandler found")
        
        updated_df = join_tools(concat_df_cleaned)
        print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
    

    def getAcquisitionsByTechnique(self, inputtechnique):
        technique_df = pd.DataFrame()
        technique_df_list = []
        if self.processQuery:
            for process_qh in self.processQuery:
                technique_df = process_qh.getAcquisitionsByTechnique(inputtechnique)
                technique_df_list.append(technique_df)

            concat_df = concat(technique_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates()

        else:
            print("No processQueryHandler found")

        updated_df = join_tools(concat_df_cleaned)
        print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
    

    def getActivitiesUsingTool(self, tool):
        if self.processQuery:
            act_activities_tool_list = [process_qh.getActivitiesUsingTool(tool) for process_qh in self.processQuery]
            
            concat_df = concat(act_activities_tool_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates()

        else:
            print("No processQueryHandler found")

        updated_df = join_tools(concat_df_cleaned)
        print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
    

def instantiate_class(activity_df):
    activity_list = []
    activity_mapping = {
        "acquisition": Acquisition,
        "processing": Processing,
        "modelling": Modelling,
        "optimising": Optimising,
        "exporting": Exporting
    }
    #print("The input activity df:", activity_df)
    for idx, row in activity_df.iterrows():
        activity_from_id = re.sub("_\\d+", "", row["unique_id"])
        #print("The current activity:", activity_from_id)
        if activity_from_id in activity_mapping.keys() and activity_from_id == "acquisition":
            activity_obj = Acquisition(row["responsible institute"], row["responsible person"], row["tool"], row["start date"], row["end date"], row["refers_to"], row["technique"])
            activity_list.append(activity_obj)
        elif activity_from_id in activity_mapping.keys() and activity_from_id != "acquisition":
            class_to_use = activity_mapping.get(activity_from_id)
            activity_obj = class_to_use(row["responsible institute"], row["responsible person"], row["tool"], row["start date"], row["end date"], row["refers_to"])
            activity_list.append(activity_obj)
    
    return activity_list

# M A T I L D E
def join_tools(activity_df):
    # ensure the tool column in the df has dtype object
    activity_df["tool"] = activity_df["tool"].astype("object")
    #print(activity_df["tool"].apply(type).unique())
    # create a sub dataframe with just the unique id and tool column
    tools_subdf = activity_df[["unique_id", "tool"]]
    # iterate over sub dataframe grouping by unique id
    for unique_id, group in tools_subdf.groupby(["unique_id"]):
        #print(f"Current unique id is {unique_id} and current group is {group}")
        # convert tools to list and then join them
        concatenated_tools = ", ".join(group["tool"].to_list())
        #print(f"Concatenated tools for {unique_id}:", concatenated_tools)
        # update the row that matches the unique_id in the tuple with the content of the concatenated tools variable
        activity_df.loc[activity_df["unique_id"] == unique_id[0], "tool"] = concatenated_tools
    
    # convert the strings in the column to lists and assign the resul to a new variable
    new_tool_col = activity_df["tool"].str.split(", ")
    # reassign the tool column
    activity_df["tool"] = new_tool_col
    # drop identical rows
    activity_df_updated = activity_df.drop_duplicates(subset=['unique_id'])

    # check 
    #print("updated dataframe:", activity_df_updated.query("unique_id == 'optimising_27'"))
    return activity_df_updated

class AdvancedMashup(BasicMashup):
    def __init__(self):
        super().__init__()  # Inherit initialization from BasicMashup

    def getActivitiesOnObjectsAuthoredBy(self, person_id: str):  # C A R L A
        """Retrieve activities related to cultural heritage objects authored by a specific person."""
        activities = []

        # Validate inputs
        if not person_id or not self.metadataQuery or not self.processQuery:
            print("Error: Missing person_id, metadataQuery, or processQuery.")
            return activities

        # Retrieve cultural heritage objects authored by the person
        authored_objects = []
        for metadata_handler in self.metadataQuery:
            try:
                authored_objects.extend(metadata_handler.getCulturalHeritageObjectsAuthoredBy(person_id))
            except Exception as e:
                print(f"Error querying authored objects from metadata handler: {e}")

        if not authored_objects:
            return activities  # No authored objects found, return empty list

        # Debug: Check authored_objects content
        print("Authored objects:", authored_objects)
        print("Types in authored_objects:", [type(obj) for obj in authored_objects])

        # Retrieve all activities
        all_activities = []
        for process_handler in self.processQuery:
            try:
                all_activities.extend(process_handler.getAllActivities())
            except Exception as e:
                print(f"Error querying activities from process handler: {e}")

        # Match activities to authored objects
        authored_object_ids = {
            obj.getId() for obj in authored_objects if hasattr(obj, "getId")
        }
        for activity in all_activities:
            if any(object_id in activity.getRelatedObjectIds() for object_id in authored_object_ids):
                activities.append(activity)

        return activities

    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:  # A L I C E
        """Retrieve cultural heritage objects involved in activities handled by a specific person."""
        objects_list = []

        # Validate input
        if not partialName or not self.processQuery or not self.metadataQuery:
            return objects_list

        # Retrieve activities by responsible person
        activities = self.getActivitiesByResponsiblePerson(partialName)

        # Retrieve all cultural heritage objects
        all_objects = self.getAllCulturalHeritageObjects()

        # Match objects to activities
        object_ids = {activity.refersTo_cho.id for activity in activities if activity.refersTo_cho}
        for cho in all_objects:
            if cho.id in object_ids:
                objects_list.append(cho)

        return objects_list

    def getObjectsHandledByResponsibleInstitution(self, institution: str) -> list[CulturalHeritageObject]:  # F R A N C E S C A
        """Retrieve cultural heritage objects involved in activities handled by a specific institution."""
        objects_list = []

        # Validate input
        if not institution or not self.processQuery or not self.metadataQuery:
            return objects_list

        # Retrieve activities by responsible institution
        activities = self.getActivitiesByResponsibleInstitution(institution)

        # Retrieve all cultural heritage objects
        all_objects = self.getAllCulturalHeritageObjects()

        # Match objects to activities
        object_ids = {activity.refersTo_cho.id for activity in activities if activity.refersTo_cho}
        for cho in all_objects:
            if cho.id in object_ids:
                objects_list.append(cho)

        return objects_list
    
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start, end):  # M A T I L D E
        if not self.metadataQuery:  # Check if there are any handlers in the list
            raise ValueError("No MetadataQueryHandler has been added to AdvancedMashup.")
    
        query_result = []
    
        # Dynamically retrieve the endpoint from the first MetadataQueryHandler
        metadata_handler = self.metadataQuery[0]  # Use the first handler
        endpoint = metadata_handler.getDbPathOrUrl()
        if not endpoint:
            raise ValueError("The endpoint has not been set in the MetadataQueryHandler.")
    
        # SPARQL query
        sparql_query = """
        PREFIX dcterms: <http://purl.org/dc/terms>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    
        SELECT ?object ?author ?name
        WHERE {
            ?author dcterms:creator ?object .
            ?author foaf:name ?name .
        }
        """
    
        # Execute the SPARQL query
        authors_cho_df = get(endpoint, sparql_query, True)
        print("Authors and objects dataframe:\n", authors_cho_df)
    
        # Associate IDs to object URIs
        objects_id = []
        for idx, row in authors_cho_df.iterrows():
            if row["object"]:
                slug = row["object"].split("/")[-1]
                objects_id.append("object_" + slug)
            else:
                print(f"Warning: No object associated to {authors_cho_df['author'].iloc[idx]}")
    
        authors_cho_df.insert(3, "objects_id", pd.Series(objects_id, dtype="string"))
        print("Dataframe with IDs:\n", authors_cho_df)
    
        # SQL query
        with connect("relational.db") as con:
            sql_query = "SELECT `start date`, `end date`, `refers_to` FROM Acquisition"
            acq_timeframe_df = read_sql(sql_query, con)
    
        # Merge the resulting dataframes
        merged = pd.merge(authors_cho_df, acq_timeframe_df, left_on="objects_id", right_on="refers_to", how="inner")
        print("Merged dataframe:\n", merged)
    
        # Filter rows based on the time range
        merged[['start date', 'end date']] = merged[['start date', 'end date']].replace("", pd.NA)
        merged = merged.dropna(subset=["start date", "end date"])
        result_df = merged[(merged["start date"] >= start) & (merged["end date"] <= end)]
    
        # Create a list of Person objects for the result
        for _, row in result_df.iterrows():
            author_uri = row["author"]
            name = row["name"]
            author_id = author_uri.split("/")[-1].replace("_", ":")
            query_result.append(Person(author_id, name))
    
        return query_result
