import pandas as pd
import json
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
    def __init__(self, id: str, name: str):
        super().__init__(str(id))
        self.name = name

    def getName(self):
        return self.name

# A L I C E, C A R L A

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
        if isinstance(author, Person): 
            self.authors.append(author)
    
    def removeAuthor(self, author):
        if author in self.authors:
            self.authors.remove(author)



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

# F R A N C E S C A,  M A T I L D E
class Activity(object):
    def __init__(self, institute, person, tool, start, end, refers_to):
        self.institute = institute
        self.person = person
        self.tools = list()
        for t in tool:
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
        result = set()
        for t in self.tools:
            result.add(t)
        return result 
    
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
        return self.refers_to

class Acquisition(Activity):
    def __init__(self, institute, person, tools, start, end, refers_to, technique):
        self.technique = technique

        super().__init__(institute, person, tools, start, end, refers_to)
    
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
        return True


class UploadHandler(Handler):
    def __init__(self, dbPathOrUrl=""):
        super().__init__(dbPathOrUrl)
    def pushDataToDb(self, file_path):
        pass

# UploadHandler has no additional attributes
""" class UploadHandler(Handler):
    pass

    def pushDataToDb(self, file_path):
        pass """

# C A R L A
class MetadataUploadHandler(UploadHandler):
    def __init__(self, dbPathOrUrl=""):
        super().__init__(dbPathOrUrl)  

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
        try:
            # Load the CSV file into a DataFrame
            df = pd.read_csv(file_path)


            # Process each row into RDF triples
            for _, row in df.iterrows():
                self.processRow(row)

            # Upload the graph to Blazegraph
            return self.uploadGraphToBlazegraph()

        except Exception as e:
            print(f"Error processing CSV file {file_path}: {e}")
            return False

    def processRow(self, row: pd.Series):
        """Processes a single row and adds RDF triples to the graph."""
        try:
            # Create the subject
            subj = URIRef(self.example + str(row["Id"]))
            self.my_graph.add((subj, DCTERMS.identifier, Literal(row["Id"])))
            print(f"Processing ID: {row['Id']}")

            # add the type
            if row.get("Type", "").strip() in self.type_mapping:
                self.my_graph.add((subj, RDF.type, self.type_mapping[row["Type"].strip()]))
                print(f"Added type: {row['Type']}")

            # Add title
            if pd.notna(row.get("Title")):
                self.my_graph.add((subj, DCTERMS.title, Literal(row["Title"].strip())))
                print(f"Added title: {row['Title']}")

            # Add creation date
            if pd.notna(row.get("Date")):
                self.my_graph.add((subj, self.schema.dateCreated, Literal(row["Date"], datatype=XSD.string)))
                print(f"Added date: {row['Date']}")

            # Add owner
            if pd.notna(row.get("Owner")):
                self.my_graph.add((subj, FOAF.maker, Literal(row["Owner"].strip())))
                print(f"Added owner: {row['Owner']}")

            # Add place
            if pd.notna(row.get("Place")):
                self.my_graph.add((subj, DCTERMS.spatial, Literal(row["Place"].strip())))
                print(f"Added place: {row['Place']}")

            # Process the authors in the line
            if "Author" in row and pd.notna(row["Author"]):
                authors = row["Author"].split(";") if isinstance(row["Author"], str) else []

                for author_string in authors:
                    author_string = author_string.strip()

                    # Extract identifier, e.g. (VIAF:123456)
                    match = re.search(r"\(([\w\-]+):([\w\-]+)\)", author_string)
                    if match:
                        id_type, id_value = match.group(1), match.group(2)
                        identifier = f"{id_type}:{id_value}"
                        name = author_string.split("(")[0].strip()
                    else:
                        identifier = author_string.replace(" ", "_")
                        name = author_string

                    # Create Person instance
                    person = Person(identifier, name)

                    # Create URI for the person, replacing ":" with "_" for a valid URI
                    person_uri = URIRef(f"http://example.org/person/{person.getId().replace(':', '_')}")

                    # Add triples to the graph
                    self.my_graph.add((subj, DCTERMS.creator, person_uri))  # object → author
                    self.my_graph.add((person_uri, FOAF.name, Literal(person.getName())))
                    print(f"Added author: {person.getName()} (ID: {person.getId()})")
        
        except Exception as e:
            print(f"Error processing row: {row}. Exception: {e}")
        
    def uploadGraphToBlazegraph(self) -> bool:
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

# F R A N C E S C A,  M A T I L D E
class ProcessDataUploadHandler(UploadHandler):
    pass

    # M A T I L D E
    # the method of ProcessDataUploadHandler that pushes data to the rel db
    def pushDataToDb(self, file_path):

        # open json file
        with open(file_path, "r", encoding="utf-8") as f:
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

        #print("Acquisition list:\n", acquisition)

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

        """ print("Acquisition dataframe:\n", acquisition_df)
        print("Acquisition dataframe info:", acquisition_df.info()) """

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

       # print("Acquisition df with unique ids:\n", acquisition_df)

        # remove multi-valued attributes from df
        def keep_single_valued(process_df):
            dtypes = process_df.dtypes
            #print(isinstance(dtypes, pd.Series))

            # iterate over the columns in the Series
            for column_name, datatype in dtypes.items():
                # if the column has datatype object...
                if datatype == object:
                    #... create a sub-dataframe containing the column and the unique_id, associate a dtype to the column and drop the multi-valued column from the dataframe
                    multi_valued = process_df[["unique_id", column_name]]
                    multi_valued[column_name].astype("object")
                    process_df.drop("tool", axis=1, inplace=True)

            # return the df and the subdataframe
            return process_df, multi_valued
    
        # function calls
        acquisition_df, acquisition_multi_valued = keep_single_valued(acquisition_df)
        processing_df, processing_multi_valued = keep_single_valued(processing_df)
        modelling_df, modelling_multi_valued = keep_single_valued(modelling_df)
        optimising_df, optimising_multi_valued = keep_single_valued(optimising_df)
        exporting_df, exporting_multi_valued = keep_single_valued(exporting_df)
        """ print("Acquisition df and multi-valued df:\n", acquisition_df, acquisition_multi_valued)
        print(acquisition_df.info())
        print(acquisition_multi_valued.info()) """
        
        # create multi-valued attributes tables
        def create_multi_valued_tables(multi_valued_df):
            tools_dict = dict()
            for idx, row in multi_valued_df.iterrows():
                # populate dictionary with unique identifiers as keys and lists of tools as values
                if isinstance(row["tool"], str):
                    tool_str = row["tool"].replace("[", "").replace("]", "").replace("'", "")
                    tool_str_to_list = tool_str.split(",")
                    #print("tool_string_to_list has type:", type(tool_str_to_list))
                    tools_dict[row["unique_id"]] = tool_str_to_list
                else:
                    tools_dict[row["unique_id"]] = row["tool"]

            #print(tools_dict)

            tools_unpacked = []
            identifiers_unpacked = []

            # iterate over each tool in the inner lists
            for tool_list in tools_dict.values():
                # and append it to the new list
                if len(tool_list) < 1:
                    tools_unpacked.append("")
                else:
                    for t in tool_list:
                        tools_unpacked.append(t)

            #print("list for tools:\n", tools_unpacked)

            # iterate over the list of identifiers
            for identifier in tools_dict.keys():
                # and append each identifier to the series as many times as the length of the list which is the value of the key corresponding to the identifier in the tools_dict
                list_length = len(tools_dict[identifier])
                if list_length < 1:
                    identifiers_unpacked.append(identifier)
                else:
                    for n in range(list_length):
                        identifiers_unpacked.append(identifier)

            #print("list for identifiers:\n", identifiers_unpacked)

            # create series and join them in a dataframe where each series is a column
            tools_series = pd.Series(tools_unpacked, dtype="string", name="tool")
            identifiers_series = pd.Series(identifiers_unpacked, dtype="string", name="unique_id")
            tools_df = pd.concat([identifiers_series, tools_series], axis=1)
            #print("The dataframe for tools:\n", tools_df)
            
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
        #print("The merged dataframe:\n", merged_tools_df)
        
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
            #print(populated_tables)
            return populated_tables


class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl: str = ""):
        super().__init__(dbPathOrUrl)
        
    def getById(self, id: str): # in the data model, getById is a method of QueryHandler, which is superclass of MetadataQueryHandler and ProcessDataQueryHandler, so it should be inherited by both. should we move it here?
        pass 

# QueryHandler has no additional attributes
""" class QueryHandler(Handler):
    pass
        
    def getById(self, id: str):
        pass  """

# A L I C E, C A R L A
class MetadataQueryHandler(QueryHandler):
    def __init__(self, dbPathOrUrl=""):
        super().__init__(dbPathOrUrl)
    
    def getById(self, id: str) -> pd.DataFrame:
        # Query for cultural heritage object
        object_query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT DISTINCT ?id ?type ?title ?dateCreated ?maker ?spatial
        WHERE {{
            ?s dcterms:identifier "{id}" ;
            rdf:type ?type .
            OPTIONAL {{ ?s dcterms:title ?title . }}
            OPTIONAL {{ ?s schema:dateCreated ?dateCreated . }}
            OPTIONAL {{ ?s foaf:maker ?maker . }}
            OPTIONAL {{ ?s dcterms:spatial ?spatial . }}
            BIND("{id}" AS ?id)
        }}
        LIMIT 1
        """
        df = self.execute_query(object_query)
        if not df.empty:
            return df

        # Query for person
        person_query = f"""
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>

        SELECT DISTINCT ?name ?id
        WHERE {{
            ?s foaf:name ?name .
            FILTER(STR(?s) = "http://example.org/person/{id}")
            BIND("{id}" AS ?id)
        }}
        LIMIT 1
        """
        return self.execute_query(person_query)

    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.dbPathOrUrl:
            return pd.DataFrame()

        try:
            sparql = SPARQLWrapper(self.dbPathOrUrl)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)

            results = sparql.query().convert()
            columns = results["head"]["vars"]
            rows = [
                [binding.get(col, {}).get("value", None) for col in columns]
                for binding in results["results"]["bindings"]
            ]
            return pd.DataFrame(rows, columns=columns)

        except:
            return pd.DataFrame()

    def getAllPeople(self) -> pd.DataFrame: # C A R L A
        query = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?personName ?personID
        WHERE {
            ?person foaf:name ?personName .
            FILTER(STRSTARTS(STR(?person), "http://example.org/person/"))
            BIND(STRAFTER(STR(?person), "http://example.org/person/") AS ?personID)
        }
        ORDER BY ?personName
        """
        return self.execute_query(query)

    def getAllCulturalHeritageObjects(self) -> pd.DataFrame: # C A R L A
        if not self.dbPathOrUrl:
            return pd.DataFrame()

        sparql = SPARQLWrapper(self.dbPathOrUrl)

        query = """
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <http://schema.org/>

        SELECT DISTINCT ?id ?type ?title ?dateCreated ?owner ?place
        WHERE {
            ?object rdf:type ?type ;
                    dcterms:identifier ?id ;
                    dcterms:title ?title .
            OPTIONAL { ?object schema:dateCreated ?dateCreated . }
            OPTIONAL { ?object foaf:maker ?owner . }
            OPTIONAL { ?object dcterms:spatial ?place . }
        }
        ORDER BY ?title
        """

        try:
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)

            results = sparql.query().convert()

            columns = results["head"]["vars"]
            rows = [
                [binding.get(col, {}).get("value", None) for col in columns]
                for binding in results["results"]["bindings"]
            ]

            df = pd.DataFrame(rows, columns=columns)
            print("Query returned", len(df), "rows.")
            return df

        except Exception as e:
            print("Error executing SPARQL query:", e)
            return pd.DataFrame()

    def getAuthorsOfCulturalHeritageObject(self, object_id: str) -> pd.DataFrame: # A L I C E
        query = f"""
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?personName ?personID
        WHERE {{
            <http://example.org/{object_id}> dcterms:creator ?person .
            OPTIONAL {{ ?person foaf:name ?personName . }}
            BIND(STRAFTER(STR(?person), "http://example.org/person/") AS ?personID)
        }}
        ORDER BY ?personName
        """
        df = self.execute_query(query)

        # Se mancano colonne o valori None, li puliamo
        if not df.empty:
            if "personName" not in df.columns:
                df["personName"] = ""
            df["personName"] = df["personName"].fillna("")  # sostituisce None con stringa vuota
            df["personID"] = df["personID"].fillna("")      # per sicurezza, anche per ID

        return df

    def getCulturalHeritageObjectsAuthoredBy(self, id_value: str) -> pd.DataFrame: # A L I C E
        query = f"""
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX schema: <http://schema.org/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?objectID ?title ?date ?owner ?place
        WHERE {{
            ?object dcterms:creator <http://example.org/person/{id_value}> .
            ?object dcterms:identifier ?objectID ;
                    dcterms:title ?title .
            OPTIONAL {{ ?object schema:dateCreated ?date . }}
            OPTIONAL {{ ?object foaf:maker ?owner . }}
            OPTIONAL {{ ?object dcterms:spatial ?place . }}
        }}
        ORDER BY ?title
        """
        return self.execute_query(query)
    
# F R A N C E S C A,  M A T I L D E        
acquisition_sql_df = DataFrame()
tool_sql_df= DataFrame()


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
    #print("the complete df:", activities)
    return activities

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self, dbPathOrUrl = ""): #ProcessDataQH has no additional attributes: def __init__...etc can be replaced with pass
        super().__init__(dbPathOrUrl)
    
    def getById(self, id: str) -> pd.DataFrame: # if we move getById to QueryHandler, this is automatically inherited from the superclass
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

        # missing inputs
        if not date:
            print("The input string is empty.")
            return start_date_df
        else:
            start_date_df = activities[(activities["start date"] >= date) & (activities["start date"] != '')]
        
            if start_date_df.empty:
                print("No match found.")
        
        return start_date_df

    # M A T I L D E
    def getActivitiesEndedBefore(self, date):
        activities = query_rel_db()
        end_date_df = DataFrame()

        # missing inputs
        if not date:
            print("The input string is empty.")
            return end_date_df
        else:
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
        filtered_df = merged_df[merged_df["technique"].str.strip().str.contains(inputtechnique, case=False, na=False)]

        if filtered_df.empty:
            print("No match found")  

        return filtered_df

    # F R A N C E S C A
    def getActivitiesUsingTool(self, tool):
        activities = query_rel_db()
        # Normalize the tool string for comparison
        tool_lower = tool.lower()
    
        # Filter rows where the tool column matches the exact or partial tool name
        activities_tool = activities[activities['tool'].str.strip().str.lower().str.contains(tool_lower, case=False, na=False)]

        if activities_tool.empty:
            print("No match found")

        return activities_tool

# C A R L A    
class BasicMashup(object):
    def __init__(self):
        self.metadataQuery = []  
        self.processQuery = []
        self.type_mapping = {
    "https://dbpedia.org/resource/Nautical_chart": NauticalChart,
    "http://example.org/ManuscriptPlate": ManuscriptPlate,
    "https://dbpedia.org/resource/Category:Manuscripts_by_collection": ManuscriptVolume,
    "https://schema.org/PublicationVolume": PrintedVolume,
    "http://example.org/PrintedMaterial": PrintedMaterial,
    "https://dbpedia.org/resource/Herbarium": Herbarium,
    "https://dbpedia.org/resource/Specimen": Specimen,
    "https://dbpedia.org/resource/Category:Painting": Painting,
    "https://dbpedia.org/resource/Category:Prototypes": Model,
    "https://dbpedia.org/resource/Category:Maps": Map,
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

    def createEntityObject(self, entity_data: dict) -> CulturalHeritageObject:
        entity_type = entity_data.get("type", "").strip()
        entity_id = str(entity_data.get("id", "")).strip()
        title = str(entity_data.get("title", "")).strip()
        date = str(entity_data.get("dateCreated", "")).strip()
        owner = str(entity_data.get("owner", "")).strip()
        place = str(entity_data.get("place", "")).strip()

        if entity_type in self.type_mapping:
            cls = self.type_mapping[entity_type]
            return cls(entity_id, title, date, owner, place)
        else:
            return CulturalHeritageObject(entity_id, title, date, owner, place)

    def createObjectList(self, df: pd.DataFrame) -> List[IdentifiableEntity]: # C A R L A
        object_list = []  
        for _, row in df.iterrows():  
            entity_data = row.to_dict()  
            obj = self.createEntityObject(entity_data)  
            object_list.append(obj) 
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
                        authors_df["auth"] = authors_df["personName"].astype(str) + "-" + authors_df["personId"].astype(str)
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
            if not self.metadataQuery: 
                return None
            for handler in self.metadataQuery:  
                try:
                    df = handler.getById(entity_id)  
                    if not df.empty:  # Check if the DataFrame is not empty
                        if 'type' in df.columns:  
                            cho_list = self.createObjectList(df)
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
            # Collect DataFrames from all handlers
            person_df_list = [handler.getAllPeople() for handler in self.metadataQuery]
            person_df_list = [df for df in person_df_list if not df.empty]  # Filter out empty DataFrames

            if person_df_list:  # Proceed only if there are valid DataFrames
                merged_df = pd.concat(person_df_list, ignore_index=True).drop_duplicates(
                    subset=["personID"], keep="first"
                )

                # Process each row to create Person or Author objects
                for _, row in merged_df.iterrows():
                    person_id = row.get("personID")
                    person_name = row.get("personName")

                    if person_id and person_name:  # Ensure values are valid
                        person_name = str(person_name).strip()
                        person_id = str(person_id).strip()

                        # Determine if the person is an Author with VIAF or ULAN ID
                        if person_id and person_name:
                            person_name = str(person_name).strip()
                            person_id = str(person_id).strip()
                            person_list.append(Person(person_id, person_name))

        print(f"Total unique people identified: {len(person_list)}")
        return person_list
    
    
    def getAllCulturalHeritageObjects(self): # C A R L A
        """
        Returns a list of objects of the class CulturalHeritageObject (or its subclasses),
        constructed from the data retrieved by all metadata handlers.
        """
        if not self.metadataQuery:
            return []

        df_list = []

        # Gather data from all metadata handlers
        for handler in self.metadataQuery:
            try:
                df_objects = handler.getAllCulturalHeritageObjects()
                if df_objects is not None and not df_objects.empty:
                    df_list.append(df_objects)
            except Exception as e:
                print(f"Error retrieving objects from handler {handler}: {e}")

        if not df_list:
            return []

        # Merge data, clean it
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        # Convert each row into a proper object using createEntityObject
        cultural_heritage_objects = []
        for _, row in df_union.iterrows():
            entity_data = row.to_dict()
            obj = self.createEntityObject(entity_data)
            cultural_heritage_objects.append(obj)

        return cultural_heritage_objects
    
    def getAuthorsOfCulturalHeritageObject(self, id: str): # A L I C E
        if not self.metadataQuery:
            return []

        df_list = []
        for handler in self.metadataQuery:
            df = handler.getAuthorsOfCulturalHeritageObject(id)
            if df is not None and not df.empty:
                df_list.append(df)

        if not df_list:
            return []

        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates(subset=["personID"]).fillna("")

        author_result_list = []
        for _, row in df_union.iterrows():
            person_id = str(row["personID"]).strip()
            person_name = str(row["personName"]).strip()

            if re.match(r'(VIAF|ULAN)_\d+', person_id):
                identifier = person_id.replace("_", ":")
                author_result_list.append(Person(person_name, identifier=identifier))
            else:
                author_result_list.append(Person(person_name))

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
            obj = self.createEntityObject(entity_data)  # Convert row to an appropriate object
            object_list.append(obj)

        return object_list

    # methods for relational db start here
    # F R A N C E S C A,  M A T I L D E
    def getAllActivities(self):
        activities_df = pd.DataFrame()
        activities_df_list = []
        if self.processQuery:
            for process_qh in self.processQuery:
                activities_df = process_qh.getAllActivities()
                activities_df_list.append(activities_df)

            concat_df = pd.concat(activities_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates() # drop only identical rows
            #print("Concatened df cleaned:", concat_df_cleaned)
        
        else:
            print("No processQueryHandler found")
        
        updated_df = join_tools(concat_df_cleaned)
        #print("Updated df:", updated_df)
        #print(instantiate_class(updated_df))
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
        #print(instantiate_class(updated_df))
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
        #print(instantiate_class(updated_df))
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
        #print(instantiate_class(updated_df))
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
        #print(instantiate_class(updated_df))
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
        #print(instantiate_class(updated_df))
        return instantiate_class(updated_df)
    

    def getActivitiesUsingTool(self, tool):
        if self.processQuery:
            act_activities_tool_list = [process_qh.getActivitiesUsingTool(tool) for process_qh in self.processQuery]
            
            concat_df = concat(act_activities_tool_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates()

        else:
            print("No processQueryHandler found")

        updated_df = join_tools(concat_df_cleaned)
        #print(instantiate_class(updated_df))
        return instantiate_class(updated_df)


""" ch_object = "" """
def get_CHO(id):
    """ global ch_object """
    cho_mapping = {
        "https://dbpedia.org/resource/Nautical_chart": NauticalChart,
        "http://example.org/ManuscriptPlate": ManuscriptPlate,
        "https://dbpedia.org/resource/Category:Manuscripts_by_collection": ManuscriptVolume,
        "https://schema.org/PublicationVolume": PrintedVolume,
        "http://example.org/PrintedMaterial": PrintedMaterial,
        "https://dbpedia.org/resource/Herbarium": Herbarium,
        "https://dbpedia.org/resource/Specimen": Specimen,
        "https://dbpedia.org/resource/Category:Painting": Painting,
        "https://dbpedia.org/resource/Category:Prototypes": Model,
        "https://dbpedia.org/resource/Category:Maps": Map,
    }

    metadata_qh = MetadataQueryHandler()
    metadata_qh.setDbPathOrUrl(metadata_qh.getDbPathOrUrl())
    cho_df = metadata_qh.getAllCulturalHeritageObjects()

    for idx, row in cho_df.iterrows():
        if row["id"] == id:
            class_to_use = cho_mapping.get(row["type"])
            return class_to_use(row["id"], row["title"], row["date"], row["owner"], row["place"])

    return None # fallback if no match is found
    
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
    #print("Length of input activity df:", len(activity_df))
    for idx, row in activity_df.iterrows():
        split_refers_to = row["refers_to"].split("_")
        obj_id = split_refers_to[-1]
        #print(obj_id)
        activity_from_id = re.sub("_\\d+", "", row["unique_id"])
        #print("The current activity:", activity_from_id)
        if activity_from_id in activity_mapping.keys() and activity_from_id == "acquisition":
            activity_obj = Acquisition(row["responsible institute"], row["responsible person"], row["tool"], row["start date"], row["end date"], get_CHO(obj_id), row["technique"])
            activity_list.append(activity_obj)
        elif activity_from_id in activity_mapping.keys() and activity_from_id != "acquisition":
            class_to_use = activity_mapping.get(activity_from_id)
            activity_obj = class_to_use(row["responsible institute"], row["responsible person"], row["tool"], row["start date"], row["end date"], get_CHO(obj_id))
            activity_list.append(activity_obj)

    return activity_list

# M A T I L D E
def join_tools(activity_df): 
    # ensure the tool column in the df has dtype object
    activity_df["tool"] = activity_df["tool"].astype("object")
    # create a sub dataframe
    tools_subdf = activity_df[["unique_id", "tool"]]
    # iterate over sub dataframe grouping by unique id
    for unique_id, group in tools_subdf.groupby(["unique_id"]):
        #print(f"Current unique id is {unique_id} and current group is {group}")
        # convert tools to list and then join them
        concatenated_tools = ", ".join(group["tool"])
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
    #print("updated dataframe:", activity_df_updated)
    return activity_df_updated

class AdvancedMashup(BasicMashup):
    def __init__(self):
        super().__init__()

    def getActivitiesOnObjectsAuthoredBy(self, person_id: str) -> List[Activity]: # C A R L A
        if not self.metadataQuery or not self.processQuery:
            return []

        authored_objects = self.getCulturalHeritageObjectsAuthoredBy(person_id)
        if not authored_objects:
            return []

        authored_ids = set(obj.getId() for obj in authored_objects)
        all_activities = self.getAllActivities()

        return [act for act in all_activities if act.refers_to in authored_ids]
        
    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        """
        Restituisce tutti gli oggetti culturali coinvolti in attività
        gestite da una persona il cui nome corrisponde (anche parzialmente)
        alla stringa in input.
        """
        results = []

        # Trova le attività gestite da quella persona
        activities = self.getActivitiesByResponsiblePerson(partialName)
        if activities.empty:
            return results

        # Ottieni tutti gli oggetti culturali
        all_objects = self.getAllCulturalHeritageObjects()
        if not all_objects:
            return results

        # Itera sulle attività e raccogli gli oggetti validi
        def is_valid_referred_object(activity):
            refersTo_cho = getattr(activity, "refersTo_cho", None)
            return refersTo_cho is not None and hasattr(refersTo_cho, "id")

        object_ids = set()
        for activity in activities:
            if is_valid_referred_object(activity):
                refersTo_cho = getattr(activity, "refersTo_cho")
                object_ids.add(refersTo_cho.id)

        # Seleziona gli oggetti culturali che corrispondono agli ID raccolti
        for cho in all_objects:
            if cho.id in object_ids:
                results.append(cho)

        return results


    def getObjectsHandledByResponsibleInstitution(self, institution: str) -> list[CulturalHeritageObject]:
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
        object_ids = set()
        for activity in activities:
            # Accedi a refersTo_cho in modo robusto
            refersTo_cho = getattr(activity, "refersTo_cho", None)
            if refersTo_cho and hasattr(refersTo_cho, "id"):
                object_ids.add(refersTo_cho.id)

        for cho in all_objects:
            if cho.id in object_ids:
                objects_list.append(cho)

        return objects_list

    # M A T I L D E
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start, end): # returns a list of objects of the class person
        query_result = []

        # retrieve all cultural heritage objects
        allCHO_df_list = []
        for handler in self.metadataQuery:
            allCHO_df = handler.getAllCulturalHeritageObjects()
            allCHO_df_list.append(allCHO_df)
        
        conc_CHO_df = pd.concat(allCHO_df_list, ignore_index=True).drop_duplicates()
        allCHO_ID_df = conc_CHO_df[["id"]]
        # store ids in a set
        ID_set = set(allCHO_ID_df["id"])

        # call getAuthorsOfCHO method to retrieve info about authors
        authors_CHO_df_list = []
        for h in self.metadataQuery:
            for obj_id in ID_set:
                authors_CHO_df = h.getAuthorsOfCulturalHeritageObject(obj_id)
                if authors_CHO_df is not None and not authors_CHO_df.empty:
                    authors_CHO_df_list.append(authors_CHO_df)
        
        conc_authors_CHO_df = pd.concat(authors_CHO_df_list, ignore_index=True).drop_duplicates().dropna(axis=0)
        # update objectID column for join with refers_to
        conc_authors_CHO_df["objectID"] = "object_" + conc_authors_CHO_df["objectID"].astype(str)
        # subdataframe
        authors_id_df = conc_authors_CHO_df[["objectID", "authorName"]]
        # drop duplicated author names: keep only those with id and store ids in a different column
        authors_id_df["authorID"] = authors_id_df["authorName"].str.extract(r'\((.*?)\)') #extract everything between parentheses (VIAF or ULAN id) and create a new column which stores the id

        # acquisition df
        acq_df_list = []
        for handler in self.processQuery:
            df_sa = handler.getActivitiesStartedAfter(start)
            df_eb = handler.getActivitiesEndedBefore(end)
            acq_df_list.extend([df_sa, df_eb])
        
        acq_timeframe_df = pd.concat(acq_df_list, ignore_index=True).drop_duplicates()
        acq_timeframe_df = acq_timeframe_df[["unique_id", "start date", "end date", "refers_to"]]
        print("Advanced: df for all activities:\n", acq_timeframe_df.head())
        
        # merge resulting dataframes
        result_df = pd.merge(authors_id_df, acq_timeframe_df, left_on="objectID", right_on="refers_to", how="inner")

        # extend the empty list with the objects of the class person compliant with the query
        for _, row in result_df.iterrows():
            author = Person(name=row["authorName"], id=row["authorID"])
            query_result.append(author)
        
        return query_result
