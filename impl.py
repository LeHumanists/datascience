import pandas as pd
import json  # Importazione del modulo standard json
import pprint
from pandas import DataFrame
from pandas import Series
from pandas import concat
from pandas import read_sql
import re
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


class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, url):
        self.dbPathOrUrl = url

class UploadHandler(Handler):
    pass

    def pushDataToDb(self, file_path) -> bool:
        # return True if data is uploaded to the db
        pass

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
            print(f"Error reading CSV file: {e}")  # Print error message if reading fails
            return False
        
        for idx, row in df.iterrows():  # Iterate over each row in the DataFrame
            # Use the existing 'Id' from the CSV file as the unique identifier
            cultural_object_id = str(row["Id"])
            subj = URIRef(self.example + cultural_object_id)  # Use the 'Id' as part of the URI
            
            # Assigning object ID
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
        
            df = pd.DataFrame(process_list)
            # iterate over columns in the df for associating datatype
            for column_name, column in df.items():
                if column_name == "tool":
                    df = df.astype("string")
                else:
                    df = df.astype(dtype={"tool": "object"})
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
            # iterate over dataframe rowa
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
                    #... pop the column from the df
                    multi_valued = process_df.pop(column_name)

            # return the df and the popped column
            return process_df, multi_valued
    
        # function calls
        acquisition_df, acquisition_multi_valued = keep_single_valued(acquisition_df)
        processing_df, processing_multi_valued = keep_single_valued(processing_df)
        modelling_df, modelling_multi_valued = keep_single_valued(modelling_df)
        optimising_df, optimising_multi_valued = keep_single_valued(optimising_df)
        exporting_df, exporting_multi_valued = keep_single_valued(exporting_df)
        print("Acquisition df and multi-valued column:\n", acquisition_df, acquisition_multi_valued)
        print(acquisition_df.info())
        # pushing tables to db
        with connect("relational.db") as con:
            acquisition_df.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_df.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_df.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_df.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_df.to_sql("Exporting", con, if_exists="replace", index=False)
        
            # check and return result
            rel_db_ac = pd.read_sql("SELECT * FROM Acquisition", con)
            rel_db_pr = pd.read_sql("SELECT * FROM Processing", con)
            rel_db_md = pd.read_sql("SELECT * FROM Modelling", con)
            rel_db_op = pd.read_sql("SELECT * FROM Optimising", con)
            rel_db_ex = pd.read_sql("SELECT * FROM Exporting", con)

            populated_tables = not any(df.empty for df in [rel_db_ac, rel_db_pr, rel_db_op, rel_db_md, rel_db_ex])
            print(populated_tables)
            return populated_tables


class QueryHandler(Handler): 
    def __init__(self, dbPathOrUrl: str = ""):  
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str) -> DataFrame: # Retrieve data by its ID. This method does nothing here and should be implemented in subclasses to provide specific query logic.
        pass
    
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

activities = DataFrame()

class ProcessDataQueryHandler(QueryHandler):
    pass

    def getAllActivities(self):
        
        with connect("relational.db") as con:
            query_acquisition = "SELECT * FROM Acquisition"
            acquisition_sql_df = read_sql(query_acquisition, con)

            query_processing = "SELECT * FROM Processing"
            processing_sql_df = read_sql(query_processing, con)

            query_modelling = "SELECT * FROM Modelling"
            modelling_sql_df = read_sql(query_modelling, con)

            query_optimising = "SELECT * FROM Optimising"
            optimising_sql_df = read_sql(query_optimising, con)

            query_exporting = "SELECT * FROM Exporting"
            exporting_sql_df = read_sql(query_exporting, con)

        activities = concat([acquisition_sql_df, processing_sql_df, modelling_sql_df, optimising_sql_df, exporting_sql_df], ignore_index=True)
        return activities


    def getActivitiesByResponsibleInstitution(self, partialName):
        institution_df = DataFrame()
        for idx, row in activities.iterrows(): # check if there is something to iterate over the rows without getting also the index
            for column_name, item in row.items():
                if column_name == "responsible institute":
                    # exact match
                    if partialName.lower() == item.lower():
                    # use backticks to refer to column names containing spaces and @ for variables
                        institution_df = activities.query("`responsible institute` == @item")
                    # partial match
                    elif partialName.lower() in item.lower():
                        institution_df = activities.query("`responsible institute` == @item")
                
        return institution_df
    
    def getActivitiesByResponsiblePerson(self, partialName):
        person_df = DataFrame()
        for idx, row in activities.iterrows():
            for column_name, person in row.items():
                if column_name == "responsible person":
                    # exact match
                    if partialName.lower() == person.lower():
                        # use backticks to refer to column names containing spaces and @ for variables
                        person_df = activities.query("`responsible institute` == @person")
                    # partial match
                    elif partialName.lower() in person.lower():
                        person_df = activities.query("`responsible institute` == @person")

        return person_df
    
    def getActivitiesStartedAfter(self, date):
        start_date_df = DataFrame()
        for idx, row in activities.iterrows():
            for column_name, item in row.items():
                if column_name == "start date":
                    start_date_df = activities.query("`start date` >= @date")
        
        return start_date_df
    
    def getActivitiesEndedBefore(self, date):
        end_date_df = DataFrame()
        for idx, row in activities.iterrows():
            for column_name, item in row.items():
                if column_name == "end date": end_date_df = activities.query("`end date` <= @date and `end date` != ''")
        
        return end_date_df
    
class BasicMashup:
    def __init__(self, metadataQuery=None, processQuery=None):
        self.metadataQuery = metadataQuery if metadataQuery is not None else []  # Initialize metadataQuery as a list of MetadataQueryHandler
        self.processQuery = processQuery if processQuery is not None else []     # Initialize processQuery as a list of ProcessorDataQueryHandler

    def cleanMetadataHandlers(self) -> bool:
        self.metadataQuery.clear()  # Clear the list of metadataQuery handlers
        return True

    def cleanProcessHandlers(self) -> bool:
        self.processQuery.clear()  # Clear the list of processQuery handlers
        return True

    def addMetadataHandler(self, handler) -> bool:
        if handler not in self.metadataQuery:  # Add a MetadataQueryHandler object to the metadataQuery list if not already present
            self.metadataQuery.append(handler)
            return True
        return False

    def addProcessHandler(self, handler) -> bool:
        if handler not in self.processQuery:  # Add a ProcessorDataQueryHandler object to the processQuery list if not already present
            self.processQuery.append(handler)
            return True
        return False

    def _createEntityObject(self, entity_data: dict) -> IdentifiableEntity:
        type_class_map = {  # Map entity types to their corresponding classes
            "Nautical_chart": NauticalChart,
            "Manuscript_plate": ManuscriptPlate,
            "Manuscript_volume": ManuscriptVolume,
            "Printed_volume": PrintedVolume,
            "Printed_material": PrintedMaterial,
            "Herbarium": Herbarium,
            "Specimen": Specimen,
            "Painting": Painting,
            "Model": Model,
            "Map": Map,
            "Person": Person,
            "Author": Author,
        }
        entity_type = entity_data.get("type", None)  # Get the type of the entity from the data
        entity_id = entity_data.get("id")  # Get the ID of the entity from the data
        if entity_type in type_class_map:  # Check if the type is in the mapping and create an instance dynamically
            cls = type_class_map[entity_type]
            return cls(**entity_data)
        else:  # Return a default IdentifiableEntity instance if no matching type is found
            return IdentifiableEntity(entity_id)

    def _createObjectList(self, df: pd.DataFrame) -> List[IdentifiableEntity]:
        object_list = []  # Initialize an empty list for storing objects
        for _, row in df.iterrows():  # Iterate over rows in the DataFrame
            entity_data = row.to_dict()  # Convert each row to a dictionary
            obj = self._createEntityObject(entity_data)  # Create an object using _createEntityObject
            object_list.append(obj)  # Append the created object to the list
        return object_list

    def getEntityById(self, entity_id: str) -> Optional[IdentifiableEntity]:
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

    def getAllPeople(self):
        person_list = []  # Initialize an empty list for storing people objects
        if self.metadataQuery:  # Check if there are any metadata handlers available
            new_person_df_list = [handler.getAllPeople() for handler in self.metadataQuery]  # Retrieve DataFrames from all handlers
            new_person_df_list = [df for df in new_person_df_list if not df.empty]  # Filter out empty DataFrames
            if new_person_df_list:  # Proceed only if there are valid DataFrames to merge
                merged_df = pd.concat(new_person_df_list, ignore_index=True).drop_duplicates(subset=["personID"], keep="first")  # Merge and deduplicate DataFrames
                person_list = [Person(row["personID"], row["personName"]) for _, row in merged_df.iterrows() if row["personID"].strip() and row["personName"].strip()]  # Create Person objects from rows
        return person_list

    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]:
        cho_list = []  # Initialize an empty list for storing cultural heritage objects
        if self.metadataQuery:  # Check if there are any metadata handlers available
            new_object_df_list = []  # Initialize a list to store DataFrames from handlers
            for handler in self.metadataQuery:  # Iterate over metadata handlers to retrieve objects
                try:
                    new_object_df = handler.getAllCulturalHeritageObjects()
                    if not new_object_df.empty:  # Add non-empty DataFrames to the list
                        new_object_df_list.append(new_object_df)
                except Exception as e:  # Print an error message if retrieval fails
                    print(f"Error retrieving cultural heritage objects from handler {handler}: {e}")
            if new_object_df_list:  # Proceed only if there are valid DataFrames to merge and process
                try:
                    merged_df = pd.concat(new_object_df_list, ignore_index=True).drop_duplicates(subset=["id"], keep="first")  # Merge and deduplicate DataFrames
                    cho_list = self._createObjectList(merged_df)  # Create objects using _createObjectList
                except Exception as e:  # Print an error message if merging or processing fails
                    print(f"Error merging or processing the DataFrames: {e}")
        return cho_list

    def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> List[Person]:
        author_list = []  # Initialize an empty list for storing authors (Person objects)
        if self.metadataQuery:  # Check if there are any metadata handlers available
            new_person_df_list = []  # Initialize a list to store DataFrames from handlers
            for handler in self.metadataQuery:  # Iterate over metadata handlers to retrieve authors of a specific object
                try:
                    new_person_df = handler.getAuthorsOfCulturalHeritageObject(objectId)
                    if not new_person_df.empty:  # Add non-empty DataFrames to the list
                        new_person_df_list.append(new_person_df)
                except Exception as e:  # Print an error message if retrieval fails
                    print(f"Error retrieving authors for object {objectId} from handler {handler}: {e}")
            if new_person_df_list:  # Proceed only if there are valid DataFrames to merge and process
                try:
                    merged_df = pd.concat(new_person_df_list, ignore_index=True).drop_duplicates(subset=["author_id"], keep="first")  # Merge and deduplicate DataFrames
                    author_list = [Person(row["author_id"], row["author_name"]) for _, row in merged_df.iterrows() if row["author_id"].strip() and row["author_name"].strip()]  # Create Person objects from rows
                except Exception as e:  # Print an error message if merging or processing fails
                    print(f"Error during merging or processing of DataFrames: {e}")
        return author_list

    def getCulturalHeritageObjectsAuthoredBy(self, AuthorId: str) -> List[CulturalHeritageObject]:
        cho_list = []  # Initialize an empty list for storing cultural heritage objects authored by a specific author
        if self.metadataQuery:  # Check if there are any metadata handlers available
            new_object_df_list = []  # Initialize a list to store DataFrames from handlers 
            for handler in self.metadataQuery:  # Iterate over metadata handlers to retrieve authored objects 
                try:
                    new_object_df = handler.getCulturalHeritageObjectsAuthoredBy(AuthorId)
                    if not new_object_df.empty: 
                        new_object_df_list.append(new_object_df)
                except Exception as e:
                    print(f"Error retrieving cultural heritage objects for AuthorId {AuthorId} from handler {handler}: {e}")
            if new_object_df_list:
                try:
                    merged_df = pd.concat(new_object_df_list, ignore_index=True).drop_duplicates(subset=["id"], keep="first")
                    cho_list = self._createObjectList(merged_df)
                except Exception as e:
                    print(f"Error during merging or processing of DataFrames: {e}")
        return cho_list 

    # methods for relational db start here
    
    def getAllActivities(self):
        if self.processQuery:
            activities_df_list = [process_qh.getAllActivities() for process_qh in self.processQuery]

            concat_df = concat(activities_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates(subset=["unique_id"])
        
        else:
            print("No processQueryHandler found")
        
        return instantiateClass(concat_df_cleaned)
        

    def getActivitiesByResponsibleInstitution(self, partialName):
        if self.processQuery:
            act_by_inst_df_list = [process_qh.getActivitiesByResponsibleInstitution(partialName) for process_qh in self.processQuery]

            concat_df = concat(act_by_inst_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates(subset=["unique_id"])
        
        else:
            print("No processQueryHandler found")
        
        return instantiateClass(concat_df_cleaned)
    

    def getActivitiesByResponsiblePerson(self, partialName):
        if self.processQuery:
            act_by_pers_df_list = [process_qh.getActivitiesByResponsiblePerson(partialName) for process_qh in self.processQuery]

            concat_df = concat(act_by_pers_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates(subset=["unique_id"])

        else:
            print("No processQueryHandler found")

        return instantiateClass(concat_df_cleaned)
    

    def getActivitiesStartedAfter(self, date):
        if self.processQuery:
            act_start_aft_list = [process_qh.getActivitiesStartedAfter(date) for process_qh in self.processQuery]

            concat_df = concat(act_start_aft_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates(subset=["unique_id"])

        else:
            print("No processQueryHandler found")

        return instantiateClass(concat_df_cleaned)
    

    def getActivitiesEndedBefore(self, date):
        if self.processQuery:
            act_end_before_list = [process_qh.getActivitiesStartedAfter(date) for process_qh in self.processQuery]

            concat_df = concat(act_end_before_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates(subset=["unique_id"])

        else:
            print("No processQueryHandler found")

        return instantiateClass(concat_df_cleaned)
    

    def getAcquisitionsByTechnique(self, inputtechnique):
        if self.processQuery:
            act_by_technique_df_list = [process_qh.getAcquisitionsByTechnique(inputtechnique) for process_qh in self.processQuery]

            concat_df = concat(act_by_technique_df_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates(subset=["unique_id"])

        else:
            print("No processQueryHandler found")

        return instantiateClass(concat_df_cleaned)
    

    def getActivitiesUsingTool(self, tool):
        if self.processQuery:
            act_activities_tool_list = [process_qh.getActivitiesUsingTool(tool) for process_qh in self.processQuery]
            
            concat_df = concat(act_activities_tool_list, ignore_index=True)
            concat_df_cleaned = concat_df.drop_duplicates(subset=["unique_id"])

        else:
            print("No processQueryHandler found")

        return instantiateClass(concat_df_cleaned)
    

def instantiateClass(activity_df):
    activity_list = []
    activity_mapping = {
        "acquisition": Acquisition,
        "processing": Processing,
        "modelling": Modelling,
        "optimising": Optimising,
        "exporting": Exporting
    }
        
    with connect("relational.db") as con:
        query = "SELECT * FROM Tools"
        tools_df_sql = read_sql(query, con)

    if len(tools_df_sql) > len(activity_df):
        for tools_idx, tools_row in tools_df_sql.iterrows():
            for act_idx, act_row in activity_df.iterrows():
                if tools_row["unique_id"] != act_row["unique_id"]:
                    tools_df_sql.drop(tools_idx)
    
    merged_df = pd.merge(activity_df, tools_df_sql, left_on="unique_id", right_on="unique_id")

    for idx, row in merged_df.iterrows():
        activity_from_id = re.sub("_\\d+", "", row["unique_id"])
        if activity_from_id in activity_mapping.keys() and activity_from_id == "acquisition":
            activity_obj = Acquisition(row["responsible institute"], row["responsible person"], row["technique"], row["tool"], row["start date"], row["end date"], row["refers_to"])
            activity_list.append(activity_obj)
        elif activity_from_id in activity_mapping.keys() and activity_from_id != "acquisition":
            class_to_use = activity_mapping.get(activity_from_id)
            activity_obj = class_to_use(row["responsible institute"], row["responsible person"], row["tool"], row["start date"], row["end date"], row["refers_to"])
            activity_list.append(activity_obj)
    
    return activity_list

class AdvancedMashup(BasicMashup):
    
    def getActivitiesOnObjectsAuthoredBy(self, person_id: str):
        activities = []  # Initialize a list to store activities
        
        # Check if person_id or the queries are empty
        if not person_id or not self.processQuery or not self.metadataQuery:
            return activities  # Return an empty list if no person_id or queries are present
        
        # Get all cultural heritage objects authored by the person
        authored_objects = []
        for metadata_handler in self.metadataQuery:
            authored_objects.extend(metadata_handler.getCulturalHeritageObjectsAuthoredBy(person_id))
        
        # If there are no authored objects, return an empty list
        if not authored_objects:
            return activities
        
        # Get all activities
        all_activities = []
        for process_handler in self.processQuery:
            all_activities.extend(process_handler.getAllActivities())
        
        # Create a list of related object IDs from the authored objects
        authored_object_ids = [ch_object.getId() for ch_object in authored_objects]
        
        # Iterate over all activities and check if they relate to any authored object
        for activity in all_activities:
            related_object_ids = activity.getRelatedObjectIds()
            
            # If the activity is related to any authored object, add it to the list
            if any(object_id in related_object_ids for object_id in authored_object_ids):
                activities.append(activity)
        
        return activities
    
    def getObjectsHandledByResponsiblePerson(self, partialName: str) -> list[CulturalHeritageObject]:
        objects_list = []

        # Check if partialName or the queries are empty
        if not partialName or not self.processQuery or not self.metadataQuery:
            return objects_list  # Return an empty list
        else:
            # Get the activities of the responsible person with a partial name match
            activities = self.getActivitiesByResponsiblePerson(partialName)
        
        # Get all cultural heritage objects
        object_list = self.getAllCulturalHeritageObjects()
        
        # Create an empty list for object IDs
        object_ids_list = []
        
        # Iterate over the activities to extract object IDs
        for activity in activities:
            # Extract the ID from the referenced object
            object_id = activity.refersTo_cho.id
            # Add the ID to the object_ids list if it's not already there
            if object_id not in object_ids_list:
                object_ids_list.append(object_id)
        
        # Add the objects to the list, avoiding duplicates
        for cho in object_list:
            # If the object's ID is in object_ids, add it to the result list
            if cho.id in object_ids_list and cho not in objects_list:
                objects_list.append(cho)
    
        return objects_list