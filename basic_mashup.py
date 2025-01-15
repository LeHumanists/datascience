from metadata_query_handler import MetadataQueryHandler
from process_data_query_handler import ProcessDataQueryHandler
from identifiable_entity import IdentifiableEntity
from graph_class import CulturalHeritageObject, Author, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map
from person import Person
from activity import Activity, Acquisition, Processing, Modelling, Optimising, Exporting
import pandas as pd
from pandas import DataFrame
from pandas import Series
from pandas import concat
from pandas import merge
from sqlite3 import connect
from pandas import read_sql
import json  # Importazione del modulo standard json
from typing import List, Optional
import re
from metadata_upload_handler import pushDataToDb, type_mapping

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
    
    def getAllCulturalHeritageObjects(self):
   # Retrieve list of handlers from self.metadataQuery in which there is information about cultural heritage objects.      
       handler_list = self.metadataQuery
   # Empty list to collect the DataFrames returned by handlers
       df_list = []
   # Empty list that contains the cultural heritage objects created by the function
       obj_result_list = []


   # Iterate over each handler
       for handler in handler_list:
       # Get the DataFrame of objects from the handler
           df_objects = handler.getAllCulturalHeritageObjects()


       # Combine authors with objects
           df_object_update = self.combineAuthorsOfObjects(df_objects, handler)
      
       # Add the DataFrame to the list
           df_list.append(df_object_update)


   # Concatenate all DataFrames, remove duplicates, and handle null values
       df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")


   # Iterate over each row of the concatenated DataFrame
       for _, row in df_union.iterrows():
           obj_type = row['type']
  
       # Check if the object type is in the type_mapping dictionary
           if obj_type in type_mapping:
           # Get the object class constructor from the mapping
               object_class = type_mapping[obj_type]
      
           # Create the object using the dynamic constructor and row data
               object = object_class(
                   id=str(row["id"]),
                   title=row['title'],
                   date=str(row['date']),
                   owner=row['owner'],
                   place=row['place'],
                   authors=row['Authors']
               )
      
           # Add the object to the result list
               obj_result_list.append(object)


           return obj_result_list

  
    def getAuthorsOfCulturalHeritageObject(self, id: str):
    # Check if there are any available handlers
        if not self.metadataQuery:
            return []  # Return an empty list if there are no handlers available
    
    # List to store valid DataFrames retrieved from each handler
        df_list = []

    # Iterate through the handlers to collect author data
        for handler in self.metadataQuery:
            try:
            # Retrieve the authors' data for the given object ID from the handler
                df = handler.getAuthorsOfCulturalHeritageObject(id)
            # If the DataFrame is not empty, add it to the list
                if not df.empty:
                    df_list.append(df)
            except Exception as e:
            # Print an error message if an exception occurs when retrieving data from the handler
                print(f"Error retrieving authors from handler {handler}: {e}")

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
    
    
    def getCulturalHeritageObjectsAuthoredBy(self, authorId: str):       
        object_result_list = []  # List to collect the final cultural heritage objects
    
    # Check if there are any handlers to query
        if not self.metadataQuery:  # If self.metadataQuery is empty or None
            return object_result_list  # Return an empty list

        df_list = []  # List to collect DataFrames to be merged

    # Collect dataframes from all handlers
        for handler in self.metadataQuery:  # Iterate over each handler in self.metadataQuery
            try:
            # Retrieve cultural heritage objects authored by the given author
                df_objects = handler.getCulturalHeritageObjectsAuthoredBy(authorId)  
            
                if not df_objects.empty:  # If the DataFrame is not empty
                # Combine author data with cultural heritage objects
                    df_object_update = self.combineAuthorsOfObjects(df_objects, handler)  
                # Add the processed DataFrame to the df_list
                    df_list.append(df_object_update)  
            except Exception as e:  # Handle exceptions in case of errors during data retrieval
            # Print an error if something goes wrong during processing
                print(f"Error processing handler {handler}: {e}")  

    # Concatenate all dataframes into one and clean it
        if not df_list:  # If df_list is empty
            return df_list  # Return an empty list

        if df_list:  # If df_list is not empty
        # Merge all DataFrames into a single one, remove duplicates, and handle NaN values
            df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        # Iterate through the concatenated dataframe
            for _, row in df_union.iterrows():  # Iterate through each row of the merged DataFrame
                obj_type = row['type']  # Extract the object type from the 'type' column

            # Check if obj_type is in type_mapping
                if obj_type in type_mapping:  # If the object type is present in type_mapping'
                # Get the corresponding class constructor for the object type
                    object_class = type_mapping[obj_type]  # Get the class associated with the object type

                # Create the object using the corresponding class constructor
                    object = object_class(  # Create the cultural heritage object using the corresponding class
                        id=str(row["id"]),  # Pass the object ID
                        title=row['title'],  # Pass the object title
                        date=str(row['date']),  # Pass the object date
                        owner=row['owner'],  # Pass the object owner
                        place=row['place'],  # Pass the object place
                        authors=row['Authors']  # Pass the authors associated with the object
                    )
                # Add the created object to the result list
                    object_result_list.append(object)
                else:
                # If the object type is not present in type_mapping
                    print(f"Warning: Unrecognized object type: {obj_type}")  # Print a warning

        return object_result_list  # Return the list of created objects

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
    
    merged_df = merge(activity_df, tools_df_sql, left_on="unique_id", right_on="unique_id")

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




