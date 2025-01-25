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
    

    def combineAuthorsOfObjects(self, df, authorId: str, handler):
        # Checks if the "Author" column exists in the dataframe
        if "authors" in df.columns:
            df_list=[]
            
            for handler in self.metadataQuery: 
                df_objects= handler.getAllCulturalHeritageObjects(authorId)
                df_authors= handler.getAllPeople()

            # Iterate through each row of the dataframe
            for idx, row in df.iterrows():# Combine object and author information
                if df_objects is not None and not df_objects.empty:
                    if df_authors is not None and not df_authors.empty:
                        # Create a map of objectID to list of Author instances
                        object_authors_map = {}
                        for obj_id in df_objects["id"].unique():
                            authors = df_authors[df_authors["objectID"] == obj_id]
                            object_authors_map[obj_id] = [
                                Author(name=row["name"], identifier=row["authorId"])
                                for _, row in authors.iterrows()
                            ]
                        
                        # Add the authors list to each row in df_objects
                        df_objects["Authors"] = df_objects["id"].map(object_authors_map.get)
                    else:
                        df_objects["Authors"] = []

                df_list.append(df_objects)
        # Remove duplicate rows in the dataframe
        return df.drop_duplicates()  # Return the dataframe without duplicate rows




    
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
            else:
                # Handle the case where the type is not mapped
                print(f"Warning: No object type: {obj_type} found.")
            
                # Continue to the next row without interrupting the process
                continue

        return obj_result_list


    def getAuthorsOfCulturalHeritageObject(self, id: str):
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
    
    def getCulturalHeritageObjectsAuthoredBy(self, authorId: str): 
        # List to collect the final cultural heritage objects      
        object_result_list = [] 
    
        # Check if there are any handlers to query
        if not self.metadataQuery: 
            return object_result_list  # Return an empty list if no handlers are available 
    
        # List to collect DataFrames to be merged
        df_list = [] 
    
        # Iterate over each handler in self.metadataQuery
        for handler in self.metadataQuery:  
            # Retrieve cultural heritage objects authored by the given author
            df_objects = handler.getCulturalHeritageObjectsAuthoredBy(authorId)  
        
            # If the DataFrame is not empty
            if df_objects is not None and not df_objects.empty:  
                # Combine author data with cultural heritage objects
                df_object_update = self.combineAuthorsOfObjects(df_objects, handler)  
            
                # Add the processed DataFrame to the df_list
                df_list.append(df_object_update)  
        
        # If df_list is empty, return it as an empty list
        if not df_list:  
            return df_list  
    
        # Merge all DataFrames into a single one, remove duplicates, and handle NaN values
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
    
        # Iterate through each row of the merged DataFrame
        for _, row in df_union.iterrows(): 
            obj_type = row['type']  # Extract the object type from the 'type' column
        
            # Check if obj_type is in type_mapping
            if obj_type in type_mapping: 
                # Get the class associated with the object type
                object_class = type_mapping[obj_type]  
            
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
                # If the object type is not present in type_mapping print a warning
                print(f"Warning: No object type: {obj_type} found")  
                
                # Continue to the next row without interrupting the process
                continue

        return object_result_list  # Return the list of created objects
    

    # methods for relational db start here
    
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
                person_df = process_qh.getActivitiesByResponsibleInstitution(partialName)
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
        if self.processQuery:
            act_by_technique_df_list = [process_qh.getAcquisitionsByTechnique(inputtechnique) for process_qh in self.processQuery]

            concat_df = concat(act_by_technique_df_list, ignore_index=True)
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
    print("The input activity df:", activity_df)
    for idx, row in activity_df.iterrows():
        activity_from_id = re.sub("_\\d+", "", row["unique_id"])
        print("The current activity:", activity_from_id)
        if activity_from_id in activity_mapping.keys() and activity_from_id == "acquisition":
            activity_obj = Acquisition(row["responsible institute"], row["responsible person"], row["tool"], row["start date"], row["end date"], row["refers_to"], row["technique"])
            activity_list.append(activity_obj)
        elif activity_from_id in activity_mapping.keys() and activity_from_id != "acquisition":
            class_to_use = activity_mapping.get(activity_from_id)
            activity_obj = class_to_use(row["responsible institute"], row["responsible person"], row["tool"], row["start date"], row["end date"], row["refers_to"])
            activity_list.append(activity_obj)
    
    return activity_list

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

