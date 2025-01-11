from metadata_query_handler import MetadataQueryHandler
from process_data_query_handler import ProcessDataQueryHandler
from identifiable_entity import IdentifiableEntity
from graph_class import CulturalHeritageObject, Author, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map
from person import Person
from activity import Activity
import pandas as pd
from pandas import DataFrame
from pandas import Series
import json  # Importazione del modulo standard json
from typing import List, Optional

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
