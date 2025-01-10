from metadata_query_handler import MetadataQueryHandler
from process_data_query_handler import ProcessDataQueryHandler
from identifiable_entity import IdentifiableEntity
from graph_class import CulturalHeritageObject, Author
from person import Person
from activity import Activity
import pandas as pd
from pandas import DataFrame
from pandas import Series
import json  # Importazione del modulo standard json
from typing import List

class BasicMashup:
    def __init__(self, MetadataQuery, ProcessQuery):
        self.metadataQuery = MetadataQuery  # Store list of MetadataQueryHandler objects
        self.processQuery = ProcessQuery    # Store list of ProcessDataQueryHandler objects

    def cleanMetadataHandlers(self):
        self.metadataQuery = []

    def cleanProcessHandlers(self):
        self.processQuery = []

    def addMetadataHandler(self, handler: MetadataQueryHandler):
        self.metadataQuery.append(handler)

    def addProcessHandler(self, handler: ProcessDataQueryHandler):
        self.processQuery.append(handler)

    def _createEntityObject(self, entity_data: dict) -> IdentifiableEntity:
        # Assuming 'type' is a key in entity_data that indicates the specific subclass
        entity_type = entity_data.get('type', None)
        entity_id = entity_data.get('id')

        # Instantiate based on type
        if entity_type == 'Person':
            return Person(entity_data['name'], entity_id)
        elif entity_type == 'Author':
            return Author(entity_data['name'], entity_id)
        elif entity_type == 'CulturalHeritageObject':
            return CulturalHeritageObject(
                entity_id,
                entity_data['title'],
                entity_data['date'],
                entity_data['owner'],
                entity_data['place'],
            )
        # Add similar cases for other subclasses
        elif entity_type == 'NauticalChart':
            return NauticalChart(entity_id, entity_data['title'], entity_data['date'], entity_data['owner'], entity_data['place'])
        # Repeat for other types as defined
        else:
            return IdentifiableEntity(entity_id)  # Default case
        
    def getAllPeople(self):
        person_list = []

        if self.metadataQuery:  # Check if there are any handlers
        # Get DataFrames from all handlers
            new_person_df_list = [handler.getAllPeople() for handler in self.metadataQuery]

        # Filter out any empty DataFrames
        new_person_df_list = [df for df in new_person_df_list if not df.empty]

        if new_person_df_list:  # Proceed only if there are valid DataFrames
            # Merge the DataFrames
            person_df = new_person_df_list[0]
            for df in new_person_df_list[1:]:
                person_df = person_df.merge(df, on='author_id', how='inner')

            # Create the list of people
            person_list = [
                Person(row["author_id"], row["author_name"])
                for _, row in person_df.iterrows()
                if row["author_id"].strip() and row["author_name"].strip()
            ]

        return person_list
    
    def getAllCulturalHeritageObjects(self) -> List[CulturalHeritageObject]: 
        cho_list = []
    
        if self.metadataQuery:  # If the list is not empty
            new_object_df_list = []

        # Collect DataFrames from all handlers
        for handler in self.metadataQuery:
            try:
                new_object_df = handler.getAllCulturalHeritageObjects()
                if not new_object_df.empty:  # Check if the DataFrame is not empty
                    new_object_df_list.append(new_object_df)
            except Exception as e:
                print(f"Error retrieving cultural heritage objects from handler {handler}: {e}")
        
        if new_object_df_list:  # Proceed only if there are valid DataFrames
            try:
                # Merge the DataFrames
                cho_df = new_object_df_list[0]
                for d in new_object_df_list[1:]:
                    cho_df = cho_df.merge(d, on=['id'], how='inner')
                
                cho_df.drop_duplicates(subset=['id'], keep='first', inplace=True)  # Remove duplicates after merging
                
                # Create the list of CulturalHeritageObject instances
                cho_list = self.createObjectList(cho_df)
            except Exception as e:
                print(f"Error merging or processing the DataFrames: {e}")
    
        if not cho_list:
            print("No cultural heritage objects found.")
    
        return cho_list

def getAuthorsOfCulturalHeritageObject(self, objectId: str) -> List[Person]:
    author_list = []

    if self.metadataQuery:  # If the list is not empty
        new_person_df_list = []

        # Collect DataFrames from all handlers
        for handler in self.metadataQuery:
            try:
                new_person_df = handler.getAuthorsOfCulturalHeritageObject(objectId)
                if not new_person_df.empty:  # Check if the DataFrame is not empty
                    new_person_df_list.append(new_person_df)
            except Exception as e:
                print(f"Error retrieving authors for object {objectId} from handler {handler}: {e}")

        if new_person_df_list:  # Proceed only if there are valid DataFrames to merge
            try:
                # Merge the DataFrames
                author_df = pd.concat(new_person_df_list, ignore_index=True).drop_duplicates(subset=['author_id'], keep='first')

                # Create the list of Person instances
                for idx, row in author_df.iterrows():
                    if row["author_id"].strip() and row["author_name"].strip():
                        person = Person(row["author_id"], row["author_name"])
                        author_list.append(person)

            except Exception as e:
                print(f"Error during the merging or processing of DataFrames: {e}")

    return author_list 

def getCulturalHeritageObjectsAuthoredBy(self, AuthorId: str) -> List[CulturalHeritageObject]: 
    cho_list = []

    if self.metadataQuery:  # If the list is not empty
        new_object_df_list = []
        
        # Collect DataFrames from all handlers
        for handler in self.metadataQuery:
            try:
                new_object_df = handler.getCulturalHeritageObjectsAuthoredBy(AuthorId)
                if not new_object_df.empty:  # Check that the DataFrame is not empty
                    new_object_df_list.append(new_object_df)
            except Exception as e:
                print(f"Error retrieving cultural heritage objects for AuthorId {AuthorId} from handler {handler}: {e}")

        if new_object_df_list:  # Proceed only if there are valid DataFrames to merge
            try:
                # Merge the DataFrames
                cho_df = pd.concat(new_object_df_list, ignore_index=True).drop_duplicates(subset=['id'], keep='first')

                # Create the list of CulturalHeritageObject instances
                cho_list = self.createObjectList(cho_df)

            except Exception as e:
                print(f"Error during merging or processing the DataFrames: {e}")
        else:
            print(f"No valid data found for AuthorId {AuthorId}.")
    
    return cho_list




    


