from metadata_query_handler import MetadataQueryHandler
from process_data_query_handler import ProcessDataQueryHandler
from identifiable_entity import IdentifiableEntity
from graph_class import CulturalHeritageObject, Author, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map
from person import Person
from activity import Activity, Acquisition, Processing, Modelling, Optimising, Exporting
from basic_mashup import BasicMashup

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
    
    
    def getObjectsHandledByResponsibleInstitution(self, institution:str):       
        objects_list = []

        #Check if institution or the queries are empty
        if not institution or not self.processQuery or not self.metadataQuery:
            return objects_list  # Return an empty list
        else:
            # Get the activities of the responsible institution with a match
            activities = self.getActivitiesByResponsibleInstitution(institution)

        # Get all cultural heritage objects
        object_list = self.getAllCulturalHeritageObjects()

        object_ids_list = []

        # Iterate through each activity in activities
        for activity in activities:
            # Extracts the id of the cultural heritage object referred to by the activity
            object_id = activity.refersTo_cho.id
            # Adds the id to the object_ids_list only if it is not already in the list
            if object_id not in object_ids_list:
                object_ids_list.append(object_id)

        # Iterates through each cultural heritage object in object_list
        for cho in object_list:
            # Checks if the object’s id exists in object_ids_list
            if cho.id in object_ids_list and cho not in objects_list:
                objects_list.append(cho)
    
        # Returns the final list of cultural heritage objects associated with the given institution
        return objects_list