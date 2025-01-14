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
        
        # Iterate over all metadata query handlers
        for metadata_handler in self.metadataQuery:
            authored_objects = metadata_handler.getCulturalHeritageObjectsAuthoredBy(person_id)
            for ch_object in authored_objects:
                object_id = ch_object.getId()  # Assuming ch_object has a getId() method
                
                # Iterate over all process query handlers
                for process_handler in self.processQuery:
                    object_activities = process_handler.getAllActivities()
                    for activity in object_activities:
                        related_object_ids = activity.getRelatedObjectIds()  # Assuming activity has getRelatedObjectIds()
                        
                        if object_id in related_object_ids:
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