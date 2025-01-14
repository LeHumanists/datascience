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
    
