from metadata_query_handler import MetadataQueryHandler
from process_data_query_handler import ProcessDataQueryHandler
from identifiable_entity import IdentifiableEntity
from graph_class import CulturalHeritageObject, Author
from person import Person
from activity import Activity

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