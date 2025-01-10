from metadata_query_handler import MetadataQueryHandler
from process_data_query_handler import ProcessDataQueryHandler
from identifiable_entity import IdentifiableEntity
from graph_class import CulturalHeritageObject, Author
from person import Person
from activity import Activity

class BasicMashup:
    def __init__(self, metadataQuery=None, processQuery=None):
        """Initialize metadataQuery and processQuery as lists of handler objects."""
        self.metadataQuery = metadataQuery if metadataQuery is not None else []  # List of MetadataQueryHandler
        self.processQuery = processQuery if processQuery is not None else []     # List of ProcessorDataQueryHandler

    def cleanMetadataHandlers(self) -> bool:
        """Clear the list of metadataQuery handlers."""
        self.metadataQuery.clear()
        return True

    def cleanProcessHandlers(self) -> bool:
        """Clear the list of processQuery handlers."""
        self.processQuery.clear()
        return True

    def addMetadataHandler(self, handler) -> bool:
        """Add a MetadataQueryHandler object to the metadataQuery list."""
        if handler not in self.metadataQuery:
            self.metadataQuery.append(handler)
            return True
        return False

    def addProcessHandler(self, handler) -> bool:
        """Add a ProcessorDataQueryHandler object to the processQuery list."""
        if handler not in self.processQuery:
            self.processQuery.append(handler)
            return True
        return False
    
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