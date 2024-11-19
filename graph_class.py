from identifiable_entity import IdentifiableEntity
from person import Person
import pandas as pd
import re

class Author(Person):
    def __init__(self, name, identifier=None):
        super().__init__(name)
        self.identifier = identifier 
        
    def getIdentifier(self):
        return self.identifier

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, entity_id, title, date, owner, place):
        super().__init__(entity_id)
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
