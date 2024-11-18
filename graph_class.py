from identifiable_entity import IdentifiableEntity
from csv import DictReader
from pprint import pprint
from person import Person

with open("meta.csv", "r", encoding="utf-8") as f:
    meta = DictReader(f)
    meta_dict = {row["Id"]: row for row in meta}

pprint(meta_dict)

class Author(Person):
    def __init__(self, name):
        super().__init__(name)

class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, entity_id, title, date, owner, place):
        super().__init__(entity_id)
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.authors = []  # Inicializa a lista de autores

    def getTitle(self):
        return self.title
    
    def getDate(self):
        return self.date
    
    def getOwner(self):
        return self.owner
    
    def getPlace(self):
        return self.place
    
    def addAuthor(self, author):
        if isinstance(author, Author):  # Verifica se é um autor
            self.authors.append(author)
    
    def removeAuthor(self, author):
        if author in self.authors:
            self.authors.remove(author)

    def getAuthors(self):
        return [author.getName() for author in self.authors]

# Classes específicas de objetos culturais
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
