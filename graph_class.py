from identifiable_entity import IdentifiableEntity

from csv import reader

with open("meta.csv", "r", encoding="utf-8") as f:
    meta = reader(f)

from pprint import pprint

pprint (type(meta)) 
        
with open("meta.csv", "r", encoding="utf-8") as f:
    meta = reader(f)
    meta_list = list(meta)
    
print("-- First execution")
for row in meta_list:
    pprint(row)  

print("\n-- Second executuion")
for row in meta_list:
    pprint(row)

class Person(object):
    def __init__ (self, name):
        self.name = name
        
    def getName(self):
        return self.name




class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, entity_id, title, date, owner, place):
        super().__init__(entity_id)
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        
    def getTitle(self):
        return self.title
    
    def getDate(self):
        return self.date
    
    def getOwner(self):
        return self.owner
    
    def getPlace(self):
        return self.place
    
class NauticalChart (CulturalHeritageObject):
    pass

class ManuscriptPlate (CulturalHeritageObject):
    pass

class ManuscriptVolume (CulturalHeritageObject):
    pass

class PrintedVolume (CulturalHeritageObject):
    pass

class PrintedMaterial (CulturalHeritageObject):
    pass

class Herbarium (CulturalHeritageObject):
    pass

class Specimen (CulturalHeritageObject):
    pass

class Painting (CulturalHeritageObject):
    pass

class Model (CulturalHeritageObject):
    pass

class Map (CulturalHeritageObject):
    pass
