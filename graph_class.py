from csv import reader

with open("meta.csv", "r", encoding="utf-8") as f:
    meta = reader(f)

from pprint import pprint

pprint (type(meta))
        
with open("meta.csv", "r", encoding="utf-8") as f:
    meta = reader(f)
    next(meta) 
    
    for row in meta:
        pprint(row)

with open("meta.csv", "r", encoding="utf-8") as f:
    meta = reader(f)
    
    print("--First iteration")
    for row in meta:
        pprint(row)   
    
    print("\n-- Second iteration")
    for row in meta:
        pprint(row)  
        
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

my_meta_list =[ 
    ["column name", "column name", "column name", "column name", "column name", "column name"],

    ]
pprint (my_meta_list)

ninth_row = my_meta_list[8]
print("-- ninth row")
pprint(ninth_row)

class CulturalHeritageObject(object):
    def __init__(self,title, date, owner, place):
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
