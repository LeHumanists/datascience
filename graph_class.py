class Person(object):
    def __init__ (self, name):
        self.name = name
        
    def getName(self):
        return self.name
        


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
