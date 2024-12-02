from identifiable_entity import IdentifiableEntity

class Person(IdentifiableEntity):
    def __init__(self, name):
        self.name = name
        
    def getName(self):
        return self.name
    