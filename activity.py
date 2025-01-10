from graph_class import CulturalHeritageObject

class Activity(object):
    def __init__(self, institute, person, tools, start, end, refers_to):
        self.institute = institute
        self.person = person
        self.tools = list()
        for t in tools:
            self.tools.append(t)

        self.start = start
        self.end = end
        self.refers_to = refers_to
    
    def getResponsibleInstitute(self):
        return self.institute

    def getResponsiblePerson(self):
        if self.person:
            return self.person
        else:
            return None

    def getTools(self):
        # it returns a list of strings
        return self.tool

    def getStartDate(self):
        if self.start:
            return self.start
        else:
            return None

    def getEndDate(self):
        if self.end:
            return self.end
        else:
            return None

    def refersTo(self):
        return CulturalHeritageObject

class Acquisition(Activity):
    def __init__(self, institute, person, tools, start, end, refersTo, technique):
        self.technique = technique

        super().__init__(institute, person, tools, start, end, refersTo)
    
    def getTechnique(self):
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass