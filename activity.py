class Activity(object):
    def __init__(self, institute, person, tools, start, end, refersTo):
        self.institute = institute
        self.person = person
        self.tool = set()
        for t in tools:
            self.tool.add(t)

        self.start = start
        self.end = end
        self.refersTo = refersTo
    
    def getResponsibleInstitute(self):
        return self.institute

    def getResponsiblePerson(self):
        if self.person:
            return self.person
        else:
            return None

    #def getTools(self):
     #   return self.tool[tool]

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

    # def refersTo(self):
    #   it has to return the CulturalHeritageObject 