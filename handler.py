class Handler(object):
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = dbPathOrUrl
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, url):
        self.dbPathOrUrl = url

class UploadHandler(Handler):
    pass

    def pushDataToDb(self, file_path) -> bool:
        # return True if data is uploaded to the db
        pass