class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, url):
        self.dbPathOrUrl = url

class UploadHandler(Handler):
    pass

    def pushDataToDb(self, file_path) -> bool:
        # return True if data is uploaded to the db
        pass