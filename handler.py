class Handler(object):
    def __init__(self, dbPathOrUrl: str = ""):
        self.dbPathOrUrl = dbPathOrUrl
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, url):
        self.dbPathOrUrl = url
        return True  # Return True to indicate success


class UploadHandler(Handler):
    pass

    def pushDataToDb(self, file_path) -> bool:
        # return True if data is uploaded to the db
        pass
    
    
