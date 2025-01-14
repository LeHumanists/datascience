from handler import Handler
from query_handler import QueryHandler
import pandas as pd
from pandas import DataFrame
from pandas import concat
from pandas import read_sql
from sqlite3 import connect
from pandas import merge

activities = DataFrame()
acquisition_sql_df = DataFrame()
tool_sql_df= DataFrame()

class ProcessDataQueryHandler(QueryHandler):
    pass

    def getAllActivities(self):
        
        with connect("relational.db") as con:
            query_acquisition = "SELECT * FROM Acquisition"
            acquisition_sql_df = read_sql(query_acquisition, con)

            query_processing = "SELECT * FROM Processing"
            processing_sql_df = read_sql(query_processing, con)

            query_modelling = "SELECT * FROM Modelling"
            modelling_sql_df = read_sql(query_modelling, con)

            query_optimising = "SELECT * FROM Optimising"
            optimising_sql_df = read_sql(query_optimising, con)

            query_exporting = "SELECT * FROM Exporting"
            exporting_sql_df = read_sql(query_exporting, con)

            query_tool = "SELECT * FROM Tools"
            tool_sql_df = read_sql(query_tool, con)


        activities = concat([acquisition_sql_df, processing_sql_df, modelling_sql_df, optimising_sql_df, exporting_sql_df], ignore_index=True)
        return activities


    def getActivitiesByResponsibleInstitution(self, partialName):
        institution_df = DataFrame()
        for idx, row in activities.iterrows(): # check if there is something to iterate over the rows without getting also the index
            for column_name, item in row.items():
                if column_name == "responsible institute":
                    # exact match
                    if partialName.lower() == item.lower():
                    # use backticks to refer to column names containing spaces and @ for variables
                        institution_df = activities.query("`responsible institute` == @item")
                    # partial match
                    elif partialName.lower() in item.lower():
                        institution_df = activities.query("`responsible institute` == @item")
                
        return institution_df
    
    def getActivitiesByResponsiblePerson(self, partialName):
        person_df = DataFrame()
        for idx, row in activities.iterrows():
            for column_name, person in row.items():
                if column_name == "responsible person":
                    # exact match
                    if partialName.lower() == person.lower():
                        # use backticks to refer to column names containing spaces and @ for variables
                        person_df = activities.query("`responsible person` == @person")
                    # partial match
                    elif partialName.lower() in person.lower():
                        person_df = activities.query("`responsible person` == @person")

        return person_df
    
    def getActivitiesStartedAfter(self, date):
        start_date_df = DataFrame()
        for idx, row in activities.iterrows():
            for column_name, item in row.items():
                if column_name == "start date":
                    start_date_df = activities.query("`start date` >= @date")
        
        return start_date_df
    
    def getActivitiesEndedBefore(self, date):
        end_date_df = DataFrame()
        for idx, row in activities.iterrows():
            for column_name, item in row.items():
                if column_name == "end date":
                     end_date_df = activities.query("`end date` <= @date and `end date` != ''")
        
        return end_date_df

    
    def getAcquisitionsByTechnique(self, inputtechnique):
        technique_df = DataFrame()
        for idx, row in acquisition_sql_df.iterrows():
            for column_name, technique in row.items():
                if column_name == "technique":
                    # exact match
                    if inputtechnique.lower() == technique.lower():
                    # use backticks to refer to column names containing spaces and @ for variables
                        technique_df = acquisition_sql_df.query("technique` == @technique")
                    # partial match
                    elif inputtechnique.lower() in technique.lower():
                        technique_df = acquisition_sql_df.query("`technique` == @technique")
                    
        return technique_df
    

    def getActivitiesUsingTool(self, tool):
        activities_with_tool = merge(activities, tool_sql_df, left_on="unique_id", right_on="unique_id")
        activities_tool = DataFrame()    
        for idx, row in activities_with_tool.iterrows():
            for column_name, value in row.items():
                if column_name == "tool":
                # Corrispondenza esatta
                    if value.lower() == tool.lower():
                        activities_tool = activities.query("`tool` == @tool")
                # Corrispondenza parziale
                    elif tool.lower() in value.lower():
                         activities_tool = activities.query("`tool` == @tool")
    
        return activities_tool
