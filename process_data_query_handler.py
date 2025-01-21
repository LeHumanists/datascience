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
            queries = {
                "Acquisition": "SELECT * FROM Acquisition",
                "Processing": "SELECT * FROM Processing",
                "Modelling": "SELECT * FROM Modelling",
                "Optimising": "SELECT * FROM Optimising",
                "Exporting": "SELECT * FROM Exporting",
                "Tools": "SELECT * FROM Tools",
            }
            dfs = {}
            for key, query in queries.items():
                dfs[key] = read_sql(query, con)
                if dfs[key].empty:
                    print(f"Warning: {key} table is empty.")

        activities = concat([dfs["Acquisition"], dfs["Processing"], dfs["Modelling"], dfs["Optimising"], dfs["Exporting"]], ignore_index=True)
        activities = merge(activities, tool_sql_df, left_on="unique_id", right_on="unique_id")

        acquisition_sql_df = merge(acquisition_sql_df, tool_sql_df, on="unique_id", how="inner")
        print("Activities type:", type(activities))  # Debugging
        print("Acquisition type:", type(acquisition_sql_df))  # Debugging
        return activities, acquisition_sql_df


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

        # Merge  the activities DataFrame  with the tool DataFrame
        #activities_with_tool = merge(activities, tool_sql_df, left_on="unique_id", right_on="unique_id")
        activities = merge(activities, tool_sql_df, left_on="unique_id", right_on="unique_id")
    
        # Normalize the tool string for comparison
        tool_lower = tool.lower()
    
        # Filter rows where the tool column matches the exact or partial tool name
        activities_tool = activities[
            activities['tool'].str.lower().str.contains(tool_lower,  case=False, na=False)
        ]
    
        return activities_tool
        
    
 
   