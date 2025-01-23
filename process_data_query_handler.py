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
    def __init__(self, dbPathOrUrl = ""):
        super().__init__(dbPathOrUrl)
    
    def getById(self, id: str) -> pd.DataFrame:
        return pd.DataFrame()

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

        activities = pd.concat([dfs["Acquisition"], dfs["Processing"], dfs["Modelling"], dfs["Optimising"], dfs["Exporting"]], ignore_index=True)
        activities = pd.merge(activities, tool_sql_df, left_on="unique_id", right_on="unique_id")

        
        print("Activities type:", type(activities))  # Debugging
        print("Acquisition type:", type(acquisition_sql_df))  # Debugging
        return activities


    def getActivitiesByResponsibleInstitution(self, partialName):
        institution_df = DataFrame()
        # handle empty input strings
        if not partialName:
            institution_df = "No match found."
        else:
            # filter the df based on input string
            cleaned_input = partialName.lower().strip()
            institution_df = activities[activities["responsible institute"].str.lower().str.strip().str.contains(cleaned_input) | activities["responsible institute"].str.lower().str.strip().eq(cleaned_input)]

    # handle non matching inputs
            if institution_df.empty:
                institution_df = "No match found."

        return institution_df
    
    
    def getActivitiesByResponsiblePerson(self, partialName):
        person_df = DataFrame()
        #handle empty input strings
        if not partialName:
            person_df = "No match found."
        else:
            #filter the df based on input string
            cleaned_input = partialName.lower().strip()
            person_df = activities[activities["responsible person"].str.lower().str.strip().str.contains(cleaned_input) | activities["responsible person"].str.lower().str.strip().eq(cleaned_input)]

            # handle non matching inputs
            if person_df.empty:
                person_df = "No match found."

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
        # fetch Acquisition table
        with connect("relational.db") as con:
            query = "SELECT * FROM Acquisition"

            acquisition_sql_df = read_sql(query, con)

        merged_df = pd.merge(acquisition_sql_df, tool_sql_df, on="unique_id", how="inner")
        filtered_df = merged_df[merged_df["technique"].str.contains(inputtechnique, case=False, na=False)]
                    
        return filtered_df

    def getActivitiesUsingTool(self, tool):
    
        # Normalize the tool string for comparison
        tool_lower = tool.lower()
    
        # Filter rows where the tool column matches the exact or partial tool name
        activities_tool = activities[activities['tool'].str.lower().str.contains(tool_lower,  case=False, na=False)]
    
        return activities_tool