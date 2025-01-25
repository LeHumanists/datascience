import datetime
from handler import Handler
from query_handler import QueryHandler
import pandas as pd
from pandas import DataFrame
from pandas import concat
from pandas import read_sql
from sqlite3 import connect
from pandas import merge

acquisition_sql_df = DataFrame()
tool_sql_df= DataFrame()

def query_rel_db():
    activities = DataFrame()
    with connect("relational.db") as con:
        queries = {
            "Acquisition": "SELECT * FROM Acquisition",
            "Processing": "SELECT * FROM Processing",
            "Modelling": "SELECT * FROM Modelling",
            "Optimising": "SELECT * FROM Optimising",
            "Exporting": "SELECT * FROM Exporting",
            "Tools": "SELECT * FROM Tools",
        }
        df_dict = {}
        for key, query in queries.items():
            df_dict[key] = read_sql(query, con)
            if df_dict[key].empty:
                print(f"Warning: {key} table is empty.")

    activities = pd.concat([df_dict["Acquisition"], df_dict["Processing"], df_dict["Modelling"], df_dict["Optimising"], df_dict["Exporting"]], ignore_index=True)
    tool_sql_df = df_dict["Tools"]
    activities = pd.merge(activities, tool_sql_df, left_on="unique_id", right_on="unique_id")

    return activities

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self, dbPathOrUrl = ""):
        super().__init__(dbPathOrUrl)
    
    def getById(self, id: str) -> pd.DataFrame:
        return pd.DataFrame()

    def getAllActivities(self):
        
        return query_rel_db()


    def getActivitiesByResponsibleInstitution(self, partialName):
        activities = query_rel_db()
        institution_df = DataFrame()
        # handle empty input strings
        if not partialName:
           print("The input string is empty.")
           return institution_df
        else:
            # filter the df based on input string
            cleaned_input = partialName.lower().strip()
            institution_df = activities[activities["responsible institute"].str.lower().str.strip().str.contains(cleaned_input) | activities["responsible institute"].str.lower().str.strip().eq(cleaned_input)]

    # handle non matching inputs
            if institution_df.empty:
                print("No match found.")

        return institution_df
    
    
    def getActivitiesByResponsiblePerson(self, partialName):
        activities = query_rel_db()
        person_df = DataFrame()
        #handle empty input strings
        if not partialName:
            print("The input string is empty.")
            return person_df
        else:
            #filter the df based on input string
            cleaned_input = partialName.lower().strip()
            person_df = activities[activities["responsible person"].str.lower().str.strip().str.contains(cleaned_input) | activities["responsible person"].str.lower().str.strip().eq(cleaned_input)]

            # handle non matching inputs
            if person_df.empty:
                print("No match found.")

        return person_df
    

    def getActivitiesStartedAfter(self, date):
        activities = query_rel_db()
        start_date_df = DataFrame()

        activities.columns = activities.columns.str.strip()
        print("Columns in the activities df:", activities.columns)

        start_date_df = activities[(activities["start date"] >= date) & (activities["start date"] != '')]
        
        if start_date_df.empty:
            print("No match found.")
        
        return start_date_df

    
    def getActivitiesEndedBefore(self, date):
        activities = query_rel_db()
        end_date_df = DataFrame()

        end_date_df = activities[(activities["end date"] <= date) & (activities["end date"] != '')]
        
        if end_date_df.empty:
            print( "No match found.")
        
        return end_date_df

    
    def getAcquisitionsByTechnique(self, inputtechnique):
        # fetch Acquisition table
        with connect("relational.db") as con:
            query1 = "SELECT * FROM Acquisition"
            query2 = "SELECT * FROM Tools"

            acquisition_sql_df = read_sql(query1, con)
            tool_sql_df = read_sql(query2, con)

        merged_df = pd.merge(acquisition_sql_df, tool_sql_df, on="unique_id", how="inner")
        filtered_df = merged_df[merged_df["technique"].str.contains(inputtechnique, case=False, na=False)]

        if filtered_df.empty:
            print("No match found")  

        return filtered_df

    def getActivitiesUsingTool(self, tool):
        activities = query_rel_db()
        # Normalize the tool string for comparison
        tool_lower = tool.lower()
    
        # Filter rows where the tool column matches the exact or partial tool name
        activities_tool = activities[activities['tool'].str.lower().str.contains(tool_lower,  case=False, na=False)]
    
        if activities_tool.empty:
            print("No match found")
            
        return activities_tool