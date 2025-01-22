import ast
import pandas as pd
from pandas import DataFrame
from pandas import Series
from pandas import concat
from collections import deque
from json import load
from sqlite3 import connect
from handler import Handler, UploadHandler

class ProcessDataUploadHandler(UploadHandler):
    pass

    # the method of ProcessDataUploadHandler that pushes data to the rel db
    def pushDataToDb(self, file_path):

        # open json file
        with open("data/process.json", "r", encoding="utf-8") as f:
            json_data = load(f)
    
        # function for extracting data from json
        def data_from_json(json_object, dict_key):
            # initialize list empty
            process = []
            # set counter
            count = 0
            # iterate over dictionaries in the input list
            for item in json_object:
                # add 1 to the counter at each iteration
                count += 1
                # iterate over key,value pairs for each dictionary
                for key, val in item.items():
                    # check if key is equal to the input key
                    if key == dict_key:
                        # if it is, append val (the inner dictionary) to the process list
                        process.append(val)
                        # and add the object id to the inner dictionary
                        val.update(refers_to="object_" + str(count))
            
            # return the list        
            return process

        # function calls
        acquisition = data_from_json(json_data, "acquisition")
        processing = data_from_json(json_data, "processing")
        modelling = data_from_json(json_data, "modelling")
        optimising = data_from_json(json_data, "optimising")
        exporting = data_from_json(json_data, "exporting")

        print("Acquisition list:\n", acquisition)

        # function for populating dataframes from lists
        def populateDf(process_list): 
        
            # assign datatype string to the entire dataframe
            df = pd.DataFrame(process_list, dtype="string")
            # then cast "tool" column to dtype object
            df["tool"] = df["tool"].astype("object")
            
            return df

        # function calls
        acquisition_df = populateDf(acquisition)
        processing_df = populateDf(processing)
        modelling_df = populateDf(modelling)
        optimising_df = populateDf(optimising)
        exporting_df = populateDf(exporting)

        print("Acquisition dataframe:\n", acquisition_df)
        print("Acquisition dataframe info:", acquisition_df.info())

        # create unique identifiers and append id column to df
        def createUniqueId(process_df, df_name):
            id = []
            # iterate over dataframe rows
            for idx, row in process_df.iterrows():
                # at each iteration, append to the list a string composed by dataframe name + underscore + the index of the row to be used as unique identifier
                id.append(str(df_name) + "_" + str(idx))

            # convert the list to a Series and insert it as first column in the df, with name "unique_id"
            process_df.insert(0, "unique_id", Series(id, dtype="string"))

            return process_df

        # function calls
        createUniqueId(acquisition_df, "acquisition")
        createUniqueId(processing_df, "processing")
        createUniqueId(modelling_df, "modelling")
        createUniqueId(optimising_df, "optimising")
        createUniqueId(exporting_df, "exporting")

        print("Acquisition df with unique ids:\n", acquisition_df)

        # remove multi-valued attributes from df
        def keep_single_valued(process_df):
            # dtypes stores a series where the first column lists the index for the Series (the column names) and the second one the datatype for each column
            dtypes = process_df.dtypes
            print(isinstance(dtypes, pd.Series))
            # iterate over the columns in the Series
            for column_name, datatype in dtypes.items():
                # if the column has datatype object...
                if datatype == object:
                    #... create a sub-dataframe containing the column and the unique_id, associate a dtype to the column and drop the multi-valued column from the dataframe
                    multi_valued = process_df[["unique_id", column_name]]
                    multi_valued[column_name].astype("object")
                    process_df.drop("tool", axis=1, inplace=True)

            # return the df and the popped column
            return process_df, multi_valued
    
        # function calls
        acquisition_df, acquisition_multi_valued = keep_single_valued(acquisition_df)
        processing_df, processing_multi_valued = keep_single_valued(processing_df)
        modelling_df, modelling_multi_valued = keep_single_valued(modelling_df)
        optimising_df, optimising_multi_valued = keep_single_valued(optimising_df)
        exporting_df, exporting_multi_valued = keep_single_valued(exporting_df)
        print("Acquisition df and multi-valued df:\n", acquisition_df, acquisition_multi_valued)
        print(acquisition_df.info())
        print(acquisition_multi_valued.info())
        
        # create multi-valued attributes tables
        def create_multi_valued_tables(multi_valued_df):
            tools_dict = dict()
            for idx, row in multi_valued_df.iterrows():
                # populate dictionary with unique identifiers as keys and lists of tools as values
                #tools_dict[row.iloc[0]] = row.iloc[1] # iloc to access values by position
                tools_dict[row.iloc[0]] = ast.literal_eval(row.iloc[1]) if isinstance(row.iloc[1], str) else row.iloc[1]

            print(tools_dict)

            #tools_dict = {key: [item.strip() for item in value.strip("[]").split(",") if item.strip()] for key, value in tools_dict.items()}
            #tools_dict = {key: [item for item in value if item] for key, value in tools_dict.items()}

            #tools = list(tools_dict.values())
            #identifiers = list(tools_dict.keys())
            tools_unpacked = []
            identifiers_unpacked = []

            #print("The list of tools:\n", tools)
            #print("The list of identifiers:\n", identifiers)

            # iterate over each tool in the inner lists
            for tool_list in tools_dict.values():
                # and append it to the pandas series
                if len(tool_list) < 1:
                    tools_unpacked.append("")
                else:
                    for t in tool_list:
                        tools_unpacked.append(t)

            print("list for tools:\n", tools_unpacked)

            # iterate over the list of identifiers
            for identifier in tools_dict.keys():
                # and append each identifier to the series as many times as the length of the list which is the value of the key corresponding to the identifier in the tools_dict
                """ if len(tools_dict[identifier]) < 1:
                    identifiers_unpacked.append(identifier)
                else:
                    for n in range(len(tools_dict[identifier])):
                        identifiers_unpacked.append(identifier) """
                list_length = len(tools_dict[identifier])
                if list_length < 1:
                    identifiers_unpacked.append(identifier)
                else:
                    for n in range(list_length):
                        identifiers_unpacked.append(identifier)

            print("list for identifiers:\n", identifiers_unpacked)

            # create a list that contains the two series and join them in a dataframe where each series is a column
            tools_series = pd.Series(tools_unpacked, dtype="string", name="tool")
            identifiers_series = pd.Series(identifiers_unpacked, dtype="string", name="unique_id")
            tools_df = pd.concat([identifiers_series, tools_series], axis=1)
            print("The dataframe for tools:\n", tools_df)
            
            return tools_df

        # function calls...
        ac_tools_df = create_multi_valued_tables(acquisition_multi_valued)
        pr_tools_df = create_multi_valued_tables(processing_multi_valued)
        md_tools_df = create_multi_valued_tables(modelling_multi_valued)
        op_tools_df = create_multi_valued_tables(optimising_multi_valued)
        ex_tools_df = create_multi_valued_tables(exporting_multi_valued)

        # function for merging tools-id tables
        def merge_mv_tables(table_1, table_2, table_3, table_4, table_5):
            merged = concat([table_1, table_2, table_3, table_4, table_5], ignore_index=True)

            return merged
        
        # function calls
        merged_tools_df = merge_mv_tables(ac_tools_df, pr_tools_df, md_tools_df, op_tools_df, ex_tools_df)
        print("The merged dataframe:\n", merged_tools_df)
        
        # pushing tables to db
        with connect("relational.db") as con:
            acquisition_df.to_sql("Acquisition", con, if_exists="replace", index=False)
            processing_df.to_sql("Processing", con, if_exists="replace", index=False)
            modelling_df.to_sql("Modelling", con, if_exists="replace", index=False)
            optimising_df.to_sql("Optimising", con, if_exists="replace", index=False)
            exporting_df.to_sql("Exporting", con, if_exists="replace", index=False)
            merged_tools_df.to_sql("Tools", con, if_exists="replace", index=False)
        
            # check and return result
            rel_db_ac = pd.read_sql("SELECT * FROM Acquisition", con)
            rel_db_pr = pd.read_sql("SELECT * FROM Processing", con)
            rel_db_md = pd.read_sql("SELECT * FROM Modelling", con)
            rel_db_op = pd.read_sql("SELECT * FROM Optimising", con)
            rel_db_ex = pd.read_sql("SELECT * FROM Exporting", con)
            rel_db_tl = pd.read_sql("SELECT * FROM Tools", con)

            populated_tables = not any(df.empty for df in [rel_db_ac, rel_db_pr, rel_db_op, rel_db_md, rel_db_ex, rel_db_tl]) # add rel_db_tl
            print(populated_tables)
            return populated_tables



# for uploading data to db (and testing)
rel_path = "relational.db"
file_path = "data/process.json"
process = ProcessDataUploadHandler()
process.setDbPathOrUrl(rel_path)
process.pushDataToDb(file_path)



    

