from handler import Handler, UploadHandler
import pandas as pd
from json import load
from json import dump

class ProcessDataUploadHandler(UploadHandler):
    pass

    # open json file
    with open("data/process.json", "r", encoding="utf-8") as f:
        json_data = load(f)
    
    # extract acquisition information
    acquisitions = []
    for item in json_data:
        acquisitions.append(item["acquisition"])
    
    print(acquisitions)

    # dump acquisition info into a new json file
    with open("acquisition_data.json", "w", encoding="utf-8") as f:
        dump(acquisitions, f, ensure_ascii=False, indent=4)

    # open new file in a pd dataframe
    acquisition_data = pd.read_json("data/acquisition_data.json", dtype={"responsible institute": "string", "responsible person": "string", "technique": "string", "tool": "object", "start date": "string", "end date": "string"})
    print(acquisition_data)

    # extract single-valued attributes
    acquisition_table = acquisition_data[["responsible institute", "technique"]]
    print(acquisition_table)

    # now tables for multi-valued attributes + primary keys + table for CHO

