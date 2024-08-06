import pandas as pd
import sklearn.feature_extraction.text as sk_text

filepaths = list()


def ingest_csv(filepaths):
    csv_frame = pd.DataFrame()
    for filename in filepaths:
        csv_frame = pd.read_csv(filename)
        
        print("Your column headers are " + csv_frame.columns)
        print("Your data types are " + csv_frame.dtypes)
        
        csv_frame.append(pd.read_csv(filename, engine='pyarrow'))
    return csv_frame

def ingest_json(filepaths):
    json_frame = pd.DataFrame()
    for filename in filepaths:
        json_frame  = pd.read_json(filename)
        
        print("Your column headers are " + json_frame.columns)
        print("Your data types are " + json_frame.dtypes)
        
    return json_frame
        
        
