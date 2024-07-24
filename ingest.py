import pandas as pd
import sklearn.feature_extraction.text as sk_text

filepaths = list()

def ingest_csv ( filepaths ):
    csv_frame = pd.DataFrame()
    tfidf_frame = pd.DataFrame()
    for filename in filepaths:
        csv_frame.columns = pd.read_csv(filename)
        print("Your column headers are " + csv_frame.columns)
        tfidf_frame = sk_text.TfidfTransformer(csv_frame,use_idf=True,smooth_idf=True,norm='l2')
        print("Your TF-IDF matrix is " + tfidf_frame)
        csv_frame.append(pd.read_csv(filename,engine='pyarrow'))
    return csv_frame