import pandas as pd
import sklearn.feature_extraction.text as sk_text

filepaths = list()

def tfidfAnalysis(dataframe):

    tfidf_frame = sk_text.TfidfTransformer(dataframe, use_idf=True, smooth_idf=True, norm='l2')
    print("Your TF-IDF matrix is " + tfidf_frame)
    
    return dataframe, tfidf_frame