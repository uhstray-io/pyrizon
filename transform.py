import pandas as pd
import sklearn.feature_extraction.text as sk_text
import sklearn.preprocessing

filepaths = list()

def tfidfAnalysis(dataframe):

    tfidf_frame = sk_text.TfidfTransformer(dataframe, use_idf=True, smooth_idf=True, norm='l2')
    print("Your TF-IDF matrix is " + tfidf_frame)
    
    return tfidf_frame 

def df_ez_pivot(df):
    index=df.iloc[:,0]
    column=df.iloc[:,1]
    pivot_frame = df.pivot(index=index, columns=column)
    print("Your pivot table is " + pivot_frame)
    return pivot_frame
