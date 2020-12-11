#functions
def first_contact(dataframe):
    print("-----------------------------------------")
    print("dataframe shape is :\t")
    print(dataframe.shape)
    print("data_columns are : \n")
    print(dataframe.columns)
    print(dataframe.dtypes)
    print("\n Number of values missing in each columns:\n")
    print(dataframe.isnull().sum())
    print("-----------------------------------------")
    return

def drop_too_much(dataframe,val=0.8):
    """return a list of column where the missing value % is sup to val"""
    if val >1:
        raise ValueError("val should be inf to 1")
    threshold = len(dataframe)*val 
    to_drop =[i for i,x in dataframe.isnull().sum().iteritems() if x>=threshold ]
    return to_drop
    
def searching_possible_transformable_variables(dataframe,v=False):
    for colname,coldata in dataframe.iteritems():
        if coldata.nunique()<= 15 and dataframe[colname].dtypes==object:
            print("For the column "+colname)
            print("The number of unique values is " +str(dataframe[colname].nunique()))
            print(dataframe[colname].value_counts())
            print(dataframe[colname].dtypes)
            continue

def is_inDay(item_pd_series):
    hour_time = int(item_pd_series.split(":")[0])
    if hour_time > 6 and hour_time < 20:
        return 1
    return 0


