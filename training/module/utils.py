import pandas as pd
import sys  #system specific parameters and names
import gc   #garbage collector interface
from datetime import datetime

def read_file(data):
    """
    shows data size, missing values, type
    
    input: dataframe
    """
    print('SHAPE',data.shape)
    print('=========================')# Investigate null rate of contained null columns
    
    df_missing_rate=pd.DataFrame([],columns=['column','total_row_missing','missing_value_rate'])
    for i,j in zip(data.columns,data.isna().sum()):
        if j>0:
            df_missing_rate=df_missing_rate._append({'column':i,'total_row_missing':j,'missing_value_rate':'{}%'.format(round(data[i].isnull().sum()*100 / data.shape[0],2))},ignore_index=True) 
    if len(df_missing_rate)<1:
        print('no missing value')
    else:
        print('MISSING VALUES')
        print(df_missing_rate)
    print('=========================')
    # pd.set_option('max_rows',200)
    print('TYPE\n{}'.format(data.dtypes))


def obj_size_fmt(num):
    if num<10**3:
        return "{:.2f}{}".format(num,"B")
    elif ((num>=10**3)&(num<10**6)):
        return "{:.2f}{}".format(num/(1.024*10**3),"KB")
    elif ((num>=10**6)&(num<10**9)):
        return "{:.2f}{}".format(num/(1.024*10**6),"MB")
    else:
        return "{:.2f}{}".format(num/(1.024*10**9),"GB")


def memory_usage():
    memory_usage_by_variable=pd.DataFrame({k:sys.getsizeof(v)\
    for (k,v) in globals().items()},index=['Size'])
    memory_usage_by_variable=memory_usage_by_variable.T
    memory_usage_by_variable=memory_usage_by_variable.sort_values(by='Size',ascending=False).head(10)
    memory_usage_by_variable['Size']=memory_usage_by_variable['Size'].apply(lambda x: obj_size_fmt(x))
    return memory_usage_by_variable

def binary_label_summary(df, col_target):
    # Count occurrences of each label
    label_counts = df[col_target].value_counts()
    
    # Calculate overall ratio
    total_samples = label_counts.sum()
    ratio_churn = label_counts[1] / total_samples
    ratio_non_churn = label_counts[0] / total_samples
    
    # Create summary table
    summary_table = pd.DataFrame({
        "Label": ["Churn", "Non-Churn"],
        "Count": [label_counts[1], label_counts[0]],
        "Ratio": [ratio_churn, ratio_non_churn]
    })
    
    return summary_table

def timer(start_time=None):
    if not start_time:
        start_time = datetime.now()
        return start_time
    elif start_time:
        thour, temp_sec = divmod((datetime.now() - start_time).total_seconds(), 3600)
        tmin, tsec = divmod(temp_sec, 60)
        print('\n Time taken: %i hours %i minutes and %s seconds.' % (thour, tmin, round(tsec, 2)))

