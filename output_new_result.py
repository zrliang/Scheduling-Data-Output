import csv
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os 

# 1. Convert relative time to absulute time(date) (start time and end time)
# 2. Change the column order

result_df = pd.read_csv("result.csv")
ini_time_stamp = 1618619400 ## ini time \改


def output_new_result(result_df):

    new_result_df = result_df.copy(deep=True)
    for i in range(len(result_df.index)):
        new_result_df['start_time'].iloc[i] =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ini_time_stamp+result_df['start_time'].iloc[i]*60))
        new_result_df['end_time'].iloc[i] =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ini_time_stamp+result_df['end_time'].iloc[i]*60)) 
    new_result_df= new_result_df[['lot_number','cust','pin_pkg','prod_id','qty','part_id','part_no','bd_id','entity','start_time','end_time']]

    # write file 
    new_result_df.to_csv("new_result.csv", index=False ,na_rep=0)

    pass

output_new_result(result_df)


