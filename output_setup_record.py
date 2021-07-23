# output setup_record.csv file
import csv
import pandas as pd
import time

result_df = pd.read_csv("result.csv")
ini_time_stamp = 1618619400 ## ini time

# 1. Sort the result.csv by entity & start_time
# 2. Record the setup of each entity by judging whether the bd_id is same or not (for i in range..)
# 3. The method of geting setuptime is recordimg previous endT and current startT(need to change to date)
# 4. The format of ouyput is using dictionary to record each entity(key) and its setup information(value) p.s. maybe 0 or more values of each entity record.
# 5. Finally, convert dict to csv. 

def output_setup_record(result_df):

    sorted_result_df=result_df.sort_values(by=['entity', 'start_time'])
    setup_dict ={}
    times_info_list=[]
    entity_name= sorted_result_df['entity'].iloc[0]
    bd_id = sorted_result_df['bd_id'].iloc[0]

    for i in range(len(sorted_result_df.index)): # serach for all data
        if entity_name == sorted_result_df['entity'].iloc[i]:  # If entity is same, need to judge whether bd_id is same. Else, record new entity(key)

            if bd_id != sorted_result_df['bd_id'].iloc[i]: # if bd_id is different need to record
                setup_start=time.strftime("%m-%d %H:%M:%S", time.localtime(ini_time_stamp+sorted_result_df['end_time'].iloc[i-1]*60))  #the endT of previous and convert to date
                setup_end=time.strftime("%m-%d %H:%M:%S", time.localtime(ini_time_stamp+sorted_result_df['start_time'].iloc[i]*60))   #the startT of current and convert to date
                # change to date
                times_info_list.append(setup_start+' > '+setup_end) 
                bd_id = sorted_result_df['bd_id'].iloc[i]

        else: 
            #add to dict
            setup_dict[entity_name]=times_info_list
            #next entitys
            entity_name = sorted_result_df['entity'].iloc[i]
            bd_id = sorted_result_df['bd_id'].iloc[i]
            times_info_list=[]

        #final one
        if i ==len(sorted_result_df.index)-1: 
            setup_dict[entity_name]=times_info_list

    # write file
    with open("setup_record.csv", "w", newline="") as f_output:
        csv_output = csv.writer(f_output)
        for key in sorted(setup_dict.keys()):
            csv_output.writerow([key] + setup_dict[key])

    pass

output_setup_record(result_df)