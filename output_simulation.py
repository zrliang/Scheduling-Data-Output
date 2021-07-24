import csv
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os 

result_df = pd.read_csv("result.csv")
machine_area_df = pd.read_excel("機台區域作業產品.xls" )[['Entity','Location']]
ini_time_stamp = 1618619400 ## ini time \改

#1. Need to find the amount of entity(ENT) which use same sets('cust','pin_pkg','prod_id','bd_id','Location') on different moment and total die quantity(outplan).
#2. Detail can be found on each function
def output_simulation(result_df,machine_area_df): 

    #  Get the filetered dataframe by judging whether proceesing through specific time
    def get_result_byFilter(merge_result,relative_time):
        filter_strt = merge_result["start_time"] <= relative_time  # retuen true or false of each record on dataframe
        filter_end = merge_result["end_time"] >= relative_time
        filter_result = merge_result[filter_strt & filter_end]     # if satisfying both condition, it will get this record(through all)  

        return filter_result

    # Get entity amount(dataframe) on specific time
    # input: filetered dataframe & new column name
    # use two times group by to get amount
    def get_ENT_df(merge_result,ent_colname): 

        temp_group= merge_result.groupby(['cust','pin_pkg','prod_id','bd_id','Location','entity']) 
        df_grp_entity = pd.DataFrame(temp_group.groups.keys())
        grp_basis = df_grp_entity.groupby([0,1,2,3,4])  # cust > Location
        df_keys= pd.DataFrame(grp_basis.groups.keys())
        df_Ent_amount= pd.DataFrame(list(grp_basis.size()))
        ENTdf= pd.concat([df_keys, df_Ent_amount], axis=1)
        ENTdf.columns=['cust','pin_pkg','prod_id','bd_id','Location',ent_colname]

        return ENTdf
    
    #Use group by to get the amount of quantity of each sets and make it to dataframe(list>df)
    #Find the lots using same group by set and accumulate their die quantity
    def get_qty_df(original_group,merge_result):

        qty_list=[]
        group_key_list= list(original_group.groups.keys())
        
        for i in range(len(group_key_list)): 
            key= group_key_list[i]
            qty_sum=0
            for j in range(len(original_group.groups[key])):
                row_index=original_group.groups[key][j]
                qty_sum+=merge_result.iloc[row_index].at['qty']
            qty_list.append(qty_sum)

        qty_df = pd.DataFrame(qty_list,columns=['Output plan'])

        return qty_df

    # initialize
    merge_result = pd.merge(result_df, machine_area_df, left_on ='entity', right_on ='Entity', how='left')
    original_group=merge_result.groupby(['cust','pin_pkg','prod_id','bd_id','Location']) 

    ENT_all = get_ENT_df(merge_result,'Allocate ENT')
    ENT_1 = get_ENT_df(get_result_byFilter(merge_result,0),'Original ENT(07:30)')      #relative to 07:30(standard time) is 0 minute
    ENT_2 = get_ENT_df(get_result_byFilter(merge_result,210),'Original ENT(11:00)')    #relative to 11:00 is need to plus 210 minute
    qty_df = get_qty_df(original_group,merge_result)

    # merge and concat all data to one dataframe
    merge_df1 = pd.merge(ENT_all, ENT_1, on =(['cust','pin_pkg','prod_id','bd_id','Location']), how ='left')
    merge_df2 = pd.merge(merge_df1, ENT_2, on =(['cust','pin_pkg','prod_id','bd_id','Location']), how ='left')
    temp_df= pd.concat([merge_df2, qty_df], axis=1)
    ## change column sequence to final version
    simulation_output_df = temp_df[['cust','pin_pkg','prod_id','bd_id','Location','Original ENT(07:30)','Original ENT(11:00)','Allocate ENT','Output plan']]

    # write to csv
    simulation_output_df.to_csv("simulation_output.csv", index=False ,na_rep=0) 

    pass


output_simulation(result_df,machine_area_df)