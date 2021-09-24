import csv
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime
import time
import os 


def output_gantt_chart(no_NA_df):

    def createFolder(directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print ('Error: Creating directory. ' +  directory) 

    # initizlize
    plot_num = 50 #each time plot 50 entities
    entities= no_NA_df.groupby("entity") # group by entity
    entity_key_list= list(entities.groups.keys()) # key list
    color_discrete_sequence_list=['#F0F8FF','#7FFFD4','#F0FFFF','#F5F5DC','#FFE4C4',    # add color cycle
                '#000000','#0000FF','#8A2BE2','#A52A2A','#DEB887','#5F9EA0',
                '#7FFF00','#D2691E','#FF7F50','#6495ED','#FFF8DC','#DC143C','#00FFFF',
                '#00008B','#008B8B','#B8860B','#A9A9A9','#006400','#BDB76B','#8B008B',
                '#556B2F','#FF8C00','#9932CC','#8B0000','#E9967A','#8FBC8F','#483D8B',
                '#2F4F4F','#00CED1','#9400D3','#FF1493','#00BFFF','#696969','#1E90FF',
                '#B22222','#FFFAF0','#228B22','#FF00FF','#DCDCDC','#F8F8FF','#FFD700',
                '#DAA520','#808080','#008000','#ADFF2F','#F0FFF0','#FF69B4','#CD5C5C',
                '#4B0082','#FFFFF0','#F0E68C','#E6E6FA','#FFF0F5','#7CFC00','#FFFACD',
                '#ADD8E6','#F08080','#E0FFFF','#FAFAD2','#90EE90','#D3D3D3','#FFB6C1',
                '#FFA07A','#20B2AA','#87CEFA','#778899','#B0C4DE','#FFFFE0','#00FF00',
                '#32CD32','#FAF0E6','#FF00FF','#800000','#66CDAA','#0000CD','#BA55D3',
                '#9370DB','#3CB371','#7B68EE','#00FA9A','#48D1CC','#C71585','#191970',
                '#F5FFFA','#FFE4E1','#FFE4B5','#FFDEAD','#000080','#FDF5E6','#808000',
                '#6B8E23','#FFA500','#FF4500','#DA70D6','#EEE8AA','#98FB98','#AFEEEE',
                '#DB7093','#FFEFD5','#FFDAB9','#CD853F','#FFC0CB','#DDA0DD','#B0E0E6',
                '#800080','#FF0000','#BC8F8F','#4169E1','#8B4513','#FA8072','#FAA460',
                '#2E8B57','#FFF5EE','#A0522D','#C0C0C0','#87CEEB','#6A5ACD','#708090',
                '#FFFAFA','#00FF7F','#4682B4','#D2B48C','#008080','#D8BFD8','#FF6347',
                '#40E0D0','#EE82EE','#F5DEB3','#FFFFFF','#F5F5F5','#FFFF00','#9ACD32']  
    df=[] #plot need dataframe
    plt_count=0 # record the times current plot
    foldername = './gantt_chart' + '/' # folder which store result picture
    createFolder(foldername)  

    #plot
    for i in range(len(entity_key_list)): 
        key= entity_key_list[i]
        plt_count+=1
        for j in range(len(entities.groups[key])):
            row_index=entities.groups[key][j]
            Lot_id =no_NA_df.iloc[row_index].at['lot_number'] 
            Entity =no_NA_df.iloc[row_index].at['entity']
            start_ts =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ini_time_stamp+no_NA_df.iloc[row_index].at['start_time']*60)) 
            end_ts =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ini_time_stamp+no_NA_df.iloc[row_index].at['end_time']*60)) 
            Sets = str(no_NA_df.iloc[row_index].at['part_no'])+ "/"+ str(no_NA_df.iloc[row_index].at['part_id'])
            Bd_id= no_NA_df.iloc[row_index].at['bd_id']
            # create plot needed dateframe
            df.append(dict(Lot_ID= Lot_id, Start=start_ts,Finish=end_ts,Entity=Entity,Sets=Sets,Bd_Id=Bd_id))
        if(plt_count==plot_num) or i==len(entity_key_list)-1: # consider final part
        #show figure
            fig = px.timeline(df, x_start="Start", x_end="Finish", y="Entity", color="Bd_Id",color_discrete_sequence=color_discrete_sequence_list,text="Lot_ID",hover_name="Sets")  
            fig.update_yaxes(categoryorder="category descending") # sort by
            fig.update_traces(textposition='inside',marker_line_color='rgb(8,48,107)')
            fig.write_html(foldername +df[0]['Entity']+"_" +df[-1]['Entity']+".html")
            # reset
            df=[]
            plt_count=0

    pass

def output_simulation(no_NA_df,machine_area_df): 

    #  Get the filetered dataframe by judging whether proceesing through specific time
    def get_result_byFilter(merge_result,relative_time):
        filter_strt = merge_result["start_time"] <= relative_time  # retuen true or false of each record on dataframe
        filter_end = merge_result["end_time"] >= relative_time
        filter_result = merge_result[filter_strt & filter_end]     # if satisfying both condition, it will get this record(through all)  

        return filter_result

    # Get new dataframe by groupby sets
    # input: filetered or processed dataframe & new column name
    # output: new df
    # method: use two times group by to get amount
    def get_ENT_df(processed_data,ent_colname): 

        temp_group= processed_data.groupby(['cust','pin_pkg','prod_id','bd_id','oper','Location','entity']) # to know entity amount
        df_grp_entity = pd.DataFrame(temp_group.groups.keys())
        grp_basis = df_grp_entity.groupby([0,1,2,3,4,5])  # from cust to location(index=0~5) #second groupby to build new dataframe format
        df_keys= pd.DataFrame(grp_basis.groups.keys())    # key
        df_Ent_amount= pd.DataFrame(list(grp_basis.size())) # value
        ENTdf= pd.concat([df_keys, df_Ent_amount], axis=1)  # merge
        ENTdf.columns=['cust','pin_pkg','prod_id','bd_id','oper','Location',ent_colname]  

        return ENTdf
    
    #Use group by to get the quantities of productivity of each sets and make it to dataframe(list>df)
    #input: merge_result
    #output: outplan df
    #method: find the lots(index) using same set and accumulate their die quantity
    def get_qty_df(oneday_result):

        qty_list=[]
        original_group=oneday_result.groupby(['cust','pin_pkg','prod_id','bd_id','oper','Location']) 
        group_key_list= list(original_group.groups.keys())
        
        for i in range(len(group_key_list)): 
            key= group_key_list[i]
            qty_sum=0
            for j in range(len(original_group.groups[key])):
                row_index=original_group.groups[key][j]
                qty_sum+=oneday_result.iloc[row_index].at['qty']
            qty_list.append(qty_sum)
        
        # make new df
        df_keys= pd.DataFrame(original_group.groups.keys())
        qty_df = pd.DataFrame(qty_list)
        outplan_df= pd.concat([df_keys, qty_df], axis=1)
        outplan_df.columns=['cust','pin_pkg','prod_id','bd_id','oper','Location','output plan'] 

        return outplan_df


    # # initialize
    merge_result = pd.merge(no_NA_df, machine_area_df, left_on ='entity', right_on ='Entity', how='left') 
    oneday_result = pd.merge(no_NA_df[no_NA_df["end_time"]<= 1440], machine_area_df, left_on ='entity', right_on ='Entity', how='left') # one day
    result_1650 = pd.merge(no_NA_df[no_NA_df["start_time"]<= 1650], machine_area_df, left_on ='entity', right_on ='Entity', how='left') # to next day 11.(27.5hrs)

    ## produce major frame(all time data) to let other dataframe to merge 
    grp_basis = merge_result.groupby(['cust','pin_pkg','prod_id','bd_id','oper','Location']) 
    df_frame= pd.DataFrame(grp_basis.groups.keys())
    df_frame.columns=['cust','pin_pkg','prod_id','bd_id','oper','Location']  

    # produce df_1650 by get the lot processing on 1650 or the final lot of each entity
    entities=result_1650.groupby("entity") 
    entity_key_list= list(entities.groups.keys())
    entity_final_jobs=[]
    for i in range(len(entity_key_list)):
        entity_final_jobs.append(entities.groups[entity_key_list[i]][-1])
    df_1650 = result_1650.iloc[entity_final_jobs]

    # produce each dataframe to be merge
    ENT_1 = get_ENT_df(get_result_byFilter(merge_result,0),'original ENT(+0)')      #relative to 07:30(standard time) is 0 minute
    ENT_2 = get_ENT_df(get_result_byFilter(merge_result,210),'original ENT(+210)')    #relative to 11:00 is need to plus 210 minute
    ENT_3 = get_ENT_df(df_1650,'allocate ENT')
    outplan_df = get_qty_df(oneday_result)                                          # one day

    # merge and concat all data to one dataframe
    merge_df1 = pd.merge(df_frame, ENT_1, on =(['cust','pin_pkg','prod_id','bd_id','oper','Location']), how ='left')
    merge_df2 = pd.merge(merge_df1, ENT_2, on =(['cust','pin_pkg','prod_id','bd_id','oper','Location']), how ='left')
    merge_df3 = pd.merge(merge_df2, ENT_3, on =(['cust','pin_pkg','prod_id','bd_id','oper','Location']), how ='left')
    simulation_output_df = pd.merge(merge_df3, outplan_df, on =(['cust','pin_pkg','prod_id','bd_id','oper','Location']), how ='left')
    
    # write to csv
    simulation_output_df.to_csv("simulation_output.csv", index=False ,na_rep=0) 
    
    pass


# read file and initialize
result_df = pd.read_csv("result.csv")
machine_area_df = pd.read_csv("./input/locations.csv" )[['Entity','Location']]
machine_area_df.replace('\s+','',regex=True,inplace=True) # remove space
config_df = pd.read_csv("./input/config.csv")
ini_time_stamp = int(time.mktime(time.strptime(config_df.at[0,'std_time'], "%Y/%m/%d %H:%M"))) # get standard time

# Get rid of repeative and NA value
repeative_index = list(result_df[result_df.duplicated("lot_number",keep=False)].index)
delete_index =[]
for i in range(len(repeative_index)):
    if result_df.iloc[repeative_index[i]].at['oper']==0: # remove lot number equals to NA by oper
        delete_index.append(repeative_index[i])
    elif result_df.iloc[repeative_index[i]].at['process_time'] !=0: # keep jobs runnning on machine by process_time
        delete_index.append(repeative_index[i])
        
no_NA_df = result_df.drop(index=delete_index) # remove  
no_NA_df.reset_index(inplace=True, drop=True)  # reset index


# execute
output_gantt_chart(no_NA_df)
output_simulation(no_NA_df,machine_area_df)