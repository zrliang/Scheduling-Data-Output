import plotly.express as px
import pandas as pd
import datetime
import time 
import os


result_df = pd.read_csv("result.csv")
ini_time_stamp = 1618619400 

# 1. Group by sets to find the entity order to plot

def plot_gantt_by_sets(result_df):

    def createFolder(directory):
        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError:
            print ('Error: Creating directory. ' +  directory) 

    plot_num =50 #each time plot 50 entities
    sets= result_df.groupby(['part_no','part_id','entity']) # group by sets
    entities= result_df.groupby('entity') # group by entity
    sets_key_list= list(sets.groups.keys()) 
    entity_key_list= list(entities.groups.keys())
    color_discrete_sequence_list=['#F0F8FF','#7FFFD4','#F0FFFF','#F5F5DC','#FFE4C4',
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
    onetime_plot_entity_order=[] # Record each time entity order to plot to update yaxes
    plt_count=0 # record the times current plot
    plt_name_count=0 #output file name need
    plot_entity_order = []   #record entity from first set to the last (not repeating).
    for i in range(len(sets_key_list)):
        if sets_key_list[i][2] not in plot_entity_order:
            plot_entity_order.append(sets_key_list[i][2])

    foldername = './plot_by_sets' + '/' # folder which store result picture
    createFolder(foldername)

    # plot
    for i in range(len(plot_entity_order)): 
        key= entity_key_list[entity_key_list.index(plot_entity_order[i])]  # find index of machine_order on entity_key_list (ex:BB568 >137)
    #   key= machine_order[i]  #test
        onetime_plot_entity_order.append(key)
        plt_count+=1
    
        for j in range(len(entities.groups[key])):
            row_index=entities.groups[key][j]

            Lot_id =result_df.iloc[row_index].at['lot_number'].strip() # remove space
            Entity =result_df.iloc[row_index].at['entity'].strip()
            start_ts =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ini_time_stamp+result_df.iloc[row_index].at['start_time']*60)) 
            end_ts =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ini_time_stamp+result_df.iloc[row_index].at['end_time']*60)) 
            Sets = result_df.iloc[row_index].at['part_no'].strip()+ "/"+ result_df.iloc[row_index].at['part_id'].strip()
            Bd_id= result_df.iloc[row_index].at['bd_id'].strip()
            df.append(dict(Lot_ID= Lot_id, Start=start_ts,Finish=end_ts,Entity=Entity,Sets=Sets,Bd_Id=Bd_id))

        if(plt_count==plot_num) or i==len(plot_entity_order)-1: # consider final part
        #show figure
            plt_name_count+=1
            fig = px.timeline(df, x_start="Start", x_end="Finish", y="Entity", color="Sets",color_discrete_sequence=color_discrete_sequence_list,text="Lot_ID",hover_name="Bd_Id")   # hover_name
            fig.update_yaxes(categoryorder="array", categoryarray= onetime_plot_entity_order) # plot by onetime_plot_entity_order
            fig.update_traces(textposition='inside',marker_line_color='rgb(8,48,107)')
            fig.write_html(foldername + str(plt_name_count) +".html")
            # reset
            df=[]
            plt_count=0
            onetime_plot_entity_order=[]

    pass

plot_gantt_by_sets(result_df)