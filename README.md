# Scheduling-Data-Output-

## setup_record.ipynb

1. Sort the result.csv by entity & start_time
2. Record the setup of each entity by judging whether the bd_id is same or not (for i in range..)
3. The method of geting setuptime is recordimg previous endT and current startT(need to change to date)
4. The format of ouyput is using dictionary to record each entity(key) and its setup information(value) p.s. maybe 0 or more values of each entity record.
5. Finally, convert dict to csv. 

##
