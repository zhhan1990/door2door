# -*- coding: utf-8 -*-
"""
计算所有可能的直达和换乘线路
Create all possible direct and transfer trips

@author: zhhan
"""

import os
import pandas
import numpy
import pp



"""
计算一条线路（a trip）的所有时间
Calculate the travel times of a trip.

df: the dataframe of the trip
"""
def gettrips(df):
    df = pandas.DataFrame(df,columns = ['trip_id','arrival_time', 'departure_time', 'stop_id',\
            'stop_sequence', 'arrival_time2', 'departure_time2'])
    count = df.shape[0]
    resultdf = None


    df2 = pandas.DataFrame(numpy.repeat(df.values, count, axis=0))
    df2.columns = df.columns
        
                
    df3 = df2.copy()
    df3['index2'] = df3.index % count        
    df3 = df3.sort_values(by=['index2','stop_sequence'])
    df3.reset_index(drop=True, inplace=True)

    df2.rename(columns={'trip_id':'trip_id_1', 
                        'arrival_time':'arrival_time_1',
                        'departure_time':'departure_time_1',
                        'stop_id': 'stop_id_1',
                        'stop_sequence':'stop_sequence_1',
                        'arrival_time2':'arrival_time2_1',
                        'departure_time2':'departure_time2_1'}, 
                        inplace = True)


    df3.rename(columns={'trip_id':'trip_id_2', 
                        'arrival_time':'arrival_time_2',
                        'departure_time':'departure_time_2',
                        'stop_id': 'stop_id_2',
                        'stop_sequence':'stop_sequence_2',
                        'arrival_time2':'arrival_time2_2',
                        'departure_time2':'departure_time2_2'}, 
                        inplace = True)


    resultdf = pandas.concat([df2,df3],axis=1,ignore_index=False)
    resultdf = resultdf[resultdf['stop_sequence_1'] < resultdf['stop_sequence_2']] 
    resultdf.drop(['index2'], axis=1, inplace=True)
    return resultdf



"""
重新建立换乘线路的时刻表
Rebuild the stop_time of the transfer trip

df: the dataframe of the transfer trip
"""
def rebuildtrip(df):
    i = 1
    predeptime = "00:00:00"
    days = 0
    for index,row in df.iterrows():
        arrival_time2 = row['arrival_time2']
        departure_time2 = row['departure_time2']        

        
        # 如果前一个节点的出发时间大于当前节点的到达时间，则天数+1
        if compareTime(predeptime, arrival_time2) == 2:
            days = days + 1  # 累计时间＋1天        
        arrival_time = addDays(arrival_time2, days) 
        df.loc[index, 'arrival_time'] = arrival_time 
        
        
        # 如果当前节点的到达时间大于出发时间，则天数+1
        if compareTime(arrival_time2, departure_time2) == 2:
            days = days + 1  # 累计时间＋1天
        departure_time = addDays(departure_time2, days) 
        df.loc[index, 'departure_time'] = departure_time 
        predeptime = departure_time2 

        df.loc[index, 'stop_sequence'] = i 
        i = i + 1
        
    
    return df



"""
计算直达线路的旅行时间
Calculate the travel times of a direct trip.

stoptimesdf: the dataframe from the stop_times.csv
tripdf: the dataframe from the trips.csv
outdir: the folder of the results
"""
def directtrips(stoptimesdf, tripdf, outdir):
    for index,row in tripdf.iterrows():
        trip_id =row["trip_id"]   
        df = stoptimesdf[stoptimesdf["trip_id"] == trip_id]
        resultdf = gettrips(df)
        outpath = outdir + '\\' + trip_id + '.csv'
        resultdf.to_csv(outpath, index = False, encoding ='gb18030')       
   


"""
并行计算直达线路
Parallel operation for direct trips

tripspath: the file path of the trips.csv
stoptimespath: the file path of the stop_times.csv
kernels: the number of parallel kernel 
outdir: the folder of the results  
"""
def paralleljobs4direct(tripspath, stoptimespath, kernels, outdir):
    ppservers = ()
    job_server = pp.Server(kernels, ppservers=ppservers)
    jobs = []
    
    stoptimesdf = pandas.read_csv(stoptimespath, encoding ='gb18030')
    tripsdf = pandas.read_csv(tripspath, encoding ='gb18030')
    n = int(tripsdf.shape[0] / kernels) + 1 #每一个核分配的任务数
    for i in range(0, kernels):        
        subtripsdf = tripsdf[ i*n : (i+1)*n]        
        jobs.append(job_server.submit(directtrips, \
                                     (stoptimesdf, subtripsdf, outdir,), \
                                     (gettrips,), ("pandas","os","numpy",)))

    for job in jobs:
        job()



"""
计算换乘线路的旅行时间
Calculate the travel times of a transfer trip.

stoptimesdf: the dataframe from the stop_times.csv
transfertripsdf: the dataframe from the transfer trips
outdir: the folder of the results
transfer_stop: the stop of the transfer trips
"""
def transfertrips(stoptimesdf, transfertripsdf, outdir, transfer_stop):   
    
    pre_from_trip_id = ""
    resultdf = None
    
    for index,row in transfertripsdf.iterrows():           
        
        from_trip_id = row["trip_id_1"]
        from_stop_id = row["stop_id_1"]
        from_stop_sequence = row["stop_sequence_1"]
        arrival_time = row["arrival_time_1"]
        arrival_time2 = row["arrival_time2_1"]

        to_trip_id = row["trip_id_2"]
        to_stop_id = row["stop_id_2"]
        to_stop_sequence = row["stop_sequence_2"]        
        departure_time = row["departure_time_2"]
        departure_time2 = row["departure_time2_2"]   
        
        
        fromdf = stoptimesdf[stoptimesdf['trip_id'] == from_trip_id]
        fromdf = fromdf[fromdf['stop_sequence'] < from_stop_sequence]
        

        row2 = pandas.DataFrame({'trip_id': from_trip_id + '_' + to_trip_id, #该字段不会重复
                                 'arrival_time': arrival_time,
                                 'departure_time': departure_time,
                                 'stop_id': from_stop_id + '_' + to_stop_id,
                                 'stop_sequence': 0,
                                 'stop_headsign': "",
                                 'pickup_type': 0,
                                 'drop_off_type': 0,
                                 'shape_dist_traveled': "",
                                 'arrival_time2': arrival_time2,
                                 'departure_time2': departure_time2},
                                 index=[1]) 
        fromdf = fromdf.append(row2, ignore_index=True)       
        
        
        todf = stoptimesdf[stoptimesdf['trip_id'] == to_trip_id]
        todf = todf[todf['stop_sequence'] > to_stop_sequence]
        fromdf = fromdf.append(todf, ignore_index=True)
        
        df = fromdf.copy()
        df = rebuildtrip(df)
        
        if pre_from_trip_id == "":                  
            resultdf = gettrips(df) #第一个结果
        else:
            resultdf2 = gettrips(df)
            if pre_from_trip_id == from_trip_id:
                resultdf = resultdf.append(resultdf2)
            else:
                outpath = outdir + '\\' + pre_from_trip_id  + '.csv' #以换乘起始点分割结果
                if os.path.exists(outpath) and os.path.getsize(outpath) > 0:
                    continue  #如果有结果，且结果不为空，则跳过，中间可能遇到错误信息
                resultdf['transfer_stop'] = transfer_stop
                resultdf = resultdf[(resultdf['trip_id_1'] != resultdf['trip_id_2']) & \
                                    (resultdf['stop_id_1'] != transfer_stop) & \
                                    (resultdf['stop_id_2'] != transfer_stop)] # 同一trip不存在换乘，换乘点不错作为起始和终止站点
                resultdf.to_csv(outpath, index = False, encoding ='gb18030')
                resultdf = resultdf2
                
        pre_from_trip_id = from_trip_id         

    
    # 最后的结果
    outpath = outdir + '\\' + pre_from_trip_id  + '.csv' #以换乘起始点分割结果
    if os.path.exists(outpath) and os.path.getsize(outpath) > 0:
        return
    else:
        resultdf['transfer_stop'] = transfer_stop
        resultdf = resultdf[(resultdf['trip_id_1'] != resultdf['trip_id_2']) & \
                            (resultdf['stop_id_1'] != transfer_stop) & \
                            (resultdf['stop_id_2'] != transfer_stop)] # 同一trip不存在换乘，换乘点不错作为起始和终止站点        
        resultdf.to_csv(outpath, index = False, encoding ='gb18030')  #如果有结果，且结果不为空，则跳过，中间可能遇到错误信息
    
    
    
"""
并行计算直达线路
Parallel operation for transfer trips

transfertripsdir: the folder of the transfer trips
stoptimespath: the dataframe from the stop_times.csv
kernels: the number of parallel kernel 
outdir: the folder of the results
"""
def paralleljobs4transfer(transfertripsdir, stoptimespath, kernels, outdir, filename):
    ppservers = ()
    job_server = pp.Server(kernels, ppservers=ppservers)
    jobs = []
    
    stoptimesdf = pandas.read_csv(stoptimespath, encoding ='gb18030')
    
    filelist = os.listdir(transfertripsdir)
    for file in filelist:
        
        if filename != "" and file.split('.')[0] != filename:
            continue #针对特定的换乘，进行修补
        
        transfertripspath =  transfertripsdir + '\\' + file
        transfertripsdf = pandas.read_csv(transfertripspath, encoding ='gb18030')   
        outdir2 = outdir + '\\' + file.split('.')[0]
        if not os.path.exists(outdir2):
            os.makedirs(outdir2)
        transfer_stop = file.split('.')[0]
        jobs.append(job_server.submit(transfertrips, \
                                      (stoptimesdf, transfertripsdf, outdir2, transfer_stop), \
                                      (gettrips, rebuildtrip, compareTime, addDays), ("pandas","os","numpy",)))
        
    for job in jobs:
        job()


    
"""
比较两个时间
Compare two times
"""
def compareTime(time1, time2):
    hour1 = int(time1.split(":")[0])
    hour2 = int(time2.split(":")[0])
    if hour1 < hour2:
        return 1
    elif hour1 > hour2:
        return 2
    else:
        return 0



"""
时间增加天数
If the time (i.e.,00:05:00) is passed the 24:00:00, the new time is 24:05:00.
"""    
def addDays(time1, days):
    hour1 = int(time1.split(":")[0])
    hour1 = hour1 + 24 * days
    return str(hour1) + ":" + time1.split(":")[1] + ":" + time1.split(":")[2]
   
    
    
    
    