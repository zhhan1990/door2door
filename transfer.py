# -*- coding: utf-8 -*-
"""
Create transfer trips

@author: zhhan
"""

import pandas 
import numpy
import pp



"""
创建换乘表
这个表格记录了站点的换乘信息
样例参见：the LSMT-China/GSFT-dataset
这里我们只考虑站内的换乘
站内最短换乘时间：火车站 30 分钟， 机场 60 分钟

Establish the transfer.csv.
This file documents the transfer information from one stop to another one.
The sample can be found in the LSMT-China/GSFT-dataset.
Here, we caclulated the intra-transfer infomation. 
The minimum transfer time (min_transfer_time) of stations and airports is 30 and 60 minutes respectively.

stopspath: the stop.csv in the LSMT-China/GSFT-dataset
transferpath: the transfer.csv in the LSMT-China/GSFT-dataset
transfer_type: intra-transfer —— 1, inter-transfer —— 2
"""
def intratransfer(stopspath, transferpath):
    stopsdf = pandas.read_csv(stopspath, encoding ='gb18030')
    transferdf = pandas.read_csv(transferpath, encoding ='gb18030')
    stopsdf = stopsdf.drop_duplicates(subset=['stop_id'],keep='first',inplace=False)
      
    for index,row in stopsdf.iterrows():    
        stopid = row['stop_id']
        stopcode = row['stop_code']
        
        if 'R' in stopcode: # Stations
            mintime = 30
        elif 'A' in stopcode: # Airports
            mintime = 60
            
        row2 = pandas.DataFrame({'from_stop_id': stopid,
                                  'to_stop_id': stopid,
                                  'transfer_type': 1,
                                  'min_transfer_time': mintime,
                                  'transfer_id': "TS"+str(index+1)},
                                  index=[1]) 
        transferdf = transferdf.append(row2, ignore_index=True)           


    transferdf.to_csv(transferpath, encoding ='gb18030', index = False)    



"""
根据 transfer.csv, 查找可换乘线路
规则:
    1) 满足可换乘时间 >= min_transfer_time
    2) 出发线路的起点不换乘，换乘线路的重点不换乘
    3) 到达时间和出发时间应为同一天内

Find the transfer trip
Rules:
    1) The transfer time >= min_transfer_time
    2) The results exclude the departure stop of the from-trip and the arrival stop of the to-trip.
    3) We focused on the transfer that happened on the same day because not all the stations can offer overnight services.

transferdf: from the transfer.csv
stoptimesdf: from the stop_times.csv
transfer_id: from the transfer.csv
outpath: transfer trip.
"""
def transfertrip(transferdf, stoptimesdf, transfer_id, outpath):
    
    subdf = transferdf[transferdf['transfer_id'] == transfer_id]
    from_id = subdf['from_stop_id'].values[0]
    to_id = subdf['to_stop_id'].values[0]
    min_transfer_time = subdf['min_transfer_time'].values[0]
    
    fromdf = stoptimesdf[stoptimesdf['stop_id'] == from_id]
    fromdf = fromdf[fromdf['stop_sequence'] != 1] # fromtrip的起点不换乘(Exclude the departure stop of the from-trip)
    
    todf = stoptimesdf[stoptimesdf['stop_id'] == to_id]
    todf = todf[(todf['stop_sequence'] == 1) | (todf['arrival_time2'] != todf['departure_time2'])] # toship的终点不换乘(Exclude the arrival stop of the to-trip)
    
    fromdf = pandas.DataFrame(fromdf,columns = ['trip_id','arrival_time', 'departure_time', 'stop_id',\
            'stop_sequence', 'arrival_time2', 'departure_time2'])        
    todf = pandas.DataFrame(todf,columns = ['trip_id','arrival_time', 'departure_time', 'stop_id',\
            'stop_sequence', 'arrival_time2', 'departure_time2'])        
        
    fromcount = fromdf.shape[0]
    tocount = todf.shape[0]
    resultdf = None
    

    df2 = pandas.DataFrame(numpy.repeat(fromdf.values, tocount, axis=0))
    df2.columns = fromdf.columns
        
    todf['index3'] = todf.index 
    df3 = pandas.DataFrame(numpy.repeat(todf.values, fromcount, axis=0))
    df3.columns = todf.columns
    df3['index2'] = df3.index % tocount        
    df3 = df3.sort_values(by=['index2', 'index3'])
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
    
    # 换乘须在同一天内(The transfer that happened on the same day)
    resultdf['arr1'] = resultdf['arrival_time2_1'].str.split(':')
    resultdf['arr2'] = resultdf['arr1'].apply(lambda x: None if x[0] == None else int(x[0])*60 + int(x[1]))
    resultdf['dep1'] = resultdf['departure_time2_2'].str.split(':')
    resultdf['dep2'] = resultdf['dep1'].apply(lambda x: None if x[0] == None else int(x[0])*60 + int(x[1]))
    resultdf['dep_arr'] = resultdf['dep2'] - resultdf['arr2']
    resultdf = resultdf[resultdf['trip_id_1'] != resultdf['trip_id_2']] 
    resultdf = resultdf[resultdf['dep_arr'] >= min_transfer_time] 
    resultdf.drop(['index2', 'index3', 'arr1', 'arr2', 'dep1', 'dep2', 'dep_arr'], axis=1, inplace=True)
    
    if resultdf.shape[0] > 0: #没有换乘，不输出
        resultdf.to_csv(outpath, encoding ='gb18030', index = False)    



"""
根据 transfer.csv, 建立换乘线路
Establish the transfer trips from transfer.csv

transferdf: from the transfer.csv
stoptimesdf: from the stop_times.csv
outdir: the folder of the results
"""
def transfertrips(transferdf, stoptimesdf, outdir):
    for index,row in transferdf.iterrows():
        transfer_id = row["transfer_id"]   
        from_stop_id = row["from_stop_id"]
        to_stop_id = row["to_stop_id"]
        outpath = outdir + '\\' + from_stop_id + '_' + to_stop_id + '.csv'
        transfertrip(transferdf, stoptimesdf, transfer_id, outpath)



"""
并行计算
Parallel operation

transferdf: from the transfer.csv
stoptimesdf: from the stop_times.csv
kernels: the number of parallel kernel 
outdir: the folder of the results
"""
def paralleljobs4transfertrips(transferpath, stoptimespath, kernels, outdir):
    ppservers = ()
    job_server = pp.Server(kernels, ppservers=ppservers)
    jobs = []
    
    transferdf = pandas.read_csv(transferpath, encoding ='gb18030')
    stoptimesdf = pandas.read_csv(stoptimespath, encoding ='gb18030')
    
   
    n = int(transferdf.shape[0] / kernels) + 1 #每一个核分配的任务数
    for i in range(0, kernels):
        subtransferdf = transferdf[ i*n : (i+1)*n]        
        jobs.append(job_server.submit(transfertrips, \
                                     (subtransferdf, stoptimesdf, outdir,), \
                                     (transfertrip,), ("pandas","os","numpy",)))

    for job in jobs:
        job()




