# -*- coding: utf-8 -*-
"""
计算两个站点之间的最短时间
这一步是trips.py的后续
Calculate the shortest travel time between two stops.
This step is followed by the trip.py

@author: zhhan2
"""

import os
import pandas
import pp



"""
计算两个站点之间的最短时间
Calculate the shortest travel time between two stops.

dirpath: the folder of the trips
outpath: the file path of the result
"""
def shortesttime(dirpath, outpath):
    
    df = None  
    for filepath, dirnames, filenames in os.walk(dirpath):
        for filename in filenames:
            filepath1 = filepath + '\\' + filename
            newdf = pandas.read_csv(filepath1, encoding ='gb18030')
            if df is None:                
                df = newdf
            else:
                df = df.append(newdf)
    if 'transfer_stop' not in df.columns:
        df['transfer_stop'] = ""
        
    df['from_to_stops'] = df['stop_id_1'] + '-' + df['stop_id_2']
    df['traveltime'] = df.apply(lambda x: int(x['arrival_time_2'].split(":")[0])*60 + int(x['arrival_time_2'].split(":")[1]) -\
                                int(x['departure_time_1'].split(":")[0])*60 - int(x['departure_time_1'].split(":")[1]), axis=1)
    
    df = df.sort_values(by=['from_to_stops','traveltime'],axis=0,ascending=[True,True]) 
    df.drop_duplicates(subset=['from_to_stops'],keep='first',inplace=True)    
    df.to_csv(outpath, encoding='gb18030', index = False)



"""
并行计算
Parallel operation

dirpath: the file path of the trips
outdir: the folder of the results 
kernels: the number of parallel kernel 
"""
def paralleljobs(dirpath, outdir, kernels):
    ppservers = ()
    job_server = pp.Server(kernels, ppservers=ppservers)
    jobs = []
    

    for filepath, dirnames, filenames in os.walk(dirpath):
        for dirname in dirnames:
            dirpath1 = dirpath + '\\' + dirname
            outpath = outdir + '\\' + dirname + '.csv'       
            jobs.append(job_server.submit(shortesttime, \
                                         (dirpath1, outpath,), \
                                         (), ("pandas","os","numpy",)))
        
    for job in jobs:
        job()



"""
将直达和换乘合并
Merge the direct and the transfer results

directpath: the shortest travel time of direct trips
transferpath: the shortest travel time of transfer trips
outpath: the file path of results
"""
def merge(directpath, transferpath, outpath):
    directdf = pandas.read_csv(directpath, encoding ='gb18030')
    transferdf = pandas.read_csv(transferpath, encoding ='gb18030')
    
    directdf = directdf[(directdf['stop_id_1']!='Nodata') & (directdf['stop_id_2']!='Nodata')]
    transferdf = transferdf[(transferdf['stop_id_1']!='Nodata') & (transferdf['stop_id_2']!='Nodata')]
    
    
    directdf['type'] = 'direct'
    transferdf['type'] = 'transfer'
    
    df = directdf.append(transferdf)
    df = df.sort_values(by=['from_to_stops','traveltime'],axis=0,ascending=[True,True]) 
    df.drop_duplicates(subset=['from_to_stops'],keep='first',inplace=True)    
    df.to_csv(outpath, encoding='gb18030', index = False)    
    
    
    

