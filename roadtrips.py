# -*- coding: utf-8 -*-
"""
The travel time by road transport.

@author: zhhan
"""

import pp
import urllib
from bs4 import BeautifulSoup
import pandas
import os
import time



"""
获取从一个地点到另一个地点的驾车时间。
通过高德调用驾车路径规划，单位为秒。
Request the driving time from one place (longitude1, latitude1) to another one (longitude2, latitude2).
We use the driving route planning in Amap.
The time duration is measured in seconds.

apikey: the key of Amap
lat1, lon1: the location for place A
lat2, lon2: the location for place B  
"""
def amapdriving(apikey, lat1, lon1, lat2, lon2):
    
    url = 'http://restapi.amap.com/v3/direction/driving?origin='+ str(lon1)+','+ str(lat1)+ \
          '&destination='+ str(lon2)+','+ str(lat2)+'&extensions=all&output=xml&key=' + apikey      
    request = urllib.request.Request(url)
    results = urllib.request.urlopen(request)

    soup = BeautifulSoup(results)
    values = soup.find_all('duration')
    if len(values) == 0:
        value = 'Null'
    else:
        value = values[0].getText()
  
    return value



"""
获取驾车时间
Get the travel time by road transport.

apikey: the key of Amap
filepath: this file documents the records from places of placepath1 to places of placepath2
"""
def drivingtime(apikey, filepath, placepath1, field1, placepath2, field2, outpath):
    df = pandas.read_csv(filepath, encoding = 'gb18030')
    
    pdf1 = pandas.read_csv(placepath1, encoding = 'gb18030')
    pdf1.set_index(field1,inplace = True)
    pdf2 = pandas.read_csv(placepath2, encoding = 'gb18030')
    pdf2.set_index(field2,inplace = True)
    
    df_result = pandas.DataFrame(columns=['place_id_x', 'place_id_y', 'duration']) 
    i = 1
    for index,row in df.iterrows():
        
        if i%100 == 0:
            time.sleep(5)
        i += 1
        
        fromid = row["place_id_x"]  
        toid = row["place_id_y"]
        lat1 = pdf1.loc[fromid, 'latitude']
        lon1 = pdf1.loc[fromid, 'longitude']

        lat2 = pdf2.loc[toid, 'latitude']
        lon2 = pdf2.loc[toid, 'longitude']
        
        duration = amapdriving(apikey, lat1, lon1, lat2, lon2)
        
        row2 = pandas.DataFrame({'place_id_x': fromid,
                                 'place_id_y': toid,
                                 'duration': duration},
                                 index=[1]) 
        df_result = df_result.append(row2, ignore_index=True)
        
    
    df_result.to_csv(outpath, index = False)
  

      
"""
并行计算
Parallel operation
"""
def ParallelJobs(apikey, dirpath, placepath1, field1, placepath2, field2, pre, maxmum, outdir):
    ppservers = ()
    job_server = pp.Server(7, ppservers=ppservers)
    jobs = []
    
    for i in range(0,maxmum):
        filepath = dirpath + pre + str(i) + '.csv'
        outpath = outdir + pre + str(i) + '.csv'
        if not os.path.exists(filepath):
            continue
        if os.path.exists(outpath):
            continue
        jobs.append(job_server.submit(drivingtime, \
                                     (apikey, filepath, placepath1, field1, placepath2, field2, outpath,), \
                                     (amapdriving,), ("pandas","urllib","bs4","os", "time", "from bs4 import BeautifulSoup")))
        
    for job in jobs:
        job()   
    
    
   
    
    
    
