# -*- coding: utf-8 -*-
"""
计算门对门时间
这是shortesttime.py的后续
Calculate the shortest door-to-door travel time.
This step is followed by the shortesttime.py

@author: zhhan
"""

import os
import pandas
import pp	



"""
计算最短门对门旅行时间
Calculate the door-to-door travel time.

place_id: the id of the place
catchmentdf: the dataframe of the catchments.csv in D2D-China\D2D-city
shortestdf: the dataframe of the shortest travel times between stops
checkdf: the dataframe of the check_in_out.csv
outpath: the file path of the results
"""
def door2door(place_id, catchmentdf, shortestdf, checkdf, outpath):
    subdf = catchmentdf[catchmentdf['place_id'] == place_id]
    subdf = subdf[~pandas.isnull(subdf['stop_id'])]
    
    checkindf = checkdf.copy()
    checkindf['stop_id_1'] = checkindf['stop_id']
    checkindf = pandas.DataFrame(checkindf, columns = ['stop_id_1','check_in']) 
    checkoutdf = checkdf.copy()
    checkoutdf['stop_id_2'] = checkoutdf['stop_id']
    checkoutdf = pandas.DataFrame(checkoutdf, columns = ['stop_id_2','check_out'])

    df1 = pandas.merge(subdf, shortestdf, how = 'inner', left_on = 'stop_id', right_on = 'stop_id_1')
    df2 = pandas.merge(df1, catchmentdf, how='inner', left_on = 'stop_id_2', right_on = 'stop_id')
    df2 = df2[(df2['place_id_x'] != df2['place_id_y'])]
    
    df3 = pandas.merge(df2, checkindf, how = 'left',  on = 'stop_id_1')
    df4 = pandas.merge(df3, checkoutdf, how = 'left',  on = 'stop_id_2')
    if(df4.shape[0] == 0):
        return None

    df4['totaltime'] = df4.apply(lambda x: round((x['traveltime_x'] + x['traveltime_y'] + x['traveltime'] + \
                                                  x['check_in'] + x['check_out']),2), axis = 1)
    
    df4 = df4.sort_values(by = ['place_id_y','totaltime'],axis = 0,ascending = [True,True]) 
    df4.drop_duplicates(subset = ['place_id_y'],keep = 'first', inplace = True)    

    df4.drop(['stop_id_x','stop_id_y'], axis = 1, inplace = True)
    df4.rename(columns = {'traveltime_x': 'traveltime_1', 'traveltime_y': 'traveltime', 'traveltime': 'traveltime_2'}, inplace = True) 
        
    df4.to_csv(outpath, encoding='gb18030', index = False)



"""
计算最短门对门旅行时间
Calculate the door-to-door travel time.

place_id: the id of the place
catchmentdf1: the dataframe of the catchments of the origin places
catchmentdf2: the dataframe of the catchments of the destination places
shortestdf: the dataframe of the shortest travel times between stops
checkdf: the dataframe of the check_in_out.csv
outpath: the file path of the results
"""
def door2door2(place_id, catchmentdf1, catchmentdf2, shortestdf, checkdf, outpath):
    subdf = catchmentdf1[catchmentdf1['place_id'] == place_id]
    subdf = subdf[~pandas.isnull(subdf['stop_id'])]
    
    checkindf = checkdf.copy()
    checkindf['stop_id_1'] = checkindf['stop_id']
    checkindf = pandas.DataFrame(checkindf, columns = ['stop_id_1','check_in']) 
    checkoutdf = checkdf.copy()
    checkoutdf['stop_id_2'] = checkoutdf['stop_id']
    checkoutdf = pandas.DataFrame(checkoutdf, columns = ['stop_id_2','check_out'])

    df1 = pandas.merge(subdf, shortestdf, how = 'inner', left_on = 'stop_id', right_on = 'stop_id_1')
    df2 = pandas.merge(df1, catchmentdf2, how='inner', left_on = 'stop_id_2', right_on = 'stop_id')
    df2 = df2[(df2['place_id_x'] != df2['place_id_y'])]
    
    df3 = pandas.merge(df2, checkindf, how = 'left',  on = 'stop_id_1')
    df4 = pandas.merge(df3, checkoutdf, how = 'left',  on = 'stop_id_2')
    if(df4.shape[0] == 0):
        return None

    df4['totaltime'] = df4.apply(lambda x: round((x['traveltime_x'] + x['traveltime_y'] + x['traveltime'] + \
                                                  x['check_in'] + x['check_out']),2), axis = 1)
    
    df4 = df4.sort_values(by = ['place_id_y','totaltime'],axis = 0,ascending = [True,True]) 
    df4.drop_duplicates(subset = ['place_id_y'],keep = 'first', inplace = True)    

    df4.drop(['stop_id_x','stop_id_y'], axis = 1, inplace = True)
    df4.rename(columns = {'traveltime_x': 'traveltime_1', 'traveltime_y': 'traveltime', 'traveltime': 'traveltime_2'}, inplace = True) 
        
    df4.to_csv(outpath, encoding='gb18030', index = False)    


    
"""
并行计算
Parallel operation

catchmentpath: the file path of the catchments.csv
shortestpath: the file path of the shortest travel times between stops
checkpath: the file path of the check_in_out.csv
outdir: the folder of the results 
kernels: the number of parallel kernel
"""    
def paralleljobs(catchmentpath, shortestpath, checkpath, outdir, kernels):
    ppservers = ()
    job_server = pp.Server(kernels, ppservers=ppservers)
    jobs = []

    catchmentdf = pandas.read_csv(catchmentpath, encoding ='gb18030')
    shortestdf = pandas.read_csv(shortestpath, encoding ='gb18030')    
    checkdf = pandas.read_csv(checkpath, encoding ='gb18030')    
    
    catchmentdf = pandas.DataFrame(catchmentdf, columns = ['place_id','stop_id','traveltime'])    
    shortestdf = pandas.DataFrame(shortestdf, columns = ['trip_id_1','stop_id_1','trip_id_2','stop_id_2',\
                                                         'transfer_stop','from_to_stops','traveltime'])
    
    catchmentdf = catchmentdf[catchmentdf['traveltime']!='None']
    catchmentdf['traveltime'] = catchmentdf.apply(lambda x: round(int(x['traveltime'])/60,2), axis=1)
    catchmentdf = catchmentdf[catchmentdf['traveltime'] <= 120] #小于2小时
    
    places = catchmentdf['place_id'].unique()
    
    for place_id in places:
        outpath = outdir + '\\' + place_id + '.csv'       
        jobs.append(job_server.submit(door2door, \
                                      (place_id, catchmentdf, shortestdf, checkdf, outpath,), \
                                      (), ("pandas","os","numpy",)))

        
    for job in jobs:
        job()



"""
并行计算
Parallel operation

catchmentpath1: the file path of the catchments of the origin places
catchmentpath2: the file path of the catchments of the destination places
shortestpath: the file path of the shortest travel times between stops
checkpath: the file path of the check_in_out.csv
outdir: the folder of the results 
kernels: the number of parallel kernel
"""    
def paralleljobs2(catchmentpath1, catchmentpath2, shortestpath, checkpath, outdir, kernels):
    ppservers = ()
    job_server = pp.Server(kernels, ppservers=ppservers)
    jobs = []

    catchmentdf1 = pandas.read_csv(catchmentpath1, encoding ='gb18030')
    catchmentdf2 = pandas.read_csv(catchmentpath2, encoding ='gb18030')
    shortestdf = pandas.read_csv(shortestpath, encoding ='gb18030')    
    checkdf = pandas.read_csv(checkpath, encoding ='gb18030')    
    
    catchmentdf1 = pandas.DataFrame(catchmentdf1, columns = ['place_id','stop_id','traveltime'])   
    catchmentdf2 = pandas.DataFrame(catchmentdf2, columns = ['place_id','stop_id','traveltime'])   
    shortestdf = pandas.DataFrame(shortestdf, columns = ['trip_id_1','stop_id_1','trip_id_2','stop_id_2',\
                                                         'transfer_stop','from_to_stops','traveltime'])
    
    catchmentdf1 = catchmentdf1[catchmentdf1['traveltime']!='None']
    catchmentdf1['traveltime'] = catchmentdf1.apply(lambda x: round(int(x['traveltime'])/60,2), axis=1)
    catchmentdf1 = catchmentdf1[catchmentdf1['traveltime'] <= 120] #小于2小时

    catchmentdf2 = catchmentdf2[catchmentdf2['traveltime']!='None']
    catchmentdf2['traveltime'] = catchmentdf2.apply(lambda x: round(int(x['traveltime'])/60,2), axis=1)
    catchmentdf2 = catchmentdf2[catchmentdf2['traveltime'] <= 120] #小于2小时

    
    places = catchmentdf1['place_id'].unique()
    
    for place_id in places:
        outpath = outdir + '\\' + place_id + '.csv'       
        jobs.append(job_server.submit(door2door2, \
                                      (place_id, catchmentdf1, catchmentdf2, shortestdf, checkdf, outpath,), \
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
def merge_alltypes(directpath, transferpath, outpath):
    if not os.path.exists(directpath) and not os.path.exists(transferpath):
        return None
    elif not os.path.exists(directpath) and os.path.exists(transferpath):
        df = pandas.read_csv(transferpath, encoding ='gb18030')
    elif os.path.exists(directpath) and not os.path.exists(transferpath):
        df = pandas.read_csv(directpath, encoding ='gb18030')  
    else:
        df1 = pandas.read_csv(directpath, encoding ='gb18030')
        df2 = pandas.read_csv(transferpath, encoding ='gb18030')
        df = df1.append(df2)
        df = df.sort_values(by = ['place_id_y','totaltime'],axis = 0,ascending = [True,True])
        df.drop_duplicates(subset = ['place_id_y'],keep = 'first', inplace = True)   
    df.to_csv(outpath, index = False)   
    


"""
将火车、高铁和飞机合并
Merge the results of conventional rail, HSR and air transport

railwaypath: the shortest travel time of conventional rail
hsrpath: the shortest travel time of HSR
airpath: the shortest travel time of air transport
outpath: the file path of results
"""
def merge_allmodes(railwaypath, hsrpath, airpath, outpath):
    if os.path.exists(railwaypath):
        df = pandas.read_csv(railwaypath, encoding ='gb18030')
    else:
        df = None

    if os.path.exists(hsrpath):
        hsrdf = pandas.read_csv(hsrpath, encoding ='gb18030')
        if df is None:
            df = hsrdf
        else:
            df = df.append(hsrdf)

    if os.path.exists(airpath):
        airdf = pandas.read_csv(airpath, encoding ='gb18030')
        if df is None:
            df = airdf
        else:
            df = df.append(airdf)
    
    if df is None:
        return None
    
    df = df.sort_values(by = ['place_id_y','totaltime'],axis = 0,ascending = [True,True])
    df.drop_duplicates(subset = ['place_id_y'],keep = 'first', inplace = True)   
    df.to_csv(outpath, index = False)       
          
    

"""
增加公路旅行时间
Add the travel time by road transport

dirpath: the file path of the shortest door-to-door travel time
roadpath: the travel time of the road transport
outdir: the folder of the results 
"""    
def addroad(dirpath, roadpath, outdir):
    roaddf = pandas.read_csv(roadpath, encoding ='gb18030')
    for i in range(1, 3):
        # print(i)
        d2ddf = pandas.DataFrame(columns=['place_id_x', 'place_id_y', 'trip_id_1', 'trip_id_2', \
                                          'stop_id_1', 'stop_id_2', 'transfer_stop', 'min_traveltime']) 
        
        place_id_x = 'X' + str(i)
        filepath = dirpath + '\\' + place_id_x + '.csv'
        if os.path.exists(filepath):
            df = pandas.read_csv(filepath, encoding ='gb18030')
        else:
            df = None
            
        roaddf_1 = roaddf[(roaddf['place_id_x'] == place_id_x) | (roaddf['place_id_y'] == place_id_x)]
        
        for j in range(1, 298):
            place_id_y = 'P' + str(j)
            
            roaddf_2 = roaddf_1[(roaddf_1['place_id_x'] == place_id_y) | (roaddf_1['place_id_y'] == place_id_y)]
            traveltime = round(float(roaddf_2.iloc[0]['traveltime']) / 60, 2)
            
            if (df is not None) and (place_id_x != place_id_y):
                subdf = df[df['place_id_y'] == place_id_y]                
                if subdf.shape[0] > 0:                          
                    totaltime = subdf.iloc[0]['totaltime']
                    if traveltime > totaltime: # 大规模交通工具下时间最短
                        row = pandas.DataFrame({'place_id_x': place_id_x,
                                                'place_id_y': place_id_y,
                                                'trip_id_1': subdf.iloc[0]['trip_id_1'],
                                                'trip_id_2': subdf.iloc[0]['trip_id_2'],
                                                'stop_id_1': subdf.iloc[0]['stop_id_1'],
                                                'stop_id_2': subdf.iloc[0]['stop_id_2'],
                                                'transfer_stop': subdf.iloc[0]['transfer_stop'],
                                                'min_traveltime': totaltime},
                                                index=[1])         
                        d2ddf = d2ddf.append(row, ignore_index=True) 
                        continue
            
            if place_id_x == place_id_y:
                traveltime = 0
            row = pandas.DataFrame({'place_id_x': place_id_x,
                                    'place_id_y': place_id_y,
                                    'trip_id_1': '',
                                    'trip_id_2': '',
                                    'stop_id_1': '',
                                    'stop_id_2': '',
                                    'transfer_stop': '',
                                    'min_traveltime': traveltime},
                                    index=[1]) 
            d2ddf = d2ddf.append(row, ignore_index=True)
        outpath = outdir + '\\' +  place_id_x + '.csv'
        d2ddf.to_csv(outpath, index = False, encoding ='gb18030') 
    



