# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 16:13:45 2019
@author: malcolmng.2015
"""
import sched, time
import datetime 
from datetime import date
from store_Sensor_Data_to_DB import write_to_heatmap
from durationCalculation import calculateDuration
from user_detection import getPastData,getBIDDistFromPi,compareDistance
from durationCalculation import resetSMSStatus
from durationCalculation import calculateDuration


def is_empty(any_structure):
    if any_structure:
        #print('Structure is not empty.')
        return False
    else:
        #print('Structure is empty.')
        return True 

#refresh heatmap every 30 seconds
s = sched.scheduler(time.time, time.sleep)
def do_something(sc): 
    print("Writing to heatmap table...")
    #------------Processing Code for Heatmap--------------------
    now_datetime = datetime.datetime.now()
    endAt = datetime.datetime.strftime(now_datetime, '%Y-%m-%d %H:%M:%S')
    endAt = str(endAt)
    table = 'Process_Table_Demo'
    startAt = str(now_datetime - datetime.timedelta(minutes=3))
    data_dict = getPastData(startAt, endAt, table, 'date_time')
    #print("Printing Raw Dictionary Data:")
    #for x in data_dict:
    #    print(x)
    pi1DistList = getBIDDistFromPi(data_dict, 'ducks&crafts')
    pi2DistList = getBIDDistFromPi(data_dict, 'fabriqade')
    #print("Beacon distance for Pi1: ")
    #print(pi1DistList)
    #print("Beacon distance for Pi2: ")
    #print(pi2DistList)
    #print()
    # to check that both dict return is not empty before comparing 
    if is_empty(pi1DistList) | is_empty(pi2DistList):
        if is_empty(pi1DistList):
            pi1DistList = []
        if is_empty(pi2DistList):
            pi2DistList = []
        write_to_heatmap(pi1DistList,pi2DistList, endAt)
    else:
        pi1CleanedList , pi2CleanedList = compareDistance( pi1DistList, pi2DistList )
        print("Cleaned Beacon List for Pi1:")
        print(pi1CleanedList)
        print("Cleaned Beacon List for Pi2:")
        print(pi2CleanedList)
        write_to_heatmap(pi1CleanedList,pi2CleanedList, endAt)
    s.enter(10, 1, do_something, (sc,))

    #------------Processing Code for duration Calculation & Sending SMS--------------------
    calculateDuration(str(date.today()))


resetSMSStatus()
s.enter(30, 1, do_something, (s,))
s.run()
        
##########################to test the date frame###################
#startime = "2019-03-21 16:35:05"
#endtime = "2019-03-21 16:38:05"
#    st = datetime.datetime.strptime(startime,'%Y-%m-%d %H:%M:%S')
#    et = datetime.datetime.strptime(endtime, '%Y-%m-%d %H:%M:%S')
#    startime = str(st + datetime.timedelta(minutes=1))
#    endtime = str(et + datetime.timedelta(minutes=1))
#    data_dict = getPastData(startime,endtime)
#    pi1DistList = getBIDDistFromPi(data_dict, 'ducks&crafts')
#    pi2DistList = getBIDDistFromPi(data_dict, 'fabriqade')
#    print("Beacon distance for Pi1: ")
#    print(pi1DistList)
#    print("Beacon distance for Pi2: ")
#    print(pi2DistList)
#    print()
#    # to check that both dict return is not empty before comparing 
#    if is_empty(pi1DistList) | is_empty(pi2DistList):
#        if is_empty(pi1DistList):
#            pi1DistList = []
#        if is_empty(pi2DistList):
#            pi2DistList = []
#        write_to_heatmap(pi1DistList,pi2DistList,endtime)
#    else:
#        pi1CleanedList , pi2CleanedList = compareDistance( pi1DistList, pi2DistList )
#        print("Cleaned Beacon List for Pi1:")
#        print(pi1CleanedList)
#        print("Cleaned Beacon List for Pi2:")
#        print(pi2CleanedList)
#        write_to_heatmap(pi1CleanedList,pi2CleanedList,endtime)