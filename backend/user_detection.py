# -*- coding: utf-8 -*-
"""
Created on Sat Mar  9 17:05:20 2019

@author: malcolmng.2015
"""
import datetime
from firebase_admin import db
from store_Sensor_Data_to_DB import sensor_Data_Handler,init_db
import pandas as pd
import collections
import traceback

# val_List = {}
init_db()
# get data for past x Mins
# seperate based on Pi
# for each Pi, split by BID
# filter out those with appearance < 30
# create a dict for each Pi : BID => Ave. dist

# 
# ref = db.reference('Process_Table')
# # get the latesest 5 objects
# snapshot = ref.order_by_value().limit_to_last(1).get()
# df = pd.DataFrame(snapshot, columns=snapshot.keys())
# print(df)

def getPastData(startime, endtime, tableName, orderBy):

    #now_datetime = (datetime.datetime.now())
    #endAt = str(now_datetime)
    #print(now_datetime)
    #ref = db.reference('Process_Table_0312')
    #startAt = str(now_datetime - datetime.timedelta(minutes=3))
    
    ref = db.reference(tableName)
    #store as orderedDict
    snapshot = ref.order_by_child(orderBy).start_at(startime).end_at(endtime).get()
    #print(snapshot)
    return snapshot


def getBIDDistFromPi( data_dict, piName ):
    # store respective pi keys 
    # xh rec to store in dict as well per pi
    try:
        pi_list = [k for k,v in data_dict.items() for i in v.items() if piName in i]
    

        pi_BID = []
    
        pi_BID_K = []
    
        #attempt to store all bid in pi1
        for k,v in data_dict.items():
            for i in pi_list:
                if i == k:
                    for j in v.items():
                        if 'beaconID' in j:
                            pi_BID.append(j)
                            pi_BID_K.append(k)
                            
        pi_a = list(dict.fromkeys(pi_BID))
        # store unique BID
        pi_b = [bid for i in pi_a for bid in i if bid != 'beaconID']
    

    
        len_pi_b = len(pi_b)
        if len_pi_b == 0:
            return []
        curr_pi_Bid = pi_b[len_pi_b-1]
        track_pi_BID = {}
        curr_count = 0
        # track num of bid in respective pi
        while len_pi_b != -1:
            curr_count = 0
            len_pi_b-=1
            curr_pi_Bid = pi_b[len_pi_b]
            for k,v in data_dict.items():
                for i in pi_BID_K:
                    if i==k:
                        for j in v.items():
                            if curr_pi_Bid in j:
                                curr_count+=1
                                track_pi_BID[curr_pi_Bid] = curr_count
        #checker for accuracy
        #t =  [k for k,v in data_dict.items() for i in pi1_BID_K for j in v.items() if i == k if 'e1866a7c681a' in j]    
        #print(len(t))
        #t =  [k for k,v in data_dict.items() for i in pi2_BID_K for j in v.items() if i == k if 'e1866a7c681a' in j] 
        #print(len(t))
    
        #remove those item with records < 5
        track_pi_BID = {key:val for key, val in track_pi_BID.items() if val > 5}
    
        #track distance 
        pi_dist_k = []
    
        pi_dist = []
    
        pi_dist_v_bid = []
    
        pi_bid_dist = {}
        ave_dist = 0
        for k, v in data_dict.items():
            for i in pi_BID_K:
                if i==k:
                    for j in v.items():
                        for bid,cnt in track_pi_BID.items():
                            if bid in j:
                                pi_dist_k.append(k)
            
                            
        for k, v in data_dict.items():
            for i in pi_dist_k:
                if i==k:
                    for j in v.items():
                        if 'distance' in j:
                            pi_dist.append(j[1])
                        if 'beaconID' in j:
                            pi_dist_v_bid.append(j[1])
                            
            # links the default keys to dist
        pi_k_dist = dict(zip(pi_dist_k,pi_dist))
        pi_k_bid = dict(zip(pi_dist_k,pi_dist_v_bid))
    
        pibid_to_track = [bid for bid, cnt in track_pi_BID.items()]
    
        len_pi_c = len(pibid_to_track)
        curr_pi_Bid_dist = pi_b[len_pi_c-1]
    
        while len_pi_c != -1:
            len_pi_c-=1
            curr_pi_Bid_dist = pi_b[len_pi_c]
            for k, dist in pi_k_dist.items():
                for i, bid in pi_k_bid.items():
                    if k == i and curr_pi_Bid_dist in bid:
                        ave_dist+=dist
                        pi_bid_dist[curr_pi_Bid_dist] = ave_dist
                    
        for k, dist in pi_bid_dist.items():
            for bid, cnt in track_pi_BID.items():
                if k == bid:
                    dist/=cnt
                    pi_bid_dist[k] = dist
        return pi_bid_dist
    except Exception as error:
        traceback.print_exc()

def compareDistance ( pi1DistList, pi2DistList ):
    removeList = []
    for i in pi1DistList:
        if i in pi2DistList:
            if pi1DistList[i] > pi2DistList[i]:
                pi2DistList.pop(i)
            else:
                removeList.append(i)

    for j in removeList:
        pi1DistList.pop(j)

    return pi1DistList , pi2DistList



