# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 14:24:58 2019

@author: malcolmng.2015
"""
import json
import datetime 
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from random import randint
import pandas as pd

#===============================================================
# Functions to Load Database
def init_db():
    if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
        cred = credentials.Certificate('is439-team-warriot-eda63e4d338c.json')
        firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://is439-team-warriot.firebaseio.com/'
})

# function to save heatmap data to db
def write_to_heatmap(pi1CleanedList, pi2CleanedList,endtime):
    # Get a database reference to our blog.
    pi1_count = len(pi1CleanedList)
    pi2_count = len(pi2CleanedList)
    tblName = 'heatmap'
    ref = db.reference(tblName)
    #now_datetime = (datetime.datetime.now())
    #now_datetime = now_datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    ref.child(endtime).set({
        'ducks&crafts': pi1_count,
        'fabriqade': pi2_count
    })

    # function to save duration data to db
def write_to_duration(date, piID, beaconID, visitDuration):
    # Get a database reference to our blog.
    print("Writing to visit_duration table...")
    tblName = 'visit_duration'
    ref = db.reference(tblName)
    now_datetime = (datetime.datetime.now())
    now_datetime = now_datetime.strftime('%Y-%m-%d %H:%M:%S')
    
    ref.push().set({
        'beaconID': beaconID,
        'piID': piID,
        'date_time':now_datetime,
        'visit_duration':visitDuration
    })

# Function to save data to DB Table
def write_db(beaconID,date_time_Converted,distance,rssi_Str,piID):
    # Get a database reference to our blog.
    ref = db.reference('Process_Table_Demo')

    ref.push().set({
        'beaconID': beaconID,
        'piID': piID,
        'date_time':date_time_Converted,
        'distance':distance,
        'rssi':rssi_Str
    })

#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Topic, jsonData):
#Parse Data
	init_db()
	json_Dict = json.loads(jsonData)
	beaconID = json_Dict['url']
	date_Time = json_Dict['lastSeen']
	distance = json_Dict['distance']
	rssi_Str = json_Dict['rssi']
	piID = json_Dict['PID']
	
	dt1 = datetime.datetime.fromtimestamp(date_Time/1000)
	date_time_Converted = dt1.strftime('%Y-%m-%d %H:%M:%S')
	#print(date_time_Converted)
	#UID = randint(10000000,99999999)
	#print(date_time_Converted, piID)
	write_db(beaconID,date_time_Converted,distance,rssi_Str,piID)




