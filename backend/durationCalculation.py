import datetime
from firebase_admin import db
from firebase_admin import credentials
import firebase_admin
import pandas as pd
import numpy as np
import scipy as sp
import sklearn
from store_Sensor_Data_to_DB import init_db, write_to_duration
from datetime import timedelta
from smsMessenger import sendMessage
from datetime import date

init_db()

beacon_to_person = { 
 'http://is439_t3_malcolm.com' : 'Mal'  ,
 'http://is439_t3_shengw.com' : 'WS'  ,
 'http://is439_t3_xiaohang.com' : 'XH' ,
}

def getPastData(startDate):
    startTime = ' 00:00:00'
    testDate = startDate + startTime
    
    ref = db.reference('Process_Table_Demo')
    testDate = datetime.datetime.strptime(testDate, '%Y-%m-%d %H:%M:%S')
    endAt = str(testDate + datetime.timedelta(days=1))

    #print(startAt)
    
    #store as orderedDict
    snapshot = ref.order_by_child('date_time').start_at(str(testDate)).end_at(endAt).get()
    return snapshot

#getPastData('2019-04-06')

def resetSMSStatus():
    init_db()
    ref = db.reference('sms')
    ducksncrafts_ref = ref.child('ducks&crafts')
    ducksncrafts_ref.update({
        'Malcolm': 0,
        'Sheng': 0,
        'Xiaohang': 0
    })
    fabriqade_ref = ref.child('fabriqade')
    fabriqade_ref.update({
        'Malcolm': 0,
        'Sheng': 0,
        'Xiaohang': 0
    })
    print('SMS Status is reset.')

def calculateDuration(startDate):
    date = startDate
    data_dict = getPastData(date)
    df = pd.DataFrame.from_dict(data_dict, orient='index')
    #subset = df['piID']
    #print(type(subset))
    if df.empty:
        print ("There is no customer visiting the mall today!")
        return

    df['date_time'] = pd.to_datetime(df['date_time'])
    df['person'] = np.nan
    for k in beacon_to_person.keys():
        df.loc[df['beaconID'] == k, 'person'] = beacon_to_person[k]
    df_time_spent = df.copy()
    df_time_spent = df_time_spent.drop(['distance','rssi'],axis=1)
    df_time_spent = df_time_spent.sort_values(['person', 'date_time'])
    for url in beacon_to_person:
        calculateDuration_store(df, df_time_spent, 'fabriqade', beacon_to_person.get(url))
        calculateDuration_store(df, df_time_spent, 'ducks&crafts', beacon_to_person.get(url))

def calculateDuration_store(df, df_time_spent, storename, person_id):
    print(df_time_spent)
    df_time_spent_specific = df_time_spent.loc[(df_time_spent['piID'] == storename) & 
                                              (df_time_spent['person'] == person_id)].copy()


    df_time_spent_specific['interval'] = pd.to_timedelta(np.nan)
    df_time_spent_specific['interval'] = df_time_spent_specific['date_time'].subtract(df_time_spent_specific['date_time'].shift(1))
    df_time_spent_specific = df_time_spent_specific.drop(df_time_spent_specific[df_time_spent_specific['interval'] > '00:03:00'].index)
    
    df_time_spent_specific['date'] = df.date_time.map(lambda x: x.strftime('%Y-%m-%d'))
    df_duration_result = df_time_spent_specific.groupby(
        ['date', 'piID', 'beaconID'])['interval'].sum().copy()
    
    df_duration_result.columns = [['visit duration']]
    
    #print(df_duration_result)
    for record in df_duration_result.iteritems():
        print("Printing record: " + str(record))
        date = record[0][0]
        piID = record[0][1]
        beaconID = record[0][2]
        duration = record[1]
        durationString = str(duration)
        visitDuration = durationString[7:]
        print("Visit Duration for " + str(piID) + " :" + str(visitDuration))
        write_to_duration(date, piID, beaconID, visitDuration)
        #----------trigger SMS-----------------
        ref = db.reference('sms/'+ str(piID))
        person = ''
        phone = ''
        if 'malcolm' in beaconID:
            person = 'Malcolm'
            phone = '+6596548513'
        elif 'shengw' in beaconID:
            person = 'Sheng'
            phone = '+6582271790'

        if duration > datetime.timedelta(seconds=30):
            print('Visit duration for ' + person + ' at ' + piID + ' is longer than 30 seconds.')
            if ref.child(person).get() == 0:
                print('SMS has been sent.')
                print()
                sendMessage(phone, piID)
                ref.update({person : 1})
            else:
                print('SMS has not been sent.')
        else:
            print('Visit duration for ' + person + ' at ' + piID + ' is shorter than 30 seconds.')
        