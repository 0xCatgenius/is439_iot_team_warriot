# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 12:57:43 2019

@author: malcolmng.2015
"""

import paho.mqtt.client as mqtt #import the client1
from store_Sensor_Data_to_DB import sensor_Data_Handler,init_db
import time
from smsMessenger import sendMessage 
from time import time, sleep

MQTT_Broker = 'test.mosquitto.org'
MQTT_Topic = 'team_warrIOT/process'


def on_message(client, userdata, msg):
    print("message received " ,str(msg.payload.decode("utf-8")))
    #print("message topic=",msg.topic)
    #print("message qos=",msg.qos)
    #print("message retain flag=",msg.retain)
    sensor_Data_Handler(msg.topic, msg.payload)

if __name__ == '__main__':
    init_db()
    print("creating new instance")
    client = mqtt.Client("team_warrIOT") #create new instance have to be unique!!!!!
    client.on_message=on_message        #attach function to callback

    print("connecting to broker")
    client.connect(MQTT_Broker) #connect to broker
    
    print("Subscribing to topic", MQTT_Topic)
    client.subscribe(MQTT_Topic, qos=2)
    client.loop_forever()    #start the loop

    #time.sleep(180) # wait
    #client.loop_stop() #stop the loop

    ###need to be in separate file
    while True:
        print('-------------------Admin System is in real time.--------------------')
        #refresh heatmap every 30 seconds
        #send SMS every one minute
        sleep(60 - time() % 60)

    #do a loop
        #refresh heatmap every 30 seconds
        #send SMS every one minute
