import json
import datetime 
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
from random import randint
import pandas as pd


def init_db():
    if firebase_admin._DEFAULT_APP_NAME not in firebase_admin._apps:
        cred = credentials.Certificate('is439-team-warriot-eda63e4d338c.json')
        firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://is439-team-warriot.firebaseio.com/'
})

init_db()
ref = db.reference('/')
#data = ref.get()
ref_table = ref.child('Process_Table_0312')
timeStamp = "2019-03-21 00:00:00"
list = ref_table.order_by_child('date_time').get()

