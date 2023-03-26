import joblib
import os
import pandas as pd
import json
import firebase_admin
from google.cloud import storage
from firebase_admin import credentials
from firebase_admin import firestore
import requests

def tranform_bmi(x):
    if x < 18.5 :
        return 1
    elif 18.5 <= x <= 24.9 :
        return 2
    elif 25 <= x <= 29.9 :
        return 3
    elif 30 <= x <= 34.9 :
        return 4
    elif 35 <= x <= 39.9 :
        return 5
    else:
        return 6    
    
def tranform_age(x):
    if x <= 24 :
        return 1
    elif 25 <= x <= 29 :
        return 2
    elif 30 <= x <= 34 :
        return 3
    elif 35 <= x <= 39 :
        return 4
    elif 40 <= x <= 44 :
        return 5
    elif 45 <= x <= 49 :
        return 6
    elif 50 <= x <= 54 :
        return 7
    elif 55 <= x <= 59 :
        return 8
    elif 60 <= x <= 64 :
        return 9
    elif 65 <= x <= 69 :
        return 10
    elif 70 <= x <= 74 :
        return 11
    elif 75 <= x <= 80 :
        return 12
    else:
        return 13
    
def combine_expln(pos_dict,neg_dict, pdict):
    for key in pdict:
        if pdict[key] > 0:
            if key in pos_dict: 
                pos_dict[key] = pos_dict[key] + pdict[key]
            else:
                pos_dict[key] = pdict[key]
        else:
            if key in neg_dict: 
                neg_dict[key] = neg_dict[key] + pdict[key]
            else:
                neg_dict[key] = pdict[key]
       
    return pos_dict, neg_dict

api_endpoint_url = os.environ['URI_SERVICE_ENDPOINT']

cred = credentials.Certificate("service-account.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

## intitalise the summary record for each level
import datetime 
now = datetime.datetime.now()

#update status of all summary documents
docs = db.collection(u'summary_heart').where(u'status', u'==', 1).stream()
for doc in docs:
    doc.reference.update({"status": 0})


docs = db.collection(u'department').stream()
for dep_doc in docs:
    dep_data = dep_doc.to_dict()
    # ext_summary=db.collection(u'summary_diabetes').where(u'department', u'==', dep_data['name']).where(u'update_date_label',u'==',now.strftime('%d %b')).stream()
    # exists = False
    # for d in ext_summary:
    #     exists = True
    # if not exists:
    summary_row = { "update_date": now,
                   "update_date_label": now.strftime('%d %b'),
                   "count": 0,
                   "evaluated": 0,
                   "status": 1,
                   "total_risk": 0.0,
                   "department": dep_data['name']}
    print("initialise summary for "+dep_data['name'])
    #db.collection(u'expense_summary').document(dep_data['name']+str(i+1)).set(summary_row)
    update_time, doc_ref = db.collection(u'summary_heart').add(summary_row)

docs = db.collection(u'person').stream()
import json

pos_dict = {}
neg_dict = {}
for doc in docs:
    item = doc.to_dict()
    #print(item['health_record'].path)
    record_ref = db.document(item['health_ref'].path)
    record = record_ref.get().to_dict()
    
    request = record
    request['Age'] = tranform_age(request['Age'])
    request['BMI'] = tranform_bmi(request['BMI'])
    request.pop('Steps',None)
    request.pop('Children',None)
    request.pop('person_ref',None)
    
    request_data = { "instances": [request] }
    print(json.dumps(request_data) )
    response = requests.post(api_endpoint_url, json=request_data)
    print(response)
    
    result = json.loads(response.text)
    print(result['predictions']['result'][0], doc.id)
    prediction_data = {'output': result['predictions']['result'][0]}
    expln = result['predictions']['explainations'][0]
    prediction_data.update(expln)
    prediction_data['timestamp'] = firestore.SERVER_TIMESTAMP
    prediction_data['health_ref'] = record_ref
    #insertnew document for prediction 
    db.collection(u'prediction_heart').document(record_ref.id).set(prediction_data)
    
    #combine explaination summary with current one
    pos_dict, neg_dict  = combine_expln(pos_dict,neg_dict, expln)
    
    
    ## record summary
    summary_docs = db.collection(u'summary_heart').where(u'department', u'==', item['department']).order_by(u'update_date', direction=firestore.Query.DESCENDING).limit(1).stream()
    for summary_doc in summary_docs:
            if (prediction_data['output'] > 0.4):
                # count number of risk
                summary_doc.reference.update({"count": firestore.Increment(1)})
            # count number of evaluated
            summary_doc.reference.update({"evaluated": firestore.Increment(1)})
            summary_doc.reference.update({"total_risk": firestore.Increment(prediction_data['output'])})
    
#record explaination summary
for factor in pos_dict:
    factor_item = {'name':factor,'value':pos_dict[factor],'type':1}
    db.collection(u'expln_summary_heart').document("pos"+factor).set(factor_item)

for factor in neg_dict:
    factor_item = {'name':factor,'value':neg_dict[factor],'type':-1}
    db.collection(u'expln_summary_heart').document("neg"+factor).set(factor_item)
