import json
import boto3
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sqlalchemy import inspect

client = boto3.client("ses")

 
    
def lambda_handler(event,context):
    url = URL(
    account = 'sz12556.ap-south-1.aws',
    user = 'Iram',
    password = 'Iram@123',
    database = "OTT_DEV",
    schema = 'OTT_LANDING_ZONE',
    warehouse='DEV_TRANSFORM',
    role = 'DEVELOPER',
    )
    engine = create_engine(url)
    connection = engine.connect()
    inspector = inspect(engine)
    schemas = inspector.get_schema_names()
    arr = []
    
    for schema in schemas:
      if schema=="ott_target_schema":
        #print("schema: %s" % schema)
        for table_name in inspector.get_table_names(schema=schema):
              #print(table_name)
              table_name=table_name.upper()
              query1 = '''SELECT $1,$2 FROM ''' + table_name 
              TABLES = pd.read_sql(query1, connection)
              arr.append(TABLES)
    print(arr)
    
    for k in list(arr):
      for i in list(k):
        flag = 0
        a=k[i].tolist()
        for j in a:
          if not j:
            flag = 1
            break
        if flag == 1:
          print(i," data not complete")
          
    for k in list(arr):
      for i in list(k):
        flag = 0
        a=k[i].tolist()
        for j in a:
          if(len(set(a)) != len(a)):
            flag = 1
            break
        if flag == 1:
          print(i," data not unique")
          
    datatypes = {}
    key = {}
    a = []
    for k in list(arr):
      for i in list(k):
        a = k[i].dtypes
        if i not in datatypes.keys():
          if a == 'object':
            a = 'string'
          datatypes[i] = a
    
    print(datatypes)
    
    print(event)
    status=[]
    
    for row in event['data']:
        from_address=row[1]
        to_address=row[2]
        subject=row[3]
        body=row[4]
        body = datatypes
        message = {"Subject": {"Data": subject}, "Body": {"Html": {"Data": body}}}
        response = client.send_email(Source = from_address, Destination = {"ToAddresses": [to_address]},Message = message)
        status.append([row[0],"Notification Sent"])
    return {
        'statusCode': 200,
        'data': status
    }
