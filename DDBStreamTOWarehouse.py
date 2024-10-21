from datetime import datetime
import pandas as pd
import boto3
from io import StringIO

def handle_insert(record):
    print("Handling Insert: ", record)
    dict = {}

    for key, value in record['dynamodb']['NewImage'].items():
        for dt, col in value.items():
            dict.update({key: col})

    dff = pd.DataFrame([dict])
    return dff



def lambda_handler(event, context):
    print(event)
    df = pd.DataFrame()

    for record in event['Records']:
        table = record['eventSourceARN'].split("/")[1]

        if record['eventName'] == "INSERT": 
            dff = handle_insert(record)
        df = dff

    if not df.empty:
        all_columns = list(df)
        df[all_columns] = df[all_columns].astype(str)

        path = table + "_" + str(datetime.now()) + ".csv"
        print(event)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)

        s3 = boto3.client('s3')
        bucketName = "de-project-mbuyelomich"
        key = "snowflake/" + table + "_" + str(datetime.now()) + ".csv"
        print(key)
        
        s3.put_object(Bucket=bucketName, Key=key, Body=csv_buffer.getvalue(),)

    print('Successfully processed %s records.' % str(len(event['Records'])))
 