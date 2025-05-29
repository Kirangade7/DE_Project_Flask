from datetime import datetime
import pandas as pd
import boto3
from io import StringIO


def handle_insert(record):
    print("Handling Insert: ", record)
    data = {}

    for key, value in record['dynamodb']['NewImage'].items():
        for dt, col in value.items():
            data.update({key: col})

    dff = pd.DataFrame([data])
    dff['EventType'] = record['eventName']
    return dff

def handle_modify(record):
    print("Handling Modify:", record)
    new_data, old_data = {}, {}

    for key, value in record['dynamodb']['NewImage'].items():
        for dt, col in value.items():
            new_data.update({key: col})

    dff_insert = pd.DataFrame([new_data])
    dff_insert['EventType'] = "INSERT"

    for key, value in record['dynamodb']['OldImage'].items():
        for dt, col in value.items():
            old_data.update({key: col})

    dff_remove = pd.DataFrame([old_data])
    dff_remove['EventType'] = "REMOVE"

    return pd.concat([dff_insert, dff_remove], ignore_index=True)

def handle_remove(record):
    print("Handle remove: ", record)

    data = {}

    for key, value in record['dynamodb']['OldImage'].items():
        for dt, col in value.items():
            data.update({key: col})

    dff = pd.DataFrame([data])
    dff['EventId'] = record['eventID']
    dff['EventType'] = record['eventName']
    return dff

def lambda_handler(event, context):
    print("Received event:")
    print(event)
    
    failed_records = []
    df = pd.DataFrame()
    table = None
    
    if 'Records' not in event:
        print("No 'Records' key found in event")
        return {}
	
    for record in event['Records']:
        try:
            table = record['eventSourceARN'].split("/")[1]

            if record['eventName'] == "INSERT":
                dff = handle_insert(record)
            elif record['eventName'] == "MODIFY":
                dff = handle_modify(record)
            elif record['eventName'] == "REMOVE":
                dff = handle_remove(record)
            else:
                continue

            if dff is not None:
                dff['created_at'] = record['dynamodb']['ApproximateCreationDateTime']
                df = pd.concat([df, dff], ignore_index=True)
                
        except Exception as e:
            print(f"Error processing record {record['eventID']}: {e}")
            failed_records.append({'itemIdentifier': record['eventID']})

    if not df.empty:
        
        print(f"DataFrame shape: {df.shape}")
       
        print(df.head())
        
        df = df.astype(str)
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3 = boto3.client('s3')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        key = f"staging/{table}_{timestamp}.csv"
        
        try:
            s3.put_object(Bucket="de-demo-7", Key=key, Body=csv_buffer.getvalue())
            print(f"Uploaded to s3://de-demo-7/{key}")
        except Exception as e:
            print(f"Failed to upload to S3: {e}")

    print(f'Successfully processed {len(event["Records"])} records.')
    
    return {'batchItemFailures': failed_records}
