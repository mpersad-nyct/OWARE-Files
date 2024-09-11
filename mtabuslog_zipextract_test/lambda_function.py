import urllib
import zipfile
import boto3
import io
import os
from botocore.exceptions import ClientError

# Secret Name and Region
secret_name = os.environ['secret_name']
region_name = os.environ['region_name']


# Set up our session and client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)
s3 = session.client('s3')

# Set up bucket name for extracted zip file
bucket2 = 'mtabuslogs-unzipped'

def lambda_handler(event, context):
    
    # Call Secrets Manager
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    
    # Retrieve zip file from event    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    if not key.endswith(".zip"):
            print(f"Skipping {key} - Not a zip file.")
    else:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            putObjects = []
            with io.BytesIO(obj["Body"].read()) as tf:
                # rewind the file
                tf.seek(0)
    
                # Read the file as a zipfile and process the members
                with zipfile.ZipFile(tf, mode='r') as zipf:
                    for file in zipf.infolist():
                        fileName = file.filename
                        putFile = s3.put_object(Bucket=bucket2, Key=fileName, Body=zipf.read(file))
                        putObjects.append(putFile)
                        print(putFile)
    
    
            # Delete zip file after unzip
            # if len(putObjects) > 0:
            #     deletedObj = s3.delete_object(Bucket=bucket, Key=key)
            #     print('deleted file:')
            #     print(deletedObj)
    
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e