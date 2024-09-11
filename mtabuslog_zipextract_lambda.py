import zipfile
import boto3
import io

secret_name = "mtabuslog_zipextract_lambda_secret"
region_name = "us-east-1"

# Set up our session and client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)
s3 = session.client('s3')

# Set up bucket name for extracted zip file
bucket2 = 'ivn-unzipped-log-files-test'
folder = 'Unzipped-Files'

def lambda_handler(event, context):
    
    # Call Secrets Manager
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    
    # Retrieve zip file from event    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    if not key.endswith(".zip"):
        print(f"Skipping '{key}' - Not a zip file.")
    else:
        try:
            print(f"Bucket: '{bucket}', Key: '{key}'")
            print(event['Records'])

            obj = s3.get_object(Bucket=bucket, Key=key)
            putObjects = []
            with io.BytesIO(obj["Body"].read()) as tf:
                # rewind the file
                tf.seek(0)
    
                # Read the file as a zipfile and process the members
                with zipfile.ZipFile(tf, mode='r') as zipf:
                    for file in zipf.infolist():
                        fileName = file.filename
                        putFile = s3.put_object(Bucket=bucket2, Key=folder+'/'+fileName, Body=zipf.read(file))
                        putObjects.append(putFile)
                        print(f"putFile: '{putFile}'")
            # Delete zip file after unzip
            if len(putObjects) > 0:
                deletedObj = s3.delete_object(Bucket=bucket, Key=key)
                print(f"Deleted file: '{deletedObj}'")

        except Exception as e:
            print(e)
            print(f"Error getting object '{key}' from bucket '{bucket}'. Make sure they exist and your bucket is in the same region as this function.")
            raise e