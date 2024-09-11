import zipfile
import io

def lambda_handler():
    try:
        #with open(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Sample of Bus Vehicle Diagnostics and GPS log from Clever Devices IVN system\CleverWare0000323240107.txt.zip", 'rb') as f:
        #    file = f
        #obj = {"Body": file}
        obj = {"Body": open(r"C:\Users\mjper\OneDrive\Documents\MTA\Operator Awareness Tool (OWARE)\Sample of Bus Vehicle Diagnostics and GPS log from Clever Devices IVN system\CleverWare0000323240107.txt.zip", 'rb')}
        with io.BytesIO(obj["Body"].read()) as tf:
            # rewind the file
            tf.seek(0)

            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(tf, mode='r') as zipf:
                for file in zipf.infolist():
                    fileName = file.filename
                    print(fileName)
                    #putFile = s3.put_object(Bucket=bucket2, Key=fileName, Body=zipf.read(file))
                    #putObjects.append(putFile)
                    #print(putFile)

    except Exception as e:
        print(e)
        raise e


def test():
    x = 2
    x+=8
    print(x)

if __name__=='__main__':
    lambda_handler()