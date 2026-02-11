import boto3
from botocore.exceptions import ClientError

def setup_minio():
    # MinIO configuration
    s3_endpoint = 'http://localhost:9000'
    access_key = 'minioadmin'
    secret_key = 'minioadmin'
    bucket_name = 'warehouse'

    print(f"Connecting to MinIO at {s3_endpoint}...")
    
    try:
        s3 = boto3.resource('s3',
                            endpoint_url=s3_endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key,
                            config=boto3.session.Config(signature_version='s3v4'),
                            region_name='us-east-1')

        # Check if bucket exists
        bucket = s3.Bucket(bucket_name)
        if bucket.creation_date:
            print(f"SUCCESS: Bucket '{bucket_name}' already exists.")
        else:
            print(f"Creating bucket '{bucket_name}'...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"SUCCESS: Bucket '{bucket_name}' created successfully.")
            
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
             print(f"Creating bucket '{bucket_name}'...")
             try:
                s3.create_bucket(Bucket=bucket_name)
                print(f"SUCCESS: Bucket '{bucket_name}' created successfully.")
             except Exception as create_err:
                 print(f"FAILED to create bucket: {create_err}")
        else:
            print(f"FAILED: Error checking bucket: {e}")
    except Exception as e:
        print(f"FAILED: Could not connect to MinIO. Is it running? Error: {e}")

if __name__ == "__main__":
    setup_minio()
