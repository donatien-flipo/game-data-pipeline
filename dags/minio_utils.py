import os
import boto3
import json
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "data")

def get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT", MINIO_ENDPOINT)
    access_key = os.getenv("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """Automatically create the bucket if it does not exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
        except Exception as e:
            print(f"Bucket '{bucket_name}' check/create notice: {e}")

def upload_to_minio(data, s3_path):
    bucket = os.getenv("MINIO_BUCKET_NAME", BUCKET_NAME)
    s3 = get_minio_client()
    ensure_bucket_exists(s3, bucket)
    s3.put_object(
        Bucket=bucket,
        Key=s3_path,
        Body=json.dumps(data, indent=4),
        ContentType='application/json'
    )

def read_from_minio(s3_path):
    bucket = os.getenv("MINIO_BUCKET_NAME", BUCKET_NAME)
    s3 = get_minio_client()
    try:
        response = s3.get_object(Bucket=bucket, Key=s3_path)
        return json.loads(response['Body'].read())
    except:
        return None