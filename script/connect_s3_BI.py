import boto3, os, io
import pandas as pd 

my_key= 'your_key' 
my_secret= 'your_secret_key' 

my_bucket_name = 'tikicrawler' 
my_file_path = 'product_tiki2024-05-08_13_09_43.275781.csv' 

session = boto3.Session(aws_access_key_id=my_key,aws_secret_access_key=my_secret) 
s3Client = session.client('s3') 
df = s3Client.get_object(Bucket=my_bucket_name, Key=my_file_path) 
heart_disease_data = pd.read_csv(io.BytesIO(df['Body'].read()), header=0) 