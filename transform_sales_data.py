import pandas as pd
import boto3
import os
import io

# Configuration
bucket = 'etl-version1'
raw_prefix = 'raw/'
cleaned_prefix = 'cleaned/'
region = 'us-east-1'

# Connect to S3
s3 = boto3.client('s3', region_name=region)

# List all files in the raw folder
response = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix)
files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

print(f"🧾 Found {len(files)} files in raw/")

for file_key in files:
    print(f"🔄 Processing {file_key}...")

    # Read file into DataFrame from S3
    obj = s3.get_object(Bucket=bucket, Key=file_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Clean and transform
    df.dropna(subset=['Price', 'Quantity'], inplace=True)
    df['Revenue'] = df['Price'] * df['Quantity']
    df.columns = [col.strip().replace(" ", "_") for col in df.columns]

    # Save back to S3 as cleaned CSV
    output_buffer = io.StringIO()
    df.to_csv(output_buffer, index=False)
    cleaned_key = cleaned_prefix + os.path.basename(file_key)

    s3.put_object(Bucket=bucket, Key=cleaned_key, Body=output_buffer.getvalue())
    print(f"✅ Uploaded cleaned file to {cleaned_key}")

print("🎉 All files cleaned and uploaded!")
