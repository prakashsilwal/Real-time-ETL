import pandas as pd
from faker import Faker
import time
import boto3
import uuid

# ========== Configuration ==========
bucket_name = 'etl-version1'       # S3 bucket name
s3_folder = 'raw/'                 # folder inside the bucket
region = 'us-east-1'               # AWS region
interval_sec = 5                   # upload frequency in seconds

# ========== Setup ==========
fake = Faker()
s3 = boto3.client('s3', region_name=region)

print("🚀 Uploading fake sales data every 5 seconds. Press Ctrl+C to stop.")

while True:
    # 1. Generate one fake row
    row = {
        'TransactionID': str(uuid.uuid4()),
        'Date': pd.Timestamp.now().isoformat(),
        'Product': fake.word(),
        'Price': round(fake.random_number(digits=3) / 10, 2),
        'Quantity': fake.random_int(min=1, max=5),
        'Customer': fake.name()
    }
    df = pd.DataFrame([row])

    # 2. Save locally
    file_name = f"sales_{int(time.time())}.csv"
    df.to_csv(file_name, index=False)

    # 3. Upload to S3
    s3_key = f"{s3_folder}{file_name}"
    s3.upload_file(file_name, bucket_name, s3_key)
    print(f"✅ Uploaded to S3: {s3_key}")

    time.sleep(interval_sec)
