-- Lambda Function

import boto3
import urllib.request
import json

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    air_quality_url = "https://api.openaq.org/v1/measurements?limit=10000"  # increasing the limit for more records
    
    with urllib.request.urlopen(air_quality_url) as response:
        air_data = json.loads(response.read().decode())
        
    # Convert data to newline-delimited JSON format
    formatted_data = "\n".join([json.dumps(item) for item in air_data['results']])
    
    s3.put_object(
        Bucket='myrawbucket-785255668313',
        Key='openaq/air_quality_data.json',
        Body=formatted_data
    )
    
    return {'statusCode': 200, 'body': 'Air Quality data saved to S3'}
