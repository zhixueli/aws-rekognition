import json
import os
import boto3
import datetime

from botocore.client import ClientError

rek = boto3.client('rekognition')

def lambda_handler(event, context):
    
    sourceS3Bucket = event['Records'][0]['s3']['bucket']['name']
    sourceS3Key = event['Records'][0]['s3']['object']['key']
    
    job_id = start_label_detection(sourceS3Bucket, sourceS3Key)
    
    return {
        'statusCode': 200,
        'body': json.dumps(job_id)
    }

# Recognizes labels in a video
def start_label_detection(bucket, key):
    
    response = rek.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        MinConfidence=90,
        NotificationChannel={
            'SNSTopicArn': "arn:aws:sns:us-east-1:066198483852:RekJobComplete",
            'RoleArn': "arn:aws:iam::066198483852:role/ContentAnalytics-rekognitionSNSRole-1B83FIP616LZK"
        }
    )
    sourceS3 = 's3://'+ bucket + '/' + key
    print('Job Id (label_detection) for file ' + sourceS3 + ': ' + response['JobId'])
    return response['JobId']
