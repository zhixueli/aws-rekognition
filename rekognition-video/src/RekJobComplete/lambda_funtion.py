import json
import boto3

rek = boto3.client('rekognition')
s3 = boto3.resource('s3')
tran = boto3.client('translate')

def lambda_handler(event, context):
    
    message = event['Records'][0]['Sns']['Message']
    print(message)

    dic = json.loads(message)
    
    jobId = dic['JobId']
    video = dic['Video']['S3ObjectName']
    result = video.replace('.mp4','.json')
    s3bucket = dic['Video']['S3Bucket']
    
    print("Job Id:" + jobId)
    print("Video:" + video)
    print("JSON:" + result)
    print("S3 Bucket:" + s3bucket)
    
    #jobId = "bf068eb56968b80d9a0f6ca5de5c0f46bd969af282ddbecdf45574a9412b0479"
    response = rek.get_label_detection(JobId=jobId)
    responseStr = json.dumps(response)
    # print(responseStr)
    
    enTxtList = []
    
    for label in response["Labels"]:
        name = label["Label"]["Name"]
        if name not in enTxtList :
            enTxtList.append(name)
        
    enTxt = "\n".join(enTxtList).lower()
    print(enTxt)
    
    tranResponse = tran.translate_text(
        Text=enTxt,
        SourceLanguageCode='en',
        TargetLanguageCode='zh'
    )
    
    zhTxt = tranResponse["TranslatedText"]
    print(zhTxt)
    zhTxtList = zhTxt.split("\n")
    #print(zhTxtList)
    
    i = 0
    for txt in enTxtList:
        responseStr = responseStr.replace('"' + txt + '"', '"' + zhTxtList[i] + '"')
        i = i + 1
        
    print(responseStr)
    
    obj = s3.Object(s3bucket, result)
    obj.put(Body=responseStr.encode())
    
    return jobId
