import json
import boto3
import DQCheck
from datetime import datetime
import os
table = os.environ['table']
rejtable=os.environ['rej_table']
dynamodb = boto3.client('dynamodb')




def lambda_handler(event, context):
    # print(event)
    sysdate = str(datetime.now())
    method = event["httpMethod"]
    if(method == 'POST'):
        body = json.loads(event['body'])
        DQ,err_msg=DQCheck.dqcheck(body)   


        ORG_NAME = body['org_name'].strip()
        ORG_ID=body['org_id'].strip()
        ORG_NAME_SCRUB=body['org_name'].upper().strip()
        ADDR_LN_1=body['addr_ln_1'].strip()
        ADDR_LN_2=body['addr_ln_2'].strip()
        ADDR_CITY=body['city'].strip()
        ADDR_STATE_CD=body['state'].strip()
        ADDR_CNTRY=body['country'].strip()
        CEO_FIRST_NAME=body['ceo_fnm'].strip()
        CEO_LAST_NAME=body['ceo_lnm'].strip()
        PHN_NBR=body['phn_nbr'].strip()
        EADDR=body['email'].strip()

        if(DQ == True):
            response = dynamodb.put_item(TableName=table, Item={'ORG_ID': {'N': ORG_ID}, 'ORG_NAME': {'S': ORG_NAME}, 'ORG_NAME_SCRUB': {'S': ORG_NAME_SCRUB},
                                                                    'ADDR_LN_1': {'S': ADDR_LN_1}, 'ADDR_LN_2': {'S': ADDR_LN_2}, 'ADDR_CITY': {'S': ADDR_CITY},
                                                                    'ADDR_STATE_CD': {'S': ADDR_STATE_CD}, 'ADDR_CNTRY': {'S': ADDR_CNTRY}, 'CEO_FIRST_NAME': {'S': CEO_FIRST_NAME}, 'CEO_LAST_NAME': {'S': CEO_LAST_NAME},
                                                                    'PHN_NBR': {'S': PHN_NBR}, 'EADDR': {'S': EADDR},'LAST_UPDATE_DT':{'S':sysdate}}, ReturnValues='ALL_OLD')
            if('Attributes' not in response):
                return{
                'statusCode':response["ResponseMetadata"]["HTTPStatusCode"],
                'body':json.dumps('Record has been Inserted')
                }
            else:
                return{
                'statusCode':response["ResponseMetadata"]["HTTPStatusCode"],
                'body':json.dumps('Record has been Updated')
                }
        else:
            response = dynamodb.put_item(TableName=rejtable, Item={'ORG_ID': {'N': ORG_ID}, 'ORG_NAME': {'S': ORG_NAME}, 'ORG_NAME_SCRUB': {'S': ORG_NAME_SCRUB},
                                                                        'ADDR_LN_1': {'S': ADDR_LN_1}, 'ADDR_LN_2': {'S': ADDR_LN_2}, 'ADDR_CITY': {'S': ADDR_CITY},
                                                                        'ADDR_STATE_CD': {'S': ADDR_STATE_CD}, 'ADDR_CNTRY': {'S': ADDR_CNTRY}, 'CEO_FIRST_NAME': {'S': CEO_FIRST_NAME}, 'CEO_LAST_NAME': {'S': CEO_LAST_NAME},
                                                                        'PHN_NBR': {'S': PHN_NBR}, 'EADDR': {'S': EADDR}, 'ERR_MSG': {'S': err_msg}, 'STATUS': {'S': 'DQ_FAILED'},'LAST_UPDATE_DT':{'S':sysdate}}, ReturnValues='ALL_OLD')
            return{
                'statusCode': response["ResponseMetadata"]["HTTPStatusCode"],
                'body': json.dumps('DATA QUALITY VALIDATION FAILED')
            }
    
    elif(method=='GET'):
        #print(event)
        if(event["queryStringParameters"]["method"]=='scan'):
            tablename=event["queryStringParameters"]["tablename"]
            limit=int(event["queryStringParameters"]["limit"])
            response = dynamodb.scan(
            TableName=tablename,
            Limit=limit )
            res=response["Items"]
            print(res)
            return {
            'statusCode': 200,
            'body': json.dumps(res)
            }
        
        
        elif(event["queryStringParameters"]["method"]=='query'):
            tablename=event["queryStringParameters"]["tablename"]
            
            ORG_ID=str(event["queryStringParameters"]["org_id"])
            
            response = dynamodb.get_item(TableName=tablename,Key={'ORG_ID':{'N':ORG_ID}})
            try:
                res=response["Item"]
                return {
                'statusCode': 200,
                'body': json.dumps(res)
                }
            except:
                return {
                'statusCode': 400,
                'body': json.dumps('Item does not exists')
                }
    
    elif(method=='DELETE'):
        body=json.loads(event['body'])
        TableName=body['tablename']
        ORG_ID=str(body['org_id'])
        response = dynamodb.delete_item(
        TableName=TableName,
        Key={'ORG_ID':{'N':ORG_ID}},ReturnValues='ALL_OLD')
        if('Attributes' not in response):
            return{
                'statusCode':400,
                'body':json.dumps('Item does not exists')
                }
        else:
            return {
                'statusCode': 200,
                'body':json.dumps('Item has been Deleted')
                }
        
