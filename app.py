#7.18.19 - initial version
from __future__ import print_function
import json
import boto3
#import datetime
#import time
import ast
#import os
#import sys



connection =  None
s3 = boto3.resource('s3')  # high level API
ssm = boto3.client('ssm', region_name="us-east-2")  # low level API

debug = 1  # 1 is ON, 0 is OFF. Output info into lambda log
here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "./vendored"))
env = os.environ['Environment']
if env == 'develop':
    runtime = 'dev'
else:
    runtime = env

ssm_base = "/1/runtime/{}".format(runtime)


def lambda_handler(event, context):
    # Lambda function to get SQS message with info about new files in Internal s3
    # data-lake-us-east-2-549323063936-internal/farm, field, user, organization-data folers
    # then get new json file content, and change (obfuscate) some personal data there to keep it private
    # event: has all info we need, no need to manually pull sqs
    print("Full Event content is:\n{}".format(event))

    for i, m in enumerate(event['Records']):
        filename = ast.literal_eval(
            m["body"])["Records"][0]["s3"]["object"]["key"] #like "farm/PROD/staging/USA/DELTA_07-17-2019.json"
        bucket = ast.literal_eval(
            m["body"])["Records"][0]["s3"]["bucket"]["name"] #like "data-lake-us-east-2-549323063936-internal"
        folder = filename.split('/')[0]  # like 'field' 
        sz = ast.literal_eval(
            m["body"])["Records"][0]["s3"]["object"]["size"]  # file size, bytes
        obf_filename = 'obfuscated_data/'+filename  #where to save obfuscated file
        print("i={}    file: {}  size: {} \n".format(i, filename, sz, ))
        #bucket = 'data-lake-us-east-2-549323063936-encrypted'
        #key = "farm/PROD/staging/USA/DELTA_07-17-2019.json"

        try: # content of specified json file from original location s3, or None
            content = read_s3_file(bucket, filename)
        except Exception as e:
            print("Exception while reading file content:{}".format(e))

        if content != None:
            obfuscated_content = mod_content(content, folder)
            write_s3_file(bucket, obf_filename, obfuscated_content)
        else:
            print("\nFile {} in folder {} WAS NOT read".format(filename, folder))

        print("---    Received from SQS {} messages, processed {} messages  ----".format(
            len(event['Records']), i+1))

def read_s3_file(bucket, path):
    # Func to read content of the file from s3
    # bucket - like 'data-lake-us-east-2-549323063936-encrypted'
    # path - full path to file in the bucket, like 'Agrian/AdHoc/NoETL/field/b9ce223f-b861-44d1-8c1e-6cff1a57dd50.json'
    # return: file content or None
    out = None
    obj = s3.Object(bucket, path)
    attempts = 0
    flag = 1  # 1=fail to read file, 0 =  file read sucessfully
    e = "No  exceptions recorded"
    while attempts < 5:  # max 10 attempts to read each file
        try:
            # keep decode for special characters.
            out = obj.get()['Body'].read().decode('utf-8')
            flag = 0
            break  # stop if success reading file
        except Exception as e:
            print('{}'.format(e))
            attempts += 1
            flag = 1

    if flag == 1:
        print("Can't read file {} {} times\n".format(path, attempts))
    return out

def write_s3_file(bucket, path, obfuscated_content):
    #Function that saves obfuscated_content into filename
    #bucket: bucket to save file (same as original one)
    #path: path to obfuscated file version
    #obfuscated_content: obfuscated content [{},{},{}]
    obj = s3.Object(bucket, path)
    try:
        obj.put(Body=obfuscated_content)
    except Exception as e:
        print("Cant save obfuscated data in {}: {}".format(path, e))

def mod_content(content, folder):
    #Function that modifies raw ARS data to preserve user privacy
    #folder: str like 'user', 'farm', 'field'
    #content: array of json files: [{"InternalId":13562980,"Id":90201,"Name":"AAA",...,"country":"USA"},{},{}] - not dicts! as there are 'true', 'false' (not 'True', 'False') elements
    #return: modified array of jsons
    mod_content = []
    for d in content:  #for each json
        j = json.loads(d) #read element as json file (almost as dict)
        if folder == 'field':
            j_obf = obfuscator(j,j["AccountId"],["Name","AccountName","Locale","FarmName","State","County","Municipality","Township"])
        elif folder == 'farm':
            j_obf = obfuscator(j,j["AccountId"],["Name","AccountName","State","County"])
        elif folder == 'user':
            j_obf = obfuscator(j,j["DefaultAccountNumber"],["FirstName","LastName","Email","PhoneNumber","DefaultZipPostalCode"])
        mod_content.append(j_obf)
    return mod_content

def obfuscator(j, id_value, fields_to_change):
    #Function to change specified elements in json file
    #j: json file 
    #id_value: string like '1234567' used to substitute real data, but to preserve uniqness  
    #fields_to_change: array of dict fields to change, like ["A","Bb","Ccc"]
    #returns: obfuscated json
    for f in fields_to_change:
        j[f] = f+'_for_id_'+id_value #should generate obfuscated string for specified key of dict
    return j