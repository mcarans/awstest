import csv
import json
import pandas as pd
import urllib.parse
import urllib.request
import boto3
import os
import traceback
import dynadbwriter as dw

s3_bucketname = os.environ['s3_bucketname']
s3_ocha_s3_folder = os.environ['s3_ocha_s3_folder'] 
s3_ocha_transformed_key	= os.environ['s3_ocha_transformed_key'] 
temp_output_filename = os.environ['temp_output_filename']
temp_output_filepath = os.environ['temp_output_filepath']	
who_feed_url = os.environ['who_feed_url']

def transform_handler (event, context):
    try:
        json_csv()
        write_to_s3()
        dw.dynamodb_handler(event, context)
    except Exception as ex:
        traceback.print_exception(type(ex), ex, ex.__traceback__)
        return {
        'statusCode': 500,
        'body': json.dumps('Error reading/transforming WHO COVID19  file'+str(ex))
        }

    # If everything executes with our exception, return 200
    return {
        'statusCode': 200,
        'body': json.dumps('WHO COVID19  file successfully transformed and stored')
    }

def write_to_s3( ):
   s3_client = boto3.client('s3')
   object = s3_client.upload_file(temp_output_filepath+temp_output_filename,s3_bucketname,s3_ocha_s3_folder+s3_ocha_transformed_key)

def json_csv():
    req = urllib.request.Request(who_feed_url)
    with urllib.request.urlopen(req) as response:
        json_feed = response.read()
    data = json.loads(json_feed.decode('utf-8'))
    normalized_df = pd.json_normalize(data.get('features'))
    normalized_df.drop(['geometry.x', 'geometry.y'], axis=1, inplace=True)
    normalized_df.columns = ['OBJECTID','ISO_2_CODE','ISO_3_CODE','ADM0_NAME','date_epicrv','NewCase','CumCase','NewDeath','CumDeath','Short_Name_ZH','Short_Name_FR','Short_Name_ES','Short_Name_RU','Short_Name_AR']
    normalized_df.to_csv(temp_output_filepath+temp_output_filename,index=False, encoding='utf-8')
    return

          
