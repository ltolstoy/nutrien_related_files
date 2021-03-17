from __future__ import print_function
import json
import boto3
import datetime
import time
import ast
import os
import sys
# import pgdb #via https://www.a2hosting.com/kb/developer-corner/postgresql/connecting-to-postgresql-using-python
import psycopg2  # via https://codereview.stackexchange.com/questions/169157/insert-data-to-postgresql
from psycopg2.extras import Json, execute_values
from sqlalchemy import create_engine
# import pg8000

s3 = boto3.resource('s3')  # high level API
ssm = boto3.client('ssm', region_name="us-east-2")  # low level API
# sqs = boto3.client('sqs') # no need , as all is in 'event'
debug = 0  # 1 is ON, 0 is OFF. Output info into lambda log
here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "./vendored"))
env = os.environ['Environment']
if env == 'develop':
    runtime = 'dev'
else:
    runtime = env

ssm_base = "/1/runtime/{}".format(runtime)


def lambda_handler(event, context):
    # LAmbda function to get SQS message with info about new files in Agrian s3
    # data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/
    # then get new json file content, and write it to Postgres DB table
    # event: has all info we need, no need to manually pull sqs

    # deal with SSM
    #print("------All Start------{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    # response_ssm = ssm.get_parameter(Name='/nutrien/org-poc/dev/AGRIUM_DATA_LAKE_RECONCILIATION_GIS_URL', WithDecryption=True)
    response_ssm_db = ssm.get_parameter(
        Name=ssm_base+'/RECONCILER_DB', WithDecryption=True)
    response_ssm_host = ssm.get_parameter(
        Name=ssm_base+'/RECONCILER_HOST', WithDecryption=True)
    response_ssm_pass = ssm.get_parameter(
        Name=ssm_base+'/RECONCILER_PASS', WithDecryption=True)
    response_ssm_user = ssm.get_parameter(
        Name=ssm_base+'/RECONCILER_USER', WithDecryption=True)
    # address0 = response_ssm["Parameter"]["Value"]
    address0 = "postgres+psycopg2://{}:{}@{}:5432/{}".format(
        response_ssm_user["Parameter"]["Value"],
        response_ssm_pass["Parameter"]["Value"],
        response_ssm_host["Parameter"]["Value"],
        response_ssm_db["Parameter"]["Value"])

    # replace only FIRST occurance !
    # address = address0.replace('postgres', 'postgresql+psycopg2', 1)
    if debug == 1:
        print("\nDB address from SSM response:\n {}\n".format(address0))

    print("Full Event content is:\n{}".format(event))
    # for rec in event['Records']:
    #	fname = ast.literal_eval(rec['body'])['Records'][0]["s3"]["object"]["key"]
    try:
        #print("Creating engine")
        engine = create_engine(address0, pool_pre_ping=True)
    except Exception as e:
        print("Exception while create_engine:\n{}\n".format(e))
    else:
        if debug == 1:
            print("Creating engine Success!")
            print(engine)
    try:
        #print("Creating connection")
        # raw_connection() comes with cursor, connect() doest have cursor!
        connection = engine.raw_connection()
    except Exception as e:
        print("Exception while engine.connect:\n{}\n".format(e))
    else:
        if debug == 1:
            print("Making connention Success!")
            print(connection)
    try:
        #print("Creating cursor")
        cursor = connection.cursor()
    except Exception as e:
        print("Exception while creating cursor:\n{}\n".format(e))
    else:
        if debug == 1:
            print("Making cursor Success!")
            print(cursor)

    #conn = psycopg2.connect(database=dbname, user=usr, password=pwd, host=host)
    inserted_rows = 0

    for i, m in enumerate(event['Records']):
        filename = ast.literal_eval(
            m["body"])["Records"][0]["s3"]["object"]["key"]
        bucket = ast.literal_eval(
            m["body"])["Records"][0]["s3"]["bucket"]["name"]
        folder = filename.split('/')[3]  # like 'field' - for DB
        sz = ast.literal_eval(
            m["body"])["Records"][0]["s3"]["object"]["size"]  # file size, bytes
        # date in str, like '2019-04-23 16:21:59'
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(
            float(m['attributes']['ApproximateFirstReceiveTimestamp'])/1000))
        nrows = write_echelon_filesinfo(
            filename, sz, date, folder, connection, cursor)
        if debug == 1:
            print("Saved in echelon_filesinfo: {} rows".format(nrows))
        # reciept_handle = m["ReceiptHandle"] #use it to delete message from Q
        print("i={}    file: {}  size: {}  date: {}\n".format(
            i, filename, sz, date))
        #bucket = 'data-lake-us-east-2-549323063936-encrypted'
        #key = 'Agrian/AdHoc/NoETL/field/b9ce223f-b861-44d1-8c1e-6cff1a57dd50.json'

        try:
            # content of specified json file from s3, or None
            content = read_s3_file(bucket, filename)
        except Exception as e:
            print("Exception while reading file content:{}".format(e))

        if debug == 1:
            print("File content:\n{}".format(content))
        if content != None:
            #print("\nFile {} in folder {} sent to write_to_postgres".format(filename, folder))
            try:
                inserted_rows = write_to_postgres(
                    content, folder, address0, connection, cursor)
                if debug == 1:
                    print("number of successfuly inserted into Postgres rows: {}".format(
                        inserted_rows))
            except Exception as e:
                print("Exception while sending to write_to_postgres: {}".format(e))
                print("Breaking loop through messages on i={}".format(i))
                break
        else:
            print("\nFile {} in folder {} WAS NOT read".format(filename, folder))

        print("---    Received from SQS {} messages, processed {} messages  ----".format(
            len(event['Records']), i+1))

    connection.close()
    #print("----All Done-----{}\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


def write_echelon_filesinfo(filename, sz, date, folder, connection, cursor):
    # Func to write new file info into Postgres DB, echelon_filesinfo schema, table according to folder
    # It assumes that corresponding tables are already in echelon_filesinfo, so need just append or update record!
    # filename: full path except bucket, like 'Agrian/AdHoc/NoETL/field/b9ce223f-b861-44d1-8c1e-6cff1a57dd50.json'
    # sz: file size in bytes
    # date: 'ApproximateFirstReceiveTimestamp' from event, in string format, like '2019-04-23 16:21:59'. SHould be very close to creation time, in UTC
    # folder: like 'grower'
    # return: number of rows inserted
    path_as_list = filename.split('/')
    if len(path_as_list) == 6:  # case of attachment or field_event_layer, where there are subfolders
        # use subfolder/filename for attachment and field_event_layer
        fname = path_as_list[4] + '/'+path_as_list[5]
    else:
        fname = path_as_list[4]  # use only filename
    table_name = folder  # like 'field'
    # this form is for queries , otherwise doesn't work. 'echelon_filesinfo.field'
    table_n = 'echelon_filesinfo.'+table_name
    query = """
			INSERT into """ + table_n + """ 
				(fname, size, date, updated_at) 
			VALUES 
				(%s,%s,%s,%s)
			ON CONFLICT (fname) DO UPDATE SET
			fname=EXCLUDED.fname, size=EXCLUDED.size, date=EXCLUDED.date, updated_at=EXCLUDED.updated_at;
		"""  # this query will  update existing record in postgres with new data

    if connection:  # if connection was created
        if cursor:  # and if cursor exists
            val = (fname, sz, date,
                   datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # fname, size, date, updated_at
            try:
                out = cursor.execute(query, val)

            except Exception as e:
                print(
                    'Exception in write_echelon_filesinfo for {}: {}'.format(folder, e))
                connection.rollback()

            connection.commit()
            return cursor.rowcount  # number of rows inserted into DB
        else:
            print("Cursor doesn't exist, no writing to Postgres going to happen")
            return 0
    else:
        print("Connection doesn't exist, no writing to Postgres going to happen")
        return 0


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
            attempts += 1
            flag = 1

    if flag == 1:
        print("Can't read file {} {} times\n".format(path, attempts))
    return out


def write_to_postgres(content, folder, address, connection, cursor):
    # Function to write collected json data into Postgresql db table
    # content: content of json file in form {...}
    # folder: name of the folder, like 'grower', for table naming
    # address: full address line to use sqlalchemy, obtained from SSM, like 'postgres://agrium_data_lake_reconciliation_gis:XxXxXx@data-lake-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com:5432/agrium_data_lake_reconciliation_gis'
    # connection: created externally, one for all files
    # cursor: created externally
    # reciept_handle = "ReceiptHandle" from the Message, to delete it from SQS after processing. Like "AQEBE9bB+K..."
    # return: number of rows inserted
    table_name = folder  # like 'field'
    # this form is for queries , otherwise doesn't work. 'echelon_in.field'
    table_n = 'echelon_in.'+table_name
    query1 = """
			INSERT into """ + table_n + """ 
				(id, json, appended_at) 
			VALUES 
				(%s,%s,%s)
			ON CONFLICT (id) DO UPDATE SET
			id=EXCLUDED.id, json=EXCLUDED.json, appended_at=EXCLUDED.appended_at;
		"""  # this query will  update existing record in postgres with new data
    #print("write_to_postgres: preparing values\n")
    if connection:  # if connection was created
        if cursor:  # and if cursor exists
            if content != u'""':  # only process not-empty strings
                id_var = json.loads(content)[folder]['id']
                val1 = (json.loads(content)[folder]['id'],
                        content,
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # id, json, appended_at
                try:
                    # connection.execute(query1, val1) #write just one row
                    out = cursor.execute(query1, val1)
                    if debug == 1:
                        print("Checking if we can read from DB, for debug")
                        check_db_content(cursor, id_var, table_n)
                except Exception as e:
                    print('Exception in write_to_postgres for {}: {}'.format(folder, e))
                    connection.rollback()
            else:
                print("Empty file detected, not going to write it to Postgres")
            connection.commit()
            return cursor.rowcount  # number of rows inserted into DB
        else:
            print("Cursor doesn't exist, no writing to Postgres going to happen")
            return 0
    else:
        print("Connection doesn't exist, no writing to Postgres going to happen")
        return 0


def check_db_content(cursor, id_var, table_n):
    # Checking if DB has what we just writen there, by reading back

    print("--- Check: quering DB table={} for id={}".format(table_n, id_var))
    query = """
			select id, appended_at from """ + table_n + """ 
			where id='"""+str(id_var)+"""';"""
    try:
        cursor.execute(query)
    except Exception as e:
        print("Exception in check_db_content: {}".format(e))
    print("--- The number of rows obtained from DB to debug: {} ".format(cursor.rowcount))
    row = cursor.fetchone()
    print("--- Row obtained from db:\n{}".format(row))
