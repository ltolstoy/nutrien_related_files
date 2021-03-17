from __future__ import print_function
import json
import boto3
import datetime
import ast, os, sys
#import pgdb #via https://www.a2hosting.com/kb/developer-corner/postgresql/connecting-to-postgresql-using-python
#import psycopg2 #via https://codereview.stackexchange.com/questions/169157/insert-data-to-postgresql
from sqlalchemy import create_engine
import pg8000

s3 = boto3.resource('s3')  # high level API
ssm = boto3.client('ssm', region_name="us-east-2")  # low level API
sqs = boto3.client('sqs')
debug=1 #1 is ON, 0 is OFF. Output info into lambda log
here = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(here, "./vendored"))

def lambda_handler(event, context):
	# LAmbda function to get SQS message with info about new files in Agrian s3 
	# data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/
	# then get new json file content, and write it to Postgres DB table

	# deal with SSM
	print("------All Start------{}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
	response_ssm = ssm.get_parameter(
		Name='/nutrien/org-poc/dev/AGRIUM_DATA_LAKE_RECONCILIATION_GIS_URL', WithDecryption=True)
	address0 = response_ssm["Parameter"]["Value"]
	#address = address0.replace('postgres', 'postgresql+psycopg2',1) #to use psycopg2
	address = address0.replace('postgres', 'postgresql+pg8000', 1) #replace only FIRST occurance !
	print("\nDB address from SSM response:\n {}\n".format(address0))
	[usr,pwd,host, port,dbname] = address.replace(':','|').replace('/','|').replace('@','|').split('|')[3:8]
	if debug==1:
		print("address parameters:\nusr = {}\npwd={}\nhost={}\nport={}\ndbname={}".format(usr,pwd,host,port,dbname))

	# deal with SQS via https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
	q_url = "https://sqs.us-east-2.amazonaws.com/549323063936/agrium-data-lake-inner-delivery-queue"
	response_sqs = sqs.receive_message(
		QueueUrl=q_url,
		MaxNumberOfMessages=10,
		MessageAttributeNames=[
			'All'
		],
		VisibilityTimeout=10,
		WaitTimeSeconds=10
	)
	if debug==1:  
		print("Full SQS response:\n{}".format(response_sqs))
	try:
		print("Creating engine")
		engine = create_engine(address,pool_pre_ping=True)
	except Exception as e:
		print("Exception while create_engine:\n{}\n".format(e))	
	else:
		if debug==1:
			print("Creating engine Success!")
			print(engine)
	try:
		print("Creating connection")
		connection = engine.raw_connection() 		  #raw_connection() comes with cursor, connect() doest have cursor!
	except Exception as e:
		print("Exception while engine.connect:\n{}\n".format(e))
	else:
		if debug==1:
			print("Making connention Success!")
			print(connection)
	try:
		print("Creating cursor")
		cursor = connection.cursor()
	except Exception as e:
		print("Exception while creating cursor:\n{}\n".format(e))
	else:
		if debug==1:
			print("Making cursor Success!")
			print(cursor)	

	#conn = psycopg2.connect(database=dbname, user=usr, password=pwd, host=host)
	inserted_rows = 0
	if "Messages" in response_sqs:  #first check that Messages are in sqs at all
		print("     response_sqs has {} messages".format(len(response_sqs['Messages'])))
		for i, m in enumerate(response_sqs['Messages']):
			filename = ast.literal_eval(
				m["Body"])["Records"][0]["s3"]["object"]["key"]
			bucket = ast.literal_eval(
				m["Body"])["Records"][0]["s3"]["bucket"]["name"]
			reciept_handle = m["ReceiptHandle"] #use it to delete message from Q
			print("i={}    file: {}\n".format(i, filename))
			#bucket = 'data-lake-us-east-2-549323063936-encrypted'
			#key = 'Agrian/AdHoc/NoETL/field/b9ce223f-b861-44d1-8c1e-6cff1a57dd50.json'
			folder = filename.split('/')[3]  # like 'field' - for DB
			try:
				content = read_s3_file(bucket, filename)  # content of specified json file from s3, or None
			except Exception as e:
				print("Exception while reading file content:{}".format(e))
				
			if debug==1:
				print("File content:\n{}".format(content))
			if content != None:
				#print("\nFile {} in folder {} sent to write_to_postgres".format(filename, folder))
				try:
					inserted_rows = write_to_postgres(content, folder, address, connection, cursor)
					if debug==1:
						print("number of successfuly inserted into Postgres rows: {}".format(inserted_rows))
				except Exception as e:
					print("Exception while sending to write_to_postgres: {}".format(e))
					print("Breaking loop through messages on i={}".format(i))
					break
			else:
				print("\nFile {} in folder {} WAS NOT read".format(filename, folder))
			if inserted_rows >=1:  #we sucessfully inserted row into postgres
				print("Deleting processed message from SQS\n")
				response_del = sqs.delete_message(
					QueueUrl=q_url,
					ReceiptHandle=reciept_handle
				)
				if response_del["ResponseMetadata"]["HTTPStatusCode"] == 200:
					print("HTTPStatusCode is 200, Message deleted sucessfully")
				else:
					print("HTTPStatusCode is {}, apparently something happened and message was not deleted!\n".format(response_del["ResponseMetadata"]["HTTPStatusCode"]))
			else:
				print("inserted_rows={} Error in Postgres insertion!".format(inserted_rows))
			print("---    Received from SQS {} messages, processed {} messages  ----".format(len(response_sqs['Messages']), i+1)) 
	else:
		print("---- No 'Message' key in response_sqs! Nothing to process\n")

	connection.close()
	print("----All Done-----{}\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


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
	#Function to write collected json data into Postgresql db table
	#content: content of json file in form {...}
	#folder: name of the folder, like 'grower', for table naming
	#address: full address line to use sqlalchemy, obtained from SSM, like 'postgres://agrium_data_lake_reconciliation_gis:XxXxXx@data-lake-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com:5432/agrium_data_lake_reconciliation_gis'
	#connection: created externally, one for all files
	#cursor: created externally
	#reciept_handle = "ReceiptHandle" from the Message, to delete it from SQS after processing. Like "AQEBE9bB+K..."
	#return: number of rows inserted
	table_name = folder #like 'field'
	table_n = 'echelon_json.'+table_name # this form is for queries , otherwise doesn't work. 'echelon_json.field'
	query1 = """
			INSERT into """ + table_n + """ 
				(id, json, appended_at) 
			VALUES 
				(%s,%s,%s)
			ON CONFLICT (id) DO UPDATE SET
			id=EXCLUDED.id, json=EXCLUDED.json, appended_at=EXCLUDED.appended_at;
		""" # this query will  update existing record in postgres with new data
	#print("write_to_postgres: preparing values\n")
	if connection: #if connection was created
		if cursor: #and if cursor exists
			if content != u'""' : # only process not-empty strings
				val1 = ( json.loads(content)[folder]['id'], 
				content, 
				datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ) # id, json, appended_at
				try:
					#connection.execute(query1, val1) #write just one row
					out = cursor.execute(query1, val1)
					if debug==1:
						print("write_to_postgres cursor.execute output:\n{}".format(out))
			
				except Exception as e:
					print('Exception in write_to_postgres for {}: {}'.format(folder, e))
					connection.rollback()
			else:
				print("Empty file detected, not going to write it to Postgres")	
			connection.commit() 
			return cursor.rowcount #number of rows inserted into DB
		else:
			print("Cursor doesn't exist, no writing to Postgres going to happen")
			return 0
	else:
		print("Connection doesn't exist, no writing to Postgres going to happen")
		return 0