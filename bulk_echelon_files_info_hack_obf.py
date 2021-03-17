#!/usr/bin/python
"""
Script to collect information from json files stored on s3, for Echelon/Agrian 43 folders, 
and save it in Postgres DB agrium_data_lake_reconciliation_gis in scheme echelon_filesinfo, one table per folder.
It also (in addition to logs like stderr, stdout, etc) saves reports in s3://emr-step-scripts-aws-account-silver/echelon_files_info/reports/ folder
If run from AWS Console Steps, use these parameters:
Using script-runner.jar (instead of command-runner.jar for spark app):
Step type 	Custom Jar
Name* 		sq-script-runner
JAR location*	s3://emr-clusters-silver-cluster-1-bootstrap/scripts/script-runner.jar
Arguments:
s3://emr-step-scripts-aws-account-silver/echelon_files_info/bulk_echelon_files_info.py -f commodity <- chanhge here folder name, or many folders comma-separated
-du agrium_data_lake_reconciliation_gis 
-dh data-lake-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com 
-dn agrium_data_lake_reconciliation_gis 
-pk /1/runtime/dev/RECONCILER_PASS (was /data-lake/data-lake-gis-db-password )
-d False
If run locally for debug, #!/usr/bin/python <-should be changed according to where your python is 
/usr/bin/python is MAC default v2.7.10 (old) - not the best choice , could be /anaconda3/bin/python for v3.6, etc
and 
change line 462 LOCAL = False to True
24/9/19 - hack version with hardcoded passwords and other DB info for environments. It looks in different folders for obfuscated datas and real
"""
#from pyspark import SparkContext, SparkConf, SQLContext
#from pyspark.sql import HiveContext, SparkSession
import sys 
import numpy as np
import pandas as pd
import time 
import json
import boto3
from datetime import datetime
from collections import OrderedDict

from sqlalchemy import create_engine, text
import argparse


def logging_prefix():
	return datetime.now().strftime('%m-%d-%Y %H:%M:%S.%f')


def log_processing_time(process, start, end):
	if end - start < 60:
		print('{0}: processing time for {1}: {2} seconds'.format(logging_prefix(), process, round(end - start)))
	else:
		print('{0}: processing time for {1}: {2} minutes'.format(logging_prefix(), process, round((end - start)/60)))


def log(message):
	print('{0}: {1}'.format(logging_prefix(), message))


def get_s3_client():
	log('Getting S3 client')
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	if LOCAL:
		session = boto3.Session(profile_name=AWS_CREDENTIALS_PROFILE)
		s3 = session.client('s3')
	else:
		s3 = boto3.client('s3')
	log('Received S3 client')
	return s3


def get_s3_resource():
	log('Getting S3 resource')
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	if LOCAL:
		session = boto3.Session(profile_name=AWS_CREDENTIALS_PROFILE)
		s3 = session.resource('s3')
	else:
		s3 = boto3.resource('s3')

	return s3


def get_ssm_client():
	log('entering get_ssm_client')
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	global REGION
	if LOCAL:
		if DEBUG: log('DEBUG: getting ssm client using local credentials')
		try:
			session = boto3.Session(profile_name=AWS_CREDENTIALS_PROFILE, region_name=REGION)
			ssm = session.client('ssm')
		except Exception as e:
			log('ERROR: exception getting SSM client. {}'.format(e))
			raise e
	else:
		if DEBUG: log('DEBUG: about to make boto3.client(\'ssm\') invocation in non-LOCAL environment')
		try:
			ssm = boto3.client('ssm', region_name=REGION)
		except Exception as e:
			log('ERROR: exception getting SSM client. {}'.format(e))
			raise e
	log('exiting get_ssm_client')
	return ssm

def get_bucket_and_key(file_uri):
	file_uri_parts = file_uri.split('/')
	bucket = file_uri_parts[0]  # e.g. 'data-lake-us-east-2-549323063936-internal'
	key = '/'.join(file_uri_parts[1:]) #e.g. 'ars/product/DEV/USA/blah.json'
	return bucket, key





def get_matching_s3_objects(bucket, prefix='', suffix=''):
	""" via https://alexwlchan.net/2018/01/listing-s3-keys-redux/
	Generate objects in an S3 bucket.
	:param bucket: Name of the S3 bucket, like 'data-lake-us-east-2-549323063936-encrypted'
	:param prefix: Only fetch objects whose key starts with this prefix (optional), like 'Agrian/AdHoc/NoETL/attachment/'
	note trailing '/', it's important for cases like 'crop_plan_x_x' and 'crop_plan'
	:param suffix: Only fetch objects whose keys end with this suffix (optional), like 'json'
	"""


	s3 = get_s3_client()
	if not s3:
		log("Can't get s3 client, exiting. AWS token expired, may be?")
		sys.exit(1)
	kwargs = {'Bucket': bucket}

	# If the prefix is a single string (not a tuple of strings), we can
	# do the filtering directly in the S3 API.
	if isinstance(prefix, str):
		kwargs['Prefix'] = prefix

	while True:
		# The S3 API response is a large blob of metadata.
		# 'Contents' contains information about the listed objects.
		try:
			resp = s3.list_objects_v2(**kwargs)
		except Exception as e:
			log("Exception in get_matching_s3_objects: {}".format(e))
			log("Probably logged in with wrong  AWS Role, or have line 399 LOCAL set to False instead of True (for local debug)")
		try:
			contents = resp['Contents']
		except KeyError:
			return

		for obj in contents:
			key = obj['Key']
			if key.startswith(prefix) and key.endswith(suffix):
				yield obj

		# The S3 API is paginated, returning up to 1000 keys at a time.
		# Pass the continuation token into the next response, until we
		# reach the final page (when this field is missing).
		try:
			kwargs['ContinuationToken'] = resp['NextContinuationToken']
		except KeyError:
			break


def get_files_info_boto(folder, env_name):
	"""
	Function that queries s3 to get files information from folder folder
	As I know the bucket to look for files, and prefix
	folder: like "grower"
	env_name: like 'dev','sit', etc
	return: pandas df, with columns: [fname, size, date, updated_at] - created from OrderedDict
	"""
	log('Entering get_files_info_boto')
	log('{}: using get_files_info_boto function'.format(folder))
	bucket='data-lake-us-east-2-549323063936-encrypted'
	if (env_name != 'prd') and (folder in ['farm','field', 'grower']):
		pref = 'Agrian/obfuscated_data/' 
	else:
		pref = 'Agrian/AdHoc/NoETL/' # so full path is bucket+'/' + prefix + folder

	log('using pref={} as env_name={} and folder={}'.format(pref, env_name, folder))
	data = OrderedDict() #to preserve the order of insertion
	data["fname"] = []
	data["size"] = []
	data["date"] = []
	data["updated_at"] = []
	
	p = pref+folder+'/' # like Agrian/AdHoc/NoETL/grower/'. NOTE that '/' is important for correct prefix, othervise filed_event with have filed_event_layer files too! (as prefix is the same, without '/')
	for item in get_matching_s3_objects(bucket, prefix=p, suffix='json'):
		path_as_list = item['Key'].split('/') #item['Key'] looks like 'Agrian/AdHoc/NoETL/list/xxx.json'
		if env_name == 'prd': #path is like 'Agrian/AdHoc/NoETL/attachment/subfolder/xxx.json'
			if len(path_as_list) == 6: #case of attachment or field_event_layer, where there are subfolders
				data["fname"].append(path_as_list[4] +'/'+path_as_list[5]) #append subfolder/fname for attachment and field_event_layer
				data["size"].append(item['Size'])              #size of file
				data["date"].append(item['LastModified'].replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))		#like datetime.datetime(2019, 5, 5, 6, 37, 3, tzinfo=tzutc())
				data["updated_at"].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) #current time
			elif len(path_as_list) == 5: # there are json files in 'Agrian/AdHoc/NoETL/', need to avoid those! So only go if there is 'Agrian/AdHoc/NoETL/folder/'
				data["fname"].append(path_as_list[4])              #append only fname
				data["size"].append(int(item['Size']))              #size of file
				data["date"].append(item['LastModified'].replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
				data["updated_at"].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) #current time
		else: #for dev, sit, pre path is like 'Agrian/obfuscated_data/field/xxx.json
			if len(path_as_list) == 5: #case of attachment or field_event_layer, where there are subfolders 'Agrian/obfuscated_data/attachment/subfolder/xxx.json'
				data["fname"].append(path_as_list[3] +'/'+path_as_list[4]) #append subfolder/fname for attachment and field_event_layer
				data["size"].append(item['Size'])              #size of file
				data["date"].append(item['LastModified'].replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))		#like datetime.datetime(2019, 5, 5, 6, 37, 3, tzinfo=tzutc())
				data["updated_at"].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) #current time
			elif len(path_as_list) == 4: # case 'Agrian/obfuscated_data/folder/xx.json'
				data["fname"].append(path_as_list[3])              #append only fname
				data["size"].append(int(item['Size']))              #size of file
				data["date"].append(item['LastModified'].replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"))
				data["updated_at"].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) #current time
	df = pd.DataFrame.from_dict(data)
	log('Exiting get_files_info_boto, all filesinfo collected')
	return df


def write_report( folder, report_message):
	"""
	Function to save report_message in s3
	ps3rep_bucket: "emr-step-scripts-aws-account-silver"
	ps3rep_folder: "echelon_files_info/reports/'
	  "s3://emr-step-scripts-aws-account-silver/echelon_files_info/reports/" path to folder for reports on s3
	folder: like "grower", to generate report filename
	report_message: few lines of text
	"""
	log('Entering write_report')
	ps3rep_bucket = "emr-step-scripts-aws-account-silver"
	ps3rep_folder= "echelon_files_info/reports/" #path to folder for reports on s3
	s3 = get_s3_client()
	if not s3:
		log("Can't get s3 client, exiting. AWS token expired, may be?")
		sys.exit(1)
	rep_fname = folder+'_'+datetime.now().strftime('%Y_%m_%d_at_%H_%M_%S')+'.txt' #like list_2019_01_30_at_11_01_59.txt
	report_message = '\n'.join(report_message)
	report_message = str.encode(report_message) # convert into bytes

	try:
		s3.put_object(Body=report_message, Bucket=ps3rep_bucket, Key=ps3rep_folder+rep_fname )
	except Exception as e:
		log("Exception in write_report: {}".format(e))
	log('Exiting write_report')


def get_size(obj, seen=None): #via https://goshippo.com/blog/measure-real-size-any-python-object/
	"""Recursively finds size of objects
	returns: size in bytes"""
	#log('Entering get_size') #It is recursive, so too many messages in logs
	size = sys.getsizeof(obj)
	if seen is None:
		seen = set()
	obj_id = id(obj)
	if obj_id in seen:
		return 0
	# Important mark as seen *before* entering recursion to gracefully handle
	# self-referential objects
	seen.add(obj_id)
	if isinstance(obj, dict):
		size += sum([get_size(v, seen) for v in obj.values()])
		size += sum([get_size(k, seen) for k in obj.keys()])
	elif hasattr(obj, '__dict__'):
		size += get_size(obj.__dict__, seen)
	elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
		size += sum([get_size(i, seen) for i in obj])
	#log('Exiting get_size')
	return size


def recreate_postgres_table(folder, db_user, db_host, db_port, db_name, db_pwd):
	"""
	Function to drop and re-create  Postgresql db table for folder, preparing for new data
	folder: name of the folder, like 'grower', for table naming
	"""
	global DEBUG
	log('Entering recreate_postgres_table')

	#log('getting SSM client so we can pull db-connection password from SSM')
	#ssm = get_ssm_client()
	#log('retrieving ssm_pw_key parameter')
	#try:
	#	response = ssm.get_parameter(Name=ssm_pw_key, WithDecryption=True)
	#except Exception as e:
	#	log('ERROR: exception retrieving ssm_pw_key. {}'.format(e))
	#	raise e
	#db_pwd = response["Parameter"]["Value"]

	address = 'postgresql://'+db_user+':'+db_pwd+'@'+db_host+':'+db_port+'/'+db_name
	log('postgresql connection string: {}'.format('postgresql://'+db_user+':*****@'+db_host+':'+db_port+'/'+db_name))

	try:
		log('getting postgres engine, connection, cursor')
		engine = create_engine(address)
		connection = engine.raw_connection()
		cursor = connection.cursor()
	except Exception as e:
		log('Exception in recreate_table for {}: {}'.format(folder, e))
		log('Exiting re-create table in ERROR state')
		return 'Bad'

	log('successful creation of postgres engine, connection, cursor')
	table_name = folder
	table_n = 'echelon_filesinfo.'+table_name # this form is for queries , otherwise doesn't work. 'echelon_filesinfo.field'
	if not engine.dialect.has_table(engine, table_name, schema='echelon_filesinfo'):  # If table don't exist, create it
		log('table {} doesn\'t exist; going to create it'.format(table_n))
		q="""CREATE TABLE """+table_n+""" (
		fname TEXT NOT NULL PRIMARY KEY,
		size INT, 
		date TIMESTAMP,
		updated_at TIMESTAMP);"""
		log('table {} did not exist, creating it'.format(table_name))
		if DEBUG: log('DEBUG: Table-creation query: {}'.format(q))
		try:
			result = engine.execute(q)
			#log(result)
			log('Successfully created {}'.format(table_n))
		except Exception as e:
			log('Exception in recreate_table for {}: {}'.format(folder, e))

	else:	#table exist, drop and create
		log('Table {} exists; dropping and recreating'.format(table_n))
		q = """DROP TABLE """+table_n+""";"""
		if DEBUG: log('DEBUG: Table-drop query: {}'.format(q))

		try:
			result = engine.execute(q)
			#log(result)
			log('Successfully dropped {}'.format(table_n))
		except Exception as e:
			log('Exception in recreate_table for {}: {}'.format(folder, e))

		log('Re-creating table {}'.format(table_n))
		q="""CREATE TABLE """+table_n+""" (
		fname TEXT NOT NULL PRIMARY KEY,
		size INT, 
		date TIMESTAMP,
		updated_at TIMESTAMP);"""
		if DEBUG: log('DEBUG: Table-recreation query: {}'.format(q))
		try:
			result = engine.execute(q)
			#log(result)
			log('Successfully re-created {}'.format(table_n))
		except Exception as e:
			log('Exception in recreate_table for {}: {}'.format(folder, e))

	log('Exiting recreate_postgres_table')
	return engine, cursor , connection # for the already open connection



def write_to_postgres(df, folder, engine, cursor,connection): 
	"""
	Function to write collected filesdata into Postgresql db table, all at once (per folder, of course)
	Need to go with executemany instead of using StringIO and copy_from, as there are conflicts (due to Lambda working while i read data from s3)
	df: pandas df, in format [fname, size, date, updated_at]
	folder: name of the folder, like 'grower', for table naming
	engine: engine from opened previously 
	cursor: cursor from opened previously
	"""
	global DEBUG

	log('Entering write_to_postgres')
	rows_number = len(df.index)  #how many rows we  are planning to write to db
	table_name = folder
	table_n = 'echelon_filesinfo.'+table_name # this form is for queries , otherwise doesn't work. 'echelon_filesinfo.field'
	
	list_of_dicts = df.to_dict("records")  #makes list of dicts from df
	if DEBUG: log('DEBUG: Building query for insertion into postgres db')
	query = """
			INSERT into """ + table_n + """ 
				("fname", "size", "date", "updated_at") 
			VALUES 
				(%(fname)s, %(size)s, %(date)s, %(updated_at)s)
			ON CONFLICT (fname) DO NOTHING ;
		"""
	if DEBUG:
		log('DEBUG: formed the query')
		log('DEBUG: query: {}'.format(query))

	try:
		log('Executing query to write to postgres')
		cursor.executemany(query, list_of_dicts)
		log('Successful execution of query')
	except Exception as e:
		log('ERROR: Exception in write_to_postgres for {}: {}'.format(folder, e))
		log('Rolling back...')
		connection.rollback()
	log('Successful execution of the query; committing')
	connection.commit()
	log('Successful commit')
	log("\tWrote to db {} rows, while df has {} rows".format(cursor.rowcount, rows_number))
	log('Exiting write_to_postgres')	




def files_info_collector( folder, db_user, db_host, db_port, db_name, db_pwd, env_name):
	"""
	For selected folder, coleects from s3: fname, size, date info
	ps3: "s3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/" path to root bucket on s3, where other folders with data persist
	folder: like "grower"
	env_name: like 'sit', 'dev'
	db_related info: comes from get_db_info()
	returns: report_message, to be written to ps3rep location (analog of stdout output when run locally)
	"""
	s0 = time.time()
	log('Entering files_info_collector')
	report_message =[] #put new strings here, it will be written to s3 after all
	df = get_files_info_boto(folder, env_name)
	s1 = time.time()
	rep_str = logging_prefix()+': '+'got fileinfo for '+str(len(df.index))+' files for '+folder
	report_message.append(rep_str)
	log('got fileinfo for '+str(len(df.index))+' files for '+folder)
	occupied_size = str(get_size(df))
	log('fileinfo occupies '+occupied_size+' bytes in RAM for '+folder)
	rep_str = logging_prefix()+': '+'fileinfo occupies '+occupied_size+' bytes in RAM for '+folder
	report_message.append(rep_str)
	engine, cursor, connection = recreate_postgres_table(folder, db_user, db_host, db_port, db_name, db_pwd) # drop and re-create table to keep filesinfo from folder
	write_to_postgres(df, folder, engine, cursor, connection)
	s2 = time.time()
	wr_t = str(round((s2-s1),1))  #writing time, string (in sec)
	rep_str = logging_prefix()+': '+'completed writing to postgres '+str(len(df.index))+' records in '+wr_t+' sec'
	report_message.append(rep_str)
	if s2-s0 > 60:
		log_str = '   Whole processing took '+str(round((s2-s0)/60,1))+' minutes for '+folder 
	else:
		log_str = '   Whole processing took '+str(round(s2-s0,1))+' seconds for '+folder
	log(log_str)
	report_message.append(logging_prefix()+':'+log_str)
	cursor.close()  #close connection only when all DB write operation (chunks or single file) is completed
	connection.close()
	log('Exiting files_info_collector')
	return report_message

def get_db_info( env_name):
	#Funcion to get DB connection parameters from AWS SSM
	#env_name: string, like 'dev', 'sit' , etc - provided as EMR step argument
	#returns: db_user, db_host, db_name, db_pwd
	global DEBUG
	log('Entering get_db_info')
	log('getting SSM client so we can pull db-connection info from SSM')
	ssm = get_ssm_client()
	
	try:
		log('retrieving db_pwd parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_PASS"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
		db_pwd = response["Parameter"]["Value"]
	except Exception as e:
		log('ERROR: exception retrieving db_pwd: {}\nBut continue with hardcoded version'.format(e))	
	
	try:
		log('retrieving db_user parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_USER"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
		db_user = response["Parameter"]["Value"]
	except Exception as e:
		log('ERROR: exception retrieving db_user: {}\nBut continue with hardcoded version'.format(e))	
	
	try:
		log('retrieving db_host parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_HOST"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
		db_host = response["Parameter"]["Value"]
	except Exception as e:
		log('ERROR: exception retrieving db_host: {}\nBut continue with hardcoded version'.format(e))	
	
	try:
		log('retrieving db_name parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_DB"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
		db_name = response["Parameter"]["Value"]
	except Exception as e:
		log('ERROR: exception retrieving db_name: {}\nBut continue with hardcoded version'.format(e))	
	
	#hack - hardcoded DB info 
	if env_name == 'dev':
		db_host = 'data-lake-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com'
		db_name = 'agrium_data_lake_reconciliation_gis'
		db_user = 'agrium_data_lake_reconciliation_gis'
		db_pwd = '69qTRCkpReNAGxziBMak'
	elif env_name == 'sit':
		db_host = 'data-lake-sit-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com'
		db_name = 'agrium_data_lake_sit_reconciliation_gis'
		db_user = 'agrium_data_lake_sit_reconciliation_gis'
		db_pwd = '3SvlAdDQtymNTo7vD9RV'
	elif env_name == 'pre':
		db_host = 'data-lake-pre-aurora-postgres-gis.cluster-c9sxhwh6zzyy.us-east-2.rds.amazonaws.com'
		db_name = 'agrium_data_lake_pre_reconciliation_gis'
		db_user = 'agrium_data_lake_pre_reconciliation_gis'
		db_pwd = 'jDSczEf1nThcc9CMY2RB'

	return db_host, db_name, db_user, db_pwd

def main():
	"""
	For every folder provided as argument, run the concatenation procedure:
	read bunch of json files from current folder, and save as one big file on local fs
	to create schema with
	"""
	global DEBUG

	ap = argparse.ArgumentParser()
	#ap.add_argument('-hive', '--hive_table_creation_files_location', required=True, help='S3 location where hive table-creation scripts reside')
	ap.add_argument('-f', '--folders', required=True, help='list of folders to be processed in comma-separated form; eg folder1,folder2,folder3')
	#ap.add_argument('-csl', '--chunk_size_limit', required=False, default=1.0, help='Processing chunk memory size limit, in GB. Defines the amount of memory to process in each chunk. The default is 1GB.')
	#ap.add_argument('-du', '--db_user', required=True, help='Database username used for executing db queries')
	#ap.add_argument('-dh', '--db_host', required=True, help='Database host, e.g. data-lake-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com')
	ap.add_argument('-dp', '--db_port', required=False, default='5432', help='Database listening port')
	#ap.add_argument('-dn', '--db_name', required=True, help='Name of the database against which queries are run')
	#ap.add_argument('-pk', '--ssm_pw_key', required=True, help='Key for the database password in the SSM Parameter Store')
	ap.add_argument('-env', '--environment', required=True, help='name of environments for script to run, one of [dev,sit,pre,prd]')
	ap.add_argument('-d', '--debug_logging', required=False, default=False, help='Debug-level toggle; True triggers logging at DEBUG level')
	args = vars(ap.parse_args())

	folders2process = args['folders'].split(",")
	DEBUG = json.loads(args['debug_logging'].lower()) #json.loads does proper string-to-boolean conversion
	#db_host = args['db_host']
	#db_name = args['db_name']
	#db_user = args['db_user']
	#ssm_pw_key = args['ssm_pw_key']
	db_port = args['db_port']
	env_name = args['environment']
	db_host, db_name, db_user, db_pwd = get_db_info(env_name)  # all comes from AWS SSM
	

	"""
	log('Input params')
	log('=======================================================================================')
	log('folders to process: {}'.format(folders2process))
	log('hive_table_creation_files_location: {}'.format(hive_table_creation_files_location))
	log('processing chunk memory size limit: {}GB'.format(args['chunk_size_limit']))
	log('target database host: {}'.format(db_host))
	log('target database port: {}'.format(db_port))
	log('target database name: {}'.format(db_name))
	log('target database username: {}'.format(db_user))
	log('ssm key for db password: {}'.format(ssm_pw_key))
	log('debug logging?: {}'.format(DEBUG))
	log('=======================================================================================')
	"""

	results = [] #list of messages for each folder
	exit_status = 0
	for folder in folders2process:
		if (env_name != 'prd') and (folder in ['farm','field', 'grower']): 
			ps3 = "s3://data-lake-us-east-2-549323063936-encrypted/Agrian/obfuscated_data/"
		else:    
			ps3 = "s3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/" #path to root bucket on s3

		try:
			log("--- Working with {} ".format((ps3 + folder)))
			report_message = files_info_collector( folder, db_user, db_host, db_port, db_name, db_pwd, env_name)
			write_report(folder, report_message)
		except Exception as e:
			log("ERROR in main: Exception processing folder {}: {}".format(folder, e))
			exit_status = 1
	if exit_status == 0:
		for i in results:
			print(i)

	log('--- Exiting with status: {}'.format(exit_status))
	sys.exit(exit_status)



if __name__== "__main__":
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	global DEBUG
	global REGION
	LOCAL = False # for local True, for cluster run - False
	AWS_CREDENTIALS_PROFILE = 'silver'
	REGION = 'us-east-2'
	main()
