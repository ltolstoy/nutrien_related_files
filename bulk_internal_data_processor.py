#!/usr/bin/python
# Script to process all existing files in s3, for specified folder, and put it into Postgres DB.
# Should distinguish obfuscated data (for dev, sit, pre environments for farm,field, user folders) and real data
# for prd env.
# DELTA and FULL files has the same structure (array of dicts, (each dict has 13 keys for farm))
# Note: we need to process only latest FULL_xxx.json file and UPDATE_xxx.json files written after that one, 
# to avoid duplications (because there are a lot of duplicated data in Inner folder:  
# every next FULL file has everything from previous time + new info, and there are UPDATES with the same info).
# How to Run in SIT cluster:
# Name: bulk_internal_data_farm_test
# JAR location: s3://emr-clusters-silver-cluster-1-bootstrap/scripts/script-runner.jar
# Arguments :s3://emr-step-scripts-aws-account-silver/internal_data_processing/bulk_internal_data_processor.py -f farm -env sit -d False
# Action on failure:Continue
# 11/07/2019 - use logging module instead of Scott's functions
# 11/18/2019 - use log-level as a script parameter (NOTSEt,DEBUG,INFO,WARNING,ERROR, CRITICAL), exclude CAN form processing, only USA for now
# v2 - 11/9/19 Instead of processing files one-by-one, may be I can join all into one big dataset and write it together

import sys  #for arguments
import numpy as np
import pandas as pd
import datetime
from datetime import datetime
import time, os, io
import json
import boto3
import logging
import logging.config
#from sqlalchemy import create_engine, text
import argparse
import psycopg2
from psycopg2.extras import Json, execute_values


#log_config via https://www.loggly.com/blog/4-reasons-a-python-logging-library-is-much-better-than-putting-print-statements-everywhere/
LOGGING_CONFIG = {
	'version': 1, # required
	'disable_existing_loggers': True, # this config overrides all other loggers
	'formatters': {
		'simple': {
			'format': '%(asctime)s %(levelname)s -- %(message)s'
		},
		'whenAndWhere': {
			'format': '%(asctime)s\t%(levelname)s -- %(processName)s %(filename)s:%(lineno)s -- %(message)s'
		}
	},
	'handlers': {
		'console': {
			'level': 'DEBUG',
			'class': 'logging.StreamHandler',
			'formatter': 'whenAndWhere'
		}
	},
	'loggers': {
		'': { # 'root' logger
			'level': 'DEBUG',
			'handlers': ['console']
		},
		'root': {
		   'level': 'CRITICAL',
		   'handlers': ['console']
	   },
	}
}

logging.config.dictConfig(LOGGING_CONFIG)
log = logging.getLogger(__name__) # factory method
#log.setLevel(logging.INFO)

def logging_prefix():
	return datetime.now().strftime('%m-%d-%Y %H:%M:%S.%f')


def log_processing_time(process, start, end):
	if end - start < 60:
		print('{0}: processing time for {1}: {2} seconds'.format(logging_prefix(), process, round(end - start)) )
	else:
		print('{0}: processing time for {1}: {2} minutes'.format(logging_prefix(), process, round((end - start)/60)) )


def log_local(message):
	print('{0}: {1}'.format(logging_prefix(), message) )


def get_s3_client():
	log.debug('Getting S3 client')
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	if LOCAL:
		session = boto3.Session(profile_name=AWS_CREDENTIALS_PROFILE)
		s3 = session.client('s3')
	else:
		s3 = boto3.client('s3')

	return s3


def get_s3_resource():
	log.debug('Getting S3 resource')
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	if LOCAL:
		session = boto3.Session(profile_name=AWS_CREDENTIALS_PROFILE)
		s3 = session.resource('s3')
	else:
		s3 = boto3.resource('s3')

	return s3


def get_ssm_client():
	log.debug('entering get_ssm_client')
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	global REGION
	if LOCAL:
		log.debug('DEBUG: getting ssm client using local credentials')
		try:
			session = boto3.Session(profile_name=AWS_CREDENTIALS_PROFILE, region_name=REGION)
			ssm = session.client('ssm')
		except Exception as e:
			log.debug('ERROR: exception getting SSM client. {}'.format(e))
			raise e
	else:
		log.debug('DEBUG: about to make boto3.client(\'ssm\') invocation in non-LOCAL environment')
		try:
			ssm = boto3.client('ssm', region_name=REGION)
		except Exception as e:
			log.debug('ERROR: exception getting SSM client. {}'.format(e))
			raise e
	log.debug('exiting get_ssm_client')
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
	kwargs = {'Bucket': bucket}

	# If the prefix is a single string (not a tuple of strings), we can
	# do the filtering directly in the S3 API.
	if isinstance(prefix, str):
		kwargs['Prefix'] = prefix

	while True:
		# The S3 API response is a large blob of metadata.
		# 'Contents' contains information about the listed objects.
		resp = s3.list_objects_v2(**kwargs)

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


def make_file_list_boto(folder, bucket, ps3):
	"""
	Function that queries s3 to get file size list and file names list from path p , folder folder
	As I know the bucket to look for files, and prefix
	folder: like "user"
	bucket: like "data-lake-us-east-2-549323063936-internal"
	ps3: path like "obfuscated_data/farm/PROD/staging/USA/" or "/user/PROD/staging/"
	returns: list of file names , list of file sizes (int)
	Ref from https://boto3.amazonaws.com/v1/documentation/api/1.9.42/reference/services/s3.html#S3.Client.list_objects_v2:
			'Key': 'string',
			'LastModified': datetime(2015, 1, 1), ...
			'Size': 123,
	"""
	log.debug('{}: starting make_file_list_boto function for {}'.format(folder,ps3))
	file_list = []
	size_list = []
	date_list = [] #<type 'datetime.datetime'>
	for item in get_matching_s3_objects(bucket, prefix=ps3, suffix='json'):
		file_list.append(item['Key'])
		size_list.append(item['Size'])
		date_list.append(item['LastModified'])  #elements are of <type 'datetime.datetime'>

	return file_list, size_list, date_list

def select_files_of_interest(file_list1):
	"""
	Gets full list of files in s3, return list of files to process: 
	latest FULL_xxx.json and all available UPDATE_ or DELTA_ after it
	file_list: like ['d/field/PROD/staging/UPDATE_12-11-2019.json','d/field/PROD/staging/FULL_10-09-2019.json',...] 
	size_list: like [12,23,345,...]
	date_list: list of datetime.datetime objects
	return: file_list
	"""
	log.debug("starting select_files_of_interest, receiving {} files".format(len(file_list1)))
	full=[x for x in file_list1 if ('FULL_' in x) and ('FULL_SAP' not in x)]  #list of files with 'FULL_' in name but exclude 'FULL_SAP_'
	dlt=[x for x in file_list1 if 'DELTA_' in x]
	upd=[x for x in file_list1 if 'UPDATE_' in x]
	date_of_full = full[-1].split('/')[-1].split('_')[1].split('.')[0]   #like '10-09-2019'	from 'FULL_10-09-2019.json'
	if len(full)>0:
		full.sort()  #alphabetic sort for list of files with FULL, from oldest to newest
	files_of_interest=[]	#list with files we are going to process, starting with FULL_+ UPDATE_ or DELTA_ files
	files_of_interest.insert(0,full[-1])		#put latest FULL_ file at 0th place	
	if len(dlt)>0:
		dlt.sort()   #alphabetic sort for list of files with DELTA, from oldest to newest
		try:
			indices = [i for i, s in enumerate(dlt) if date_of_full in s]  #find indices of files with the 'date_of_full' in filename
			files_of_interest = files_of_interest + dlt[indices[0]:]		#we need files older and including date_of_full, including latest FULL_xxx.json file
		except Exception as e:
			log.exception("exception in select_files_of_interest for DELTA_ files: date_of_full={}, full={},len(dlt)={}, indices={}, e={}".format(
				date_of_full, full,len(dlt), indices, e ) )	
	if len(upd)>0:
		upd.sort()
		try:
			indices = [i for i, s in enumerate(upd) if date_of_full in s]  #find indices of files with the 'date_of_full' in filename
			files_of_interest = files_of_interest + upd[indices[0]:]	#we need files older and including date_of_full, including latest FULL_xxx.json file at the beginning	
			#files_of_interest.insert(0,full[-1])	#put latest FULL_ file at 0th place
		except Exception as e:
			log.exception("exception in select_files_of_interest for UPDATE_ files: date_of_full={}, full={}, len(upd)={}, indices={}, e={}".format(
				date_of_full, full, len(upd), indices, e ) )

	log.debug("finishing select_files_of_interest, returning {} files".format(len(files_of_interest)))
	log.info("select_files_of_interest: selected for processing {} files of {}".format(len(files_of_interest), len(file_list1) ) )
	return files_of_interest

def read_s3_json(connection, bucket, key):
	""" adopted from app.py
	Reads file content from json object on S3.
	Attempts 3 times with an increasing backoff time before either returning object or None in case of failure.

	Parameters:
		connection (obj): boto3 s3 client object
		bucket (string): S3 bucket name
		key (string): specific file name within S3 bucket to query

	Returns:
		obj or None
	"""
	log.debug("starting read_s3_json for key={}".format(key) )
	out = None
	attempts = 1
	flag = 1  # 1=had not read file, 0 =  file read
	exc = "No  exceptions recorded"
	while attempts < 4:  # max 3 attempts to read each file, backoff time increases after each attempt
		try:
			# keep decode for special characters.
			log.debug("Reading JSON object from S3: bucket={} key={}".format(bucket, key) )
			obj = connection.get_object(Bucket=bucket, Key=key)
			out = json.load(obj['Body'])
			flag = 0
			log.debug("Reading JSON object complete!")
			break  # stop if success reading file
		except Exception as e:
			exc = e
			attempts += 1
			flag = 1
			time.sleep((attempts)*5.0)

	if flag == 1:
		log.exception("Can't read file {} {} times\nLast recorded exception: {}".format(
			key, attempts, str(exc)))
	log.debug("finishing read_s3_json")
	return out 


def get_content_boto3(iterator):
	"""
	Reads from s3 files in list (iterator). Need to get bucket from x (from iterator)
	iterator: list of files on s3 to read:
	lim: number of read file attempts
	returns: list of json files contents
	via https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
	 and https://stackoverflow.com/questions/41263304/s3-connection-timeout-when-using-boto3
	"""
	log.debug('Entering get_content_boto3')
	s3 = get_s3_resource()
	result = []
	for x in iterator:
		log.debug('DEBUG: getting contents of file {}'.format(x))
		x_split = x.split('/') # x is like 's3://data-lake-us-east-2-549323063936-encrypted/Agrian/obfuscated_data/grower/xx.json'
		bucket = x_split[2]  #e.g. 'data-lake-us-east-2-549323063936-encrypted'
		path = '/'.join(x_split[3:]) #e.g. 'Agrian/AdHoc/NoETL/list/8.json' or 'Agrian/obfuscated_data/grower/xx.json'
		obj = s3.Object(bucket, path)
		attempts = 0
		max_attempts = 5
		while attempts < max_attempts:
			try:
				content = obj.get()['Body'].read().decode('utf-8')
				log.debug('DEBUG: retrieved the body of the file')
				result.append(content) #Note: result is a list of contents of json files, distributed to current worker
				log.debug('DEBUG: appended body to results list')
				break
			except Exception as e:
				attempts += 1
				if attempts == max_attempts - 1: log.debug("ERROR: get_content_boto3 got error with {}:\n{}".format(x, e))
	log.debug('Exiting get_content_boto3 with {} results.'.format(len(result)))
	return [result]


def get_size(obj, seen=None): #via https://goshippo.com/blog/measure-real-size-any-python-object/
	"""Recursively finds size of objects"""
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
	return size

def get_db_info( env_name):
	#Funcion to get DB connection parameters from AWS SSM
	#env_name: string, like 'dev', 'sit' , etc - provided as EMR step argument
	#returns: db_user, db_host, db_name, db_pwd
	#global DEBUG
	log.debug('Entering get_db_info')
	log.debug('getting SSM client so we can pull db-connection info from SSM')
	log.info("get_db_info: env_name={}".format(env_name) )
	ssm = get_ssm_client()
	
	try:
		log.debug('retrieving db_pwd parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_PASS"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
	except Exception as e:
		log.debug('ERROR: exception retrieving db_pwd: {}'.format(e))	
	db_pwd = response["Parameter"]["Value"]
	log.debug("get_db_info: db_pwd {}".format(db_pwd))
	try:
		log.debug('retrieving db_user parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_USER"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
	except Exception as e:
		log.debug('ERROR: exception retrieving db_user: {}'.format(e))	
	db_user = response["Parameter"]["Value"]
	log.debug("get_db_info: db_user {}".format(db_user))
	try:
		log.debug('retrieving db_host parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_HOST"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
	except Exception as e:
		log.debug('ERROR: exception retrieving db_host: {}'.format(e))	
	db_host = response["Parameter"]["Value"]  #like 'data-lake-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com'
	log.debug("get_db_info: db_host {}".format(db_host))
	try:
		log.debug('retrieving db_name parameter')
		n="/1/runtime/"+ env_name + "/RECONCILER_DB"
		response = ssm.get_parameter(Name=n, WithDecryption=True)
	except Exception as e:
		log.debug('ERROR: exception retrieving db_name: {}'.format(e))	
	db_name = response["Parameter"]["Value"]
	log.debug("get_db_info: db_name {}".format(db_name))
	"""
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
		"""

	return db_host, db_name, db_user, db_pwd



def create_db_connection(engine, host, user, password, database):
	""" this one was adopted from nutrien-ars-ingest-sam/app.py Lambda
	Creates DB connection object either with psycopg2 or db8000.
	Attempts 3 times with an increasing backoff time before either returning object or None in case of failure.

	Parameters:
		engine (str): desired library either psycopg2 or db8000
		host (string): DB hostname
		user (string): DB username
		password (string): DB password
		database (string): default database

	Returns:
		obj or None
	"""
	log.debug("starting create_db_connection")
	pgconn = None

	if engine == 'psycopg2':
		pgconn = psycopg2.connect(host=host, user=user, password=password, database=database)
	else:
		log.error('Unknown engine provided!')
	log.debug("finishing create_db_connection")
	return pgconn

def db_table_existance_check(cursor, db_name, table_name, schema):
	"""Function to check if table exists in db
	cursor: created before from pcycopg2 connection
	db_name: like 'agrium_data_lake_sit_reconciliation_gis', received from SSM before
	schema: 'internal_in' usually
	table_name: like 'account','farm','field','user','organization','insert_log'
	return: True or False
	"""
	q="""SELECT EXISTS(SELECT 1 FROM information_schema.tables 
			  WHERE table_catalog='"""+db_name+"""' AND 
					table_schema='"""+schema+"""' AND 
					table_name='"""+table_name+"""');"""
	try:
		cursor.execute(q)
	except Exception as e:
			log.exception('Exception in db_table_existance_check for {}: {}'.format(table_name, e))
	try:
		out = cursor.fetchone()[0]  #gets True or False
	except Exception as e:
		log.exception('Exception in db_table_existance_check for {}: {}'.format(table_name, e))
	log.debug("db_table_existance_check for {}:{}".format(table_name,out))
	return out # True or False

def db_table_drop(cursor, schema, table_name):
	"""Function to drop table_name from db
	cursor: created before from pcycopg2 connection
	schema: 'internal_in' usually
	table_name: like 'account','farm','field','user','organization','insert_log'
	"""
	table_n = schema+'.'+table_name # like internal_in.user
	q = """DROP TABLE """+table_n+""";"""
	try:
		cursor.execute(q)
		log.debug("db_table_drop: dropping table {}".format(table_n) )
	except Exception as e:
		log.exception('Exception in db_table_drop for {}: {}'.format(table_name, e))

def db_table_create(cursor, schema, table_name):
	"""Function to create table_name at db
	cursor: created before from pcycopg2 connection
	schema: 'internal_in' usually
	table_name: like 'account','farm','field','user','organization','insert_log'
	"""
	q=""
	if table_name == 'account':
		q="""CREATE TABLE IF NOT EXISTS internal_in.account
		(
		accountnumber text
		, eid text
		, name text
		, activestatus text
		, debtstatus text
		, creditstatus text
		, locationcode text
		, zippostalcode text
		, customertype text
		, organization_eid text
		, row_loaded_timestamp timestamp with time zone DEFAULT NOW()
		, source_file text
		);"""
	elif table_name == 'farm':
		q="""CREATE TABLE IF NOT EXISTS internal_in.farm
		(
		internalid text
		, id text
		, name text
		, accountname text
		, accountid text
		, accountactive text
		, acres text
		, state text
		, county text
		, active text
		, createdtimestamp text
		, modifiedtimestamp text
		, country text
		, row_loaded_timestamp timestamp with time zone DEFAULT NOW()
		, source_file text
		);"""
	elif table_name == 'field':
		q="""CREATE TABLE IF NOT EXISTS internal_in.field
		(
		internalid text
		, id text
		, name text
		, accountid text
		, accountactive text
		, accountname text
		, locale text
		, acres text
		, state text
		, county text
		, municipality text
		, active text
		, farmname text
		, farmid text
		, section text
		, township text
		, range text
		, quarter text
		, createdtimestamp text
		, modifiedtimestamp text
		, country text
		, row_loaded_timestamp timestamp with time zone DEFAULT NOW()
		, source_file text
		);"""
	elif table_name == 'insert_log':
		q="""CREATE TABLE IF NOT EXISTS internal_in.insert_log
		(
		table_name text
		, source_file text
		, last_loaded_at text DEFAULT NOW()
		);"""
	elif table_name == 'organization':
		q="""CREATE TABLE IF NOT EXISTS internal_in.organization
		(
		name text
		, eid text
		, managingusereid text
		, relationship text
		, user_eid text
		, row_loaded_timestamp timestamp with time zone DEFAULT NOW()
		, source_file text
		);"""
	elif table_name == 'user':
		q="""CREATE TABLE IF NOT EXISTS internal_in.user
		(
		firstname text
		, lastname text
		, email text
		, phonenumber text
		, eid text
		, defaultaccountnumber text
		, defaultaccountsetbyuser text
		, defaultbranchid text
		, defaultbranchsetbyuser text
		, defaultzippostalcode text
		, defaultzippostalcodesetbyuser text
		, defaultcropconsultant text
		, phonenumbers jsonb
		, row_loaded_timestamp timestamp with time zone DEFAULT NOW()
		, source_file text
		);"""
	else:
		log.error("db_table_create: unknown table_name received, can not create such table, as there is no create statement!")
	try:
		cursor.execute(q)
		log.debug('db_table_create: creating table {}'.format(table_name))
	except Exception as e:
		log.exception('Exception in db_table_create for {}: {}'.format(table_name, e))

def recreate_postgres_tables(pgconn, db_name, schema, folder, count):
	"""
	Function to drop and re-create  Postgresql db tables for internal_in schema, preparing for new data
	pgconn: - psycopg2 connection to db, created before
	db_name: like 'agrium_data_lake_sit_reconciliation_gis' - obtained from SSM before
	schema: 'internal_in' by default
	folder: like 'user', 'farm', 'field'
	count: 0 or 1, it is important, as for 0 (USA for 'farm' and 'field', or 'user') we need to drop exisiting tables, for 1 (CAN)- not, 
	as tables already dropped and supposed to have only fresh USA info!
	"""
	log.debug('Entering recreate_postgres_tables')
	cursor = pgconn.cursor()
	if folder == 'farm':
		tables = ['farm']  #we need to recreate them all, one-by-one, except insert_log (don't touch this one)
	elif folder == 'field':
		tables = ['field']
	elif folder == 'user':
		tables = ['account', 'organization', 'user']
	if count == 0:
		log.debug('recreate_postgres_tables: count = 0 (USA), so re-creating exisiting tables for {}'.format(folder))
		for t in tables:
			if db_table_existance_check(cursor, db_name, t, schema): #if table exists
				db_table_drop(cursor, schema, t)
				db_table_create(cursor, schema, t)
			else:
				db_table_create(cursor, schema, t)
	else:
		log.debug('recreate_postgres_tables: count = 1 (CAN), so doing nothing to exisiting tables for {}, assuming it has fresh data from USA'.format(folder))
	log.debug('Exiting recreate_postgres_tables')
	



def write_report( folder, report_message):
	"""
	Function to save report_message in s3
	ps3rep_bucket: "emr-step-scripts-aws-account-silver"
	ps3rep_folder: "echelon_data_processing/reports/'
	  "s3://emr-step-scripts-aws-account-silver/echelon_data_processing/reports/" path to folder for reports on s3
	folder: like "grower", to generate report filename
	report_message: few lines of text
	"""
	log.debug('Entering write_report')
	ps3rep_bucket = "emr-step-scripts-aws-account-silver"
	ps3rep_folder= "echelon_data_processing/reports/" #path to folder for reports on s3
	s3 = get_s3_client()
	if not s3:
		log.debug("Can't get s3 client, exiting. AWS token expired, may be?")
		sys.exit(1)
	rep_fname = folder+'_'+datetime.now().strftime('%Y_%m_%d_at_%H_%M_%S')+'.txt' #like list_2019_01_30_at_11_01_59.txt
	report_message = '\n'.join(report_message)
	report_message = str.encode(report_message) # convert into bytes

	try:
		s3.put_object(Body=report_message, Bucket=ps3rep_bucket, Key=ps3rep_folder+rep_fname )
	except Exception as e:
		log.debug("Exception in write_report: {}".format(e))
	log.debug('Exiting write_report')

def validate_payload(payload):
	""" adopted from app.py
	Validates that payload list to be processed does not contain null values and that elements are dictionaries
	(not some other datatypes). A dictionary with separate lists of valid and invalid records is returned, with invalid
	list also containing the index key from the source data.

	Parameters:
		payload (list): List to be processed

	Returns:
		dict
	"""

	processed_payload = {
		'valid': [rec for rec in payload if type(rec) is dict],
		'invalid': [(n, rec) for n, rec in enumerate(payload) if type(rec) is not dict]
	}

	return processed_payload

def get_relatedid(direction, dimension, fact, key, id1, id2=None, id3=None):
	"""
	Takes two hierarchically related dictionaries and adds up to 3 desired relational id values (as identified by a key),
	either 'up' to get dimension key and insert into fact dict or 'down' to get fact key and insert into dimension dict.
		direction (str): either 'up' or 'down'
		dimension (dict): dictionary representing the dimension table
		fact (dict): dictionary representing fact table
		key (str): user defined identifier used as a prefix in the key name to be inserted
		id1 (str): key from the dimension or fact dict to insert into corresponding related dict
		id2 (str): Optional 2nd key from the dimension or fact dict to insert into corresponding related dict
		id3 (str): Optional 3nd key from the dimension or fact dict to insert into corresponding related dict
	Returns: dict
	"""
	log.debug("starting get_relatedid with direction={}, dimension={}, fact={}, key={}, id1={}".format(direction, dimension, fact, key, id1))
	if direction == 'up':
		output = dict(fact)
		output[key+'_'+id1] = dimension[id1]
		if id2 is not None:
			output[key+'_'+id2] = dimension[id2]
		if id3 is not None:
			output[key+'_'+id3] = dimension[id3]
	if direction == 'down':
		output = dict(dimension)
		output[key+'_'+id1] = fact[id1]
		if id2 is not None:
			output[key+'_'+id2] = fact[id2]
		if id3 is not None:
			output[key+'_'+id3] = fact[id3]
	log.debug("finishing get_relatedid")
	return output


def get_distinctdicts(list_of_dicts):
	"""
	Takes a list of dictionaries and returns a list with any duplicate enlements removed
	Parameters:
		list_of_dicts (list): list of dictionary objects
	Returns: list
	"""
	log.debug("starting get_distinctdicts")
	set_of_dicts = set(map(lambda x: json.dumps(x, sort_keys=True), list_of_dicts))
	log.debug("finishing get_distinctdicts")
	return [json.loads(d) for d in set_of_dicts]

def split_nonstruct(datadict, leave=[None]):
	"""
	Loops through dict and returns (as a dictionary) only key value pairs where value is non-struct type.
		datadict (dict): Dictionary to be processed
		leave (dict): List of key name(s) to leave in the dictionary
	Returns: dict
	"""
	return {k: datadict[k] for k in datadict if type(datadict[k]) not in [list, dict] or k in leave}


def user_breakdown(json_data):
	""" adopted from app.py
	Un-nests JSON data and assigns relational keys from parent entity. Returns dictionary of flat json objects.
	This function is specific to user data provided by the Inner team and must contain specific dictionary keys to work.

	Parameters:
		json_data (dict): nested dictionary

	Returns:
		dict
	"""
	log.debug("starting user_breakdown")
	user_split = get_distinctdicts([split_nonstruct(user, leave=['PhoneNumbers']) for user in json_data])
	org_split = get_distinctdicts([
		get_relatedid('up', user, split_nonstruct(org), 'User', 'Eid')
		for user in json_data
		for org in user['Organizations']
	])
	acct_split = get_distinctdicts([
		get_relatedid('up', org, split_nonstruct(acct), 'Organization', 'Eid')
		for user in json_data
		for org in user['Organizations']
		for acct in org['Accounts']
	])
	log.debug("finishing user_breakdown")
	return {'user': user_split, 'organization': org_split, 'account': acct_split}

def generate_insert_statement(schema_name, table_name, data_sample, file_name,
							  infer_cols=True, col_names=None, execute_values=True):
	"""
	Generates a SQL INSERT statement using keys from provided data_sample, or using user-specified column names
	if provided. Returns list with first element being the SQL statement, the second value being the named parameter
	string.
		schema_name (string): Name of schema where statement will be executed.
		table_name (string): Name of table into which statement will perform the insert.
		data_sample (list): Data to be inserted (or sample thereof)
		infer_cols (bool): Whether to infer column names from dict keys of sample data (uses first dict in list of data_sample)
		file_name (string): source file name
		col_names (list): List of user specified column names
		execute_values (bool): if true, named parameter string in statement replaced by %s and returned in list separately
	Returns: list
	"""
	log.debug("starting generate_insert_statement")
	if not infer_cols:
		cols = col_names
	else:
		cols = data_sample[0].keys()

	cols_string = str(tuple(cols)+('source_file',)).replace("'", "")
	values_string = str(tuple('%('+s+')s' for s in cols) + ('"'+file_name+'"', )).replace("'", "").replace('"', "'")
	if not execute_values:
		statement = 'INSERT INTO {}.{} {} VALUES {}'.format(
			schema_name,
			table_name,
			cols_string,
			values_string
		)
		return [statement, None]
	else:  #if execute_values =True - our case
		statement = 'INSERT INTO {}.{} {} VALUES %s'.format(
			schema_name,
			table_name,
			cols_string,
		)
	log.debug("finishing generate_insert_statement")
	return [statement, values_string]



def insert_into_postgres_table(data, schema_name, table_name, connection, file_name):
	"""
	Inserts data (in the form of lists of dictionaries) into specified table.
	By default the target table is truncated before inserting data.

	Parameters:
		data (list): List of non-nested data dictioanries, representing rows of data with keys as column names.
		schema_name (string): Name of the schema in target database , usually 'internal_in'
		table_name(string): Name of the table into which data is to be inserted. Table must exist already! like 'user','far','field'
		connection (obj): Open postgres connection object created by psycopg2 when connecting to database.
		* engine (string): db connection engine. If 'psycopg2', makes string for that library's execute_values method.
		file_name (string): source file name like "obfuscated_data/farm/PROD/staging/USA/UPDATE_xx.json"
		*truncate_first (bool): If True, truncate table before inserting data.

	Returns:
		boolean
	"""
	log.debug("starting insert_into_postgres_table")
	#truncate_statement = "TRUNCATE TABLE {}.{}".format(schema_name, table_name)
	incoming_file_schema = {
	'field': [
		'InternalId',
		'Id',
		'Name',
		'AccountId',
		'AccountActive',
		'AccountName',
		'Locale',
		'Acres',
		'State',
		'County',
		'Municipality',
		'Active',
		'FarmName',
		'FarmId',
		'Section',
		'Township',
		'Range',
		'Quarter',
		'CreatedTimestamp',
		'ModifiedTimestamp',
		'country'
	],
	'farm': [
		'InternalId',
		'Id',
		'Name',
		'AccountName',
		'AccountId',
		'AccountActive',
		'Acres',
		'State',
		'County',
		'Active',
		'CreatedTimestamp',
		'ModifiedTimestamp',
		'country'
	],
	'user': [
		'FirstName',
		'LastName',
		'Email',
		'PhoneNumber',
		'Eid',
		'DefaultAccountNumber',
		'DefaultAccountSetByUser',
		'DefaultBranchId',
		'DefaultBranchSetByUser',
		'DefaultZipPostalCode',
		'DefaultZipPostalCodeSetByUser',
		'DefaultCropConsultant',
		'PhoneNumbers'
	],
	'organization': [
		'Name',
		'Eid',
		'ManagingUserEid',
		'Relationship',
		'User_Eid',
	],
	'account': [
		'AccountNumber',
		'Eid',      
		'Name',                
		'ActiveStatus',        
		'DebtStatus',          
		'CreditStatus',        
		'LocationCode',        
		'ZipPostalCode',       
		'CustomerType',        
		'Organization_Eid', 
	],
	'insert_log': [
		'table_name'
	]
	} #dictionary with column names for every table 
	if not data or data == []:
		log.info("insert_into_postgres_table: nothing to insert into {}.{}".format(schema_name, table_name))
		return False
	file_name_ascii = file_name.encode("ascii","ignore")  #encode UTF string into ASCII, to avoid error like 'type "u" does not exist' in u'obfuscat...'
	insert_statement = generate_insert_statement(schema_name, table_name, data, file_name_ascii,
					infer_cols=False, col_names=incoming_file_schema[table_name], execute_values=True)  #[statement, values_string], and here value_string -> template !
	
	#statement = INSERT INTO internal_in.farm (InternalId, Id, Name, AccountName, AccountId, AccountActive, 
	# Acres, State, County, Active, CreatedTimestamp, ModifiedTimestamp, 
	# country, source_file) 
	#VALUES %s
	#'template' aka 'value_string'=(%(InternalId)s, %(Id)s, %(Name)s, %(AccountName)s, %(AccountId)s, %(AccountActive)s, 
	# %(Acres)s, %(State)s, %(County)s, %(Active)s, %(CreatedTimestamp)s, %(ModifiedTimestamp)s, 
	# %(country)s, 'obfuscated_data/farm/PROD/staging/USA/DELTA_10-13-2019.json')

	log.info("insert_into_postgres_table: inserting into {}.{}\n".format(schema_name, table_name))
	cur = connection.cursor()
	try:
		# insert data
		t1 = time.time()
		log.debug("running insert_into_postgres_table for {} records".format(len(data)))
		#log.debug("insert_into_postgres_table {} records:\n sql={}\n argslist={}\n template={}".format(len(data),insert_statement[0],data,insert_statement[1]))
		execute_values(cur, sql=insert_statement[0], argslist=data, template=insert_statement[1])
		#cur.executemany(insert_statement[0], data)

		t2 = time.time()
		et = t2 - t1
		log.info('Succesfully inserted {0} records into {1}.{2}. Execution time: {3:.3f} sec'.format(
			str(len(data)), schema_name, table_name, et))
	except Exception as e:
		log.exception('Exception in insert_into_postgres_table for {}.{} : {}\n'.format(
			schema_name, table_name, e))
		log.exception('insert_into_postgres_table insert_statement aka sql={}\ninsert_statement[1] aka template={}\ndata[0]={}\n'.format( insert_statement[0], insert_statement[1], data[0]))
		connection.rollback()
		return False
	else:
		log.info("Commiting to DB...")
		connection.commit()

	cur.close()
	log.debug("finishing insert_into_postgres_table")
	return True

def processor(ps3, folder, bucket, db_user, db_host, db_port, db_name, db_pwd, env_name, count):
	""" Function which does the actual job: having bucket and ps3 path, gets list of files in that folder,
	and organizes input of the content in those into DB.
	ps3: path like "obfuscated_data/farm/PROD/staging/USA/"
	bucket: "data-lake-us-east-2-549323063936-internal"  s3 bucket with data (real and obf)
	folder: like 'user', 'farm', 'field'
	env_name: like 'dev', 'sit', 'pre', 'prd'
	count: 0 or 1, it's impprtant, as for 0 (USA for 'far', 'field' or 'user') we need to drop exisiting tables, for 1 (CAN)- not, 
	as tables already dropped and supposed to have only fresh USA info!
	"""
	#ps3 = "user/PROD/staging/"  # ONLY for debugging, testing REAL data in SIT DB by providing path to real data
	#ps3 = "farm/PROD/staging/USA/"  # ONLY for debugging, testing REAL data 
	#ps3 = "field/PROD/staging/USA/"  # ONLY for debugging, testing REAL data 
	log.debug("starting processor for ps3={}, folder={}, count={}".format(ps3, folder, count))
	try:
		file_list1, size_list1, date_list1 = make_file_list_boto(folder, bucket, ps3) #note date_list has elements of <type 'datetime.datetime'>
	except Exception as e:
		log.exception("Exception in processor func: {}".format(e))
	try:
		file_list = select_files_of_interest(file_list1) #list with files to process, from latest FULL_xx.json till today
		#log.debug("For DEBUG purposes only file_list is set to just one file here")
		#file_list = ["obfuscated_data/farm/PROD/staging/USA/FULL_10-09-2019.json"]	#smallest file , 17 records, 6.3K in size, for testing
	except Exception as e:
		log.exception("Exception in processor func: {}".format(e))
	log.info("files selected for processing, beginning and end:")
	if len(file_list)>=4:  #for long list
		log.info("beginning of the list: {}\n end of the list: {}".format(file_list[:2], file_list[-2:]) )
	elif len(file_list)>=2: #for short list debugging
		log.info("beginning of the list: {}\n end of the list: {}".format(file_list[:1], file_list[-1:]) )
	else:#for very short list debugging
		log.info("beginning of the list: {}".format(file_list[0]) )
	

	try:
		pgconn = create_db_connection(engine='psycopg2', host=db_host, user=db_user, password=db_pwd, database=db_name)
		if pgconn is None:
			log.error('Could not establish DB connection. Exiting function.')
			return "BAD"
	except Exception as e:
		log.exception("Exception in processor func: {}".format(e))
	
	try:
		s3 = get_s3_client() 
	except Exception as e:
		log.exception("Exception in processor func: {}".format(e))
		return "BAD"
	schema_name = 'internal_in'
	try:
		recreate_postgres_tables(pgconn, db_name, schema_name, folder, count) # if count==0, we need to drop all exisiting internal_in tables and re-create them, before staring writing there
	except Exception as e:
		log.exception("Exception in processor func while running recreate_postgres_tables: {}".format(e))

	
	if folder in ['farm','field']:
		tables_to_process = [folder] #['farm'] or ['field']
	elif folder == 'user':
		tables_to_process = ['user', 'organization', 'account'] #list of tables to process, tu un-nest data and split into user, org, acc
	
	data_json = [] #data from all read files joined into 1 list of dicts, to process it at once later
	
	for i, f in enumerate(file_list):  #now read data drom each file one-by-one, f= filename or key, i-counter
		if len(file_list)>100:
			if i%50 ==0: #output only every 50% of file names
				log.info("\tworking with {}/{}, file={}".format(i+1,len(file_list),f))
		else:  #for short file lists (farm, field) output every file
			log.info("\tworking with {}/{}, file={}".format(i+1,len(file_list),f))
		data = read_s3_json(s3, bucket, f)  #here f = key, like "obfuscated_data/farm/PROD/staging/USA/UPDATE_xxx.json". Small List of dicts
		if type(data) == list:
			data_json = data_json + data  #both are lists, so just add new data to full data_json list (list of dicts)
		else:
			log.error("type of data read from file {} is {}, can not concatenate it to data_json, exiting now".format(f,type(data)))
			sys.exit(1)
	
	sz = get_size(data_json)  #just in case, to make sure it fits in memory ok
	log.info("data_json has {} records , size={} bytes, files read {}".format(len(data_json), sz, len(file_list)))
	if not data_json or data_json == [None]:
		log.info('Unable to process data_json , it is None or empty, exiting now')
		sys.exit(1) #continue
	
	if 'user' not in f and type(tables_to_process) == str:  #for farm, field
		data_in = {tables_to_process: data_json}
		tables_to_process = [tables_to_process] #make a list of one element, like ['farm'] or ['field']
	if 'user' not in f and type(tables_to_process) == list:  #for ['farm'], ['field'] which is our case
		data_in = {tables_to_process[0]: data_json}  #like {'farm': data_json}
		#tables_to_process = [tables_to_process] #make a list of one element, like ['farm'] or ['field']
	elif 'user' in f and type(tables_to_process) == list: #the only option for 'user', looks like ['user', 'organization', 'account']
		# make sure records in the payload are dictionaries
		valid_data = validate_payload(data_json)['valid']  #eliminating not-dict data
		data_in = user_breakdown(valid_data)
		for i, r in enumerate(data_in['user']):
			data_in['user'][i]['PhoneNumbers'] = Json(r['PhoneNumbers'])
	else:
		log.error('Unknown data provided... Terminating: type(tables_to_process)={}, tables_to_process={}'.format(
			type(tables_to_process), tables_to_process) )
		raise Exception


	for table_name in tables_to_process: #'farm', or 'field', or ['user','organization','account']
		log.info("Inserting table {}".format(table_name))
		successful_insert = insert_into_postgres_table(data_in[table_name], schema_name, table_name, pgconn, file_name=f)  #True/False 
		log.debug("successful_insert = {}\n".format(successful_insert) )                                              

		# let the insert commits finish before moving on
		slt = 5 		#sleep time, sec
		log.debug("sleeping for {} sec after writing {}".format(slt, table_name))
		time.sleep(slt)
	
	pgconn.close()

	return 'OK'

def main():
	"""
	For every folder provided as argument, run function that reads content of the folder xxx 
	at s3://data-lake-us-east-2-549323063936-internal/xxx, creates a list of files to be processed, and runs processing on those files
	"""
	#global DEBUG

	ap = argparse.ArgumentParser()
	ap.add_argument('-f', '--folders', required=True, help='list of folders to be processed in comma-separated form; eg folder1,folder2,folder3')
	ap.add_argument('-env', '--environment', required=True, help='name of environments for script to run, one of [dev,sit,pre,prd]')
	ap.add_argument('-d', '--debug_level', required=False, default='INFO', help='Debug level, one of NOTSET(most verbose),DEBUG,INFO,WARNING,ERROR,CRITICAL')
	args = vars(ap.parse_args())

	folders2process = args['folders'].split(",")
	#DEBUG = json.loads(args['debug_logging'].lower()) #json.loads does proper string-to-boolean conversion
	loglevel=args['debug_level'].upper()  # in ['NOTSET','DEBUG','INFO','WARNING','ERROR','CRITICAL']
	if loglevel in ['NOTSET','DEBUG','INFO','WARNING','ERROR','CRITICAL']:
		numeric_loglevel = getattr(logging, loglevel.upper(), None )  #converts string to int: NOTSET->0, DEBUG->10,... CRITICAL->50
		log.setLevel( numeric_loglevel )  #only this works, need to put int value to set it for whole script
		log.debug("loglevel={}, numeric_loglevel={}".format(loglevel, numeric_loglevel))
	else:
		log.setLevel(logging.DEBUG)
	db_port = '5432' #args['db_port']
	env_name = args['environment']
	db_host, db_name, db_user, db_pwd = get_db_info(env_name)  # all comes from AWS SSM
	

	log.info('Input parameters')
	log.info('=======================================================================================')
	log.info('folders to process: {}'.format(folders2process))
	log.info('target database host: {}'.format(db_host))
	log.info('target database port: {}'.format(db_port))
	log.info('target database name: {}'.format(db_name))
	log.info('target database username: {}'.format(db_user))
	log.info('target db password: {}'.format(db_pwd))
	log.info('debug log-level: {}'.format(args['debug_level']))
	log.info('=======================================================================================')

	log.info('Starting the magic....')

	results = [] #list of messages for each folder, 2 for farm and field (if USA nad CAn both are processed), one for user
	exit_status = 0
	 
	bucket = "data-lake-us-east-2-549323063936-internal" #s3 bucket where data is
	for folder in folders2process: # we don't need '/' at the beginning of the path: '/user/PROD/staging/'
		ps = [] # array of paths excluding s3 bucket. 'farm' and 'field' have CAN and USA subfolders, 'user' doesn't 
		log.info("\tWorking on folder {}".format(folder))
		if env_name in ['dev','sit','pre'] and folder in ['farm','field']:
			ps.append("obfuscated_data/"+folder+"/PROD/staging/USA/")
			#ps.append("obfuscated_data/"+folder+"/PROD/staging/CAN/")
		elif env_name in ['dev','sit','pre'] and folder == 'user':  #for user there is no CAN and USA
			ps.append("obfuscated_data/"+folder+"/PROD/staging/")  
		elif env_name =='prd' and folder in ['farm','field']:  # need real data for prd
			ps.append(folder+"/PROD/staging/USA/")
			#ps.append(folder+"/PROD/staging/CAN/")
		elif env_name == 'prd' and folder == 'user':  #for user there is no CAN and USA
			ps.append(folder+"/PROD/staging/")

		#log.debug("EXITing for test purpose for now.")
		#sys.exit(1)

		for count,ps3 in enumerate(ps):  #for each CAN and USA subfolders.
			#count is 0, 1, and it's impprtant: for 0 (USA or user) we need to drop exisiting tables, for 1 (CAN)- not!
			try:
				s0 = time.time()
				log.info("\tWorking with {}, {} of {}".format('s3://'+bucket+'/'+ps3, count+1, len(ps) ) )  
				status = processor(ps3, folder, bucket, db_user, db_host, db_port, db_name, db_pwd, env_name, count) #status is BAD or OK
				#status = concatenation_parallel(ps3, hive_table_creation_files_location, folder, size_lim, db_user, db_host, db_port, db_name, db_pwd, env_name) #BAD or OK

				if status.find('ERROR') == -1:
					message = 'Success processing {}'.format(folder)
					log.debug(message)
					exit_status=0  #all good
					s1=time.time()
					log.info('processing time for {0}: {1:0.3f} min'.format(folder, (s1-s0)/60 ) )
					results.append([folder, status])
					log.debug('-'*20)
				else:
					message = 'Failure processing {}'.format(folder)
					log.debug(message)
					s1 = time.time()
					log.info('processing time for {0}: {1:0.3f} min'.format(folder, (s1-s0)/60 ) )
					results.append([folder, status])
					exit_status = 1
					log.debug('-'*20)
			except Exception as e:
				log.exception("ERROR: Exception processing folder {}: {} ".format(folder, e))
				exit_status = 1
				log.debug('-'*20)
	if exit_status == 0:
		for i in results:
			log.info(i)

	log.info('Exiting with status: {}'.format(exit_status))
	sys.exit(exit_status)



if __name__== "__main__":
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	global DEBUG
	global REGION
	LOCAL = False
	AWS_CREDENTIALS_PROFILE = 'silver'
	REGION = 'us-east-2'
	main()
