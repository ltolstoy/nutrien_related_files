#!/usr/bin/python
# Script to concatenate all json files in the s3 folder into one, saved locally, to create schema from it for the hive table
#v1 - 11/21/18 - use only master node to copy and concat files
#v2 11/30/18 - use spark parallelize to read files
#v3 - 12/3/18 - use boto3 to write output file to s3 via https://stackoverflow.com/questions/40336918/how-to-write-a-file-or-data-to-an-s3-object-using-boto3
#v4 - 12/10/18 - use chunks for big datasets, as I got memory error, joined file is too big. Also use new 
# make_file_list_s4cmd_uni -universal version, for cases where there are sub-folders there instead of json files
#v5 - 12/20/18 - add Hive table drop and re-create part, added ETE for chunks, changed get_content_s3 (add more attempts
#  to read file if no success 1st time)
#v6 - 15/1/19 - change get_content_s3 to limit attempts, as it hangs sometimes if can't read some file from s3, and write to log a list of failed read jsons  
# via https://stackoverflow.com/questions/40806225/pyspark-logging-from-the-executor
#read those log files with 'yarn logs -applicationId .....' pyspark.log
# use get_size from https://goshippo.com/blog/measure-real-size-any-python-object/ to check the size of chunks before 
# writing it to s3 (due to 5gb limit of boto)
#v7 - 1/28/19 - created make_file_list_boto (only boto3), change make_file_list_s4cmd_boto_uni, now use boto3 (seems to be even faster than s4cmd!), but aws s3 ls for subfolders,
# and changed get_content_s3 to get_content_boto3. Problem with prefix used in make_file_lists4cmd_boto_uni, it appends extra files (with same prefix)
# need '/' at the end of prefix
#v8 - use get_content_boto3 to collect json files, as it's faster


import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext
#from pyspark.sql.functions import *
from pyspark.sql import HiveContext, SparkSession
import sys  #for arguments
import numpy as np
import datetime
import time, os
import json
import subprocess32 as sp
import pandas as pd
import boto3
from botocore.client import Config
import logging
import socket 
import ptvsd

#ptvsd.enable_attach()
#ptvsd.wait_for_attach()


def chunkIt_with_sizes(files,sizes, lim):
	#Splits filelist, sizelist into parts with sizes ~ lim
	#files: list of file names
	#sizes: list of corresponding file sizes
	#return: list of few lists of files and list of few lists of sizes
	cur =0 #current position
	out = []
	out_s = []
	sm = 0
	start, stop = 0 , 0
	while stop < len(files):
		start = cur
		while (sm <= lim) & (stop < len(files)):
			sm += sizes[cur]
			cur+=1
			stop = cur
			#print(sm,start,stop)
		if stop <= len(files):
			out.append(files[start:stop])
			out_s.append(sizes[start:stop])
		else:
			out.append(files[start:stop-1])
			out_s.append(sizes[start:stop-1])
		
		#print('start {}, stop {}'.format(start, stop) )
		start = cur
		sm = 0 # drop sum for new loop
	return out, out_s 


def t_report(text,s0,s1):
	#To count the time the current operation took, in sec, min
	#text - message like "Reading part took "
	#s0, s1 - time.time() output
	print("{0}: {1:.1f}s or {2:.1f}m\n".format(text, s1-s0, (s1-s0)/60 ))


def get_matching_s3_objects(bucket, prefix='', suffix=''):
	""" via https://alexwlchan.net/2018/01/listing-s3-keys-redux/
	Generate objects in an S3 bucket.
	:param bucket: Name of the S3 bucket, like 'data-lake-us-east-2-549323063936-encrypted'
	:param prefix: Only fetch objects whose key starts with this prefix (optional), like 'Agrian/AdHoc/NoETL/attachment/'
	note trailing '/', it's important for cases like 'crop_plan_x_x' and 'crop_plan'
	:param suffix: Only fetch objects whose keys end with this suffix (optional), like 'json'
	"""
	#config = Config(connect_timeout=5, read_timeout=5, retries={'max_attempts': 0})
	#s3 = boto3.client('s3',config=config)

	s3 = boto3.client('s3')
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


def make_file_list_boto(folder):  
	# Function that queries s3 to get file size list and file names list from path p , folder folder
	# As I know the bucket to look for files, and prefix
	#folder: like "grower"
	#return: list of file names , list of file sizes (int)
	print('{}: using make_file_list_boto function'.format(folder))
	bucket='data-lake-us-east-2-549323063936-encrypted'
	pref='Agrian/AdHoc/NoETL/' # so full path is bucket+'/' + prefix + folder

	file_list = []
	#date_list = []
	size_list = []
	p = pref+folder+'/' # like Agrian/AdHoc/NoETL/grower/'. NOTE that '/' is important for correct prefix, othervise filed_event with have filed_event_layer files too! (as prefix is the same, without '/')
	for item in get_matching_s3_objects(bucket, prefix=p, suffix='json'): #bcs yield in get_matching_s3_objects
		path_as_list = item['Key'].split('/') #item['Key'] looks like 'Agrian/AdHoc/NoETL/list/xxx.json'

		if len(path_as_list) == 6: #case of attachment or field_event_layer, where there are subfolders
			file_list.append(path_as_list[4] +'/'+path_as_list[5]) #append subfolder/fname for attachment and field_event_layer
			size_list.append(item['Size'])              #size of file
		elif len(path_as_list) == 5: # there are json files in 'Agrian/AdHoc/NoETL/', need to avoid those! So only go if there is 'Agrian/AdHoc/NoETL/folder/'
			file_list.append(path_as_list[4])              #append only fname
			size_list.append(int(item['Size']))              #size of file
		#date_list.append(item['LastModified'])     #date 

	return file_list, size_list


def make_file_list_s4cmd_boto_uni(folder):  
	#Function that queries s3 to get file size and file names from folder folder
	#Even in case there are not files but sub-folders with tiff, jpeg, zip and json (attachment and field_event_layer) 
	# Need to re-read the whole list again, to get file size
	#p: path like "s3n://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/grower/*.json"
	#folder: like "grower"
	#return: list of file names , list of file sizes
	#out=sp.check_output([ 'aws', 's3', 'ls', 's3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/'+folder+'/']).split() #list
	s0 = time.time()
	path = 's3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/'+folder+'/' #for real app
	#print('{}: using make_file_list_s4cmd_uni function'.format(folder))
	#out=sp.check_output([ '/usr/local/bin/s4cmd', 'ls', path]).split() #list
	df=pd.DataFrame(columns=['date','time','size','fname'])
	if folder == 'attachment' or folder == 'field_event_layer' : 
		print('{}: using make_file_list_s4cmd_boto_uni function, aws s3 ls with grep way'.format(folder))
		#out[0] == b'DIR':  #mean there are sub-folders with 2-3 different files instead of just files
		#out=sp.check_output([ '/usr/local/bin/s4cmd', 'ls', path]).split() #list
		s1 = time.time()
		#print('{} - Special situation for {} - there are sub-folders instead of json files there. Re-reading with | grep json'.format(str(datetime.datetime.now()).split('.')[0], path ) )
		#t_report('initial info request took',s0,s1)
		s4proc = sp.Popen(['aws','s3', 'ls', path, '--recursive'],
					 stdout=sp.PIPE) #part defore grep
		grepproc = sp.Popen(['grep','.json'],
						stdin = s4proc.stdout,
						stdout=sp.PIPE)
		out2, _ = grepproc.communicate()  #tuple ('s1\ns2\ns3\n...\n', None)
		out1 = out2.split() #split on \n and whitespace
		#out1=sp.check_output([ '/usr/local/bin/s4cmd.py', 'ls', path,'--recursive','|grep','json']).split() #in subfolders there are 2+ files!
		df['date']=out1[0::4]
		df['time']=out1[1::4]
		df['size']=out1[2::4]
		df['fname']=out1[3::4]

		df['fname']=df['fname'].apply(lambda x: x.split('/')[-2] + '/' + x.split('/')[-1] ) #extracts only folder/filename from full path, when use s4cmd
		df['size']=df['size'].apply(pd.to_numeric)
		df['datetime']=df['date'] +' '+ df['time']
		df['datetime']=df['datetime'].apply(pd.to_datetime)
		df = df.drop(['date', 'time'], axis=1)
		
		file_list, size_list = df['fname'].tolist(), df['size'].tolist()
		size_list = [int(x) for x in size_list]
		s2 = time.time()
		t_report('info extraction from subfolders took',s1,s2)			
	else:  #case of normal folder
		"""out=sp.check_output([ '/usr/local/bin/s4cmd', 'ls', path]).split() #list
		df['date']=out[0::4]
		df['time']=out[1::4]
		df['size']=out[2::4]
		df['fname']=out[3::4]
		df['fname']=df['fname'].apply(lambda x: x.split('/')[-1]) #extracts only filename from full path, when use s4cmd
		df['size']=df['size'].apply(pd.to_numeric)
		df['datetime']=df['date'] +' '+ df['time']
		df['datetime']=df['datetime'].apply(pd.to_datetime)
		df = df.drop(['date', 'time'], axis=1)"""
		#try to use boto, it seems to be faster!
		print('{}: using make_file_list_s4cmd_boto_uni function, boto3 way'.format(folder))
		bucket='data-lake-us-east-2-549323063936-encrypted'
		pref='Agrian/AdHoc/NoETL/' # so full path is bucket+'/' + prefix + folder

		file_list = []
		#date_list = []
		size_list = []
		p = pref+folder+'/' # like Agrian/AdHoc/NoETL/grower/'. NOTE that '/' is important for right prefix, othervise filed_event with have filed_event_layer files too! (as prefix is the same, without '/')
		for item in get_matching_s3_objects(bucket, prefix=p, suffix='json'): #bcs yield in get_matching_s3_objects
			path_as_list = item['Key'].split('/') #item['Key'] looks like 'Agrian/AdHoc/NoETL/list/xxx.json'

			if len(path_as_list) == 6: #case of attachment or field_event_layer, where there are subfolders
				file_list.append(path_as_list[4] +'/'+path_as_list[5]) #append subfolder/fname for attachment and field_event_layer
				size_list.append(item['Size'])              #size of file
			elif len(path_as_list) == 5: # there are json files in 'Agrian/AdHoc/NoETL/', need to avoid those! So only go if there is 'Agrian/AdHoc/NoETL/folder/'
				file_list.append(path_as_list[4])              #append only fname
				size_list.append(int(item['Size']))              #size of file
			#date_list.append(item['LastModified'])     #date 
		
	
	return file_list, size_list #filtered by exclude_some_files()



def save_lists(folder, file_list, size_list):
	#save file names and sizes in local file, for faster processing (as field-event file info collection takes 21 min, even with s4cmd!)
	fn='/home/ltolstoy/scripts/'+folder+'_file_list.txt'
	with open(fn,'w') as f:
		f.write(','.join(file_list)) #saving file with filenames, separated by ','
	
	fns='/home/ltolstoy/scripts/'+folder+'_size_list.txt'
	with open(fns,'w') as f1:
		f1.write(','.join(str(x) for x in size_list)) #saving file with file sizes as string
		

def read_lists_from_files(folder):
	#I saved file_list and size_list as ',' separated list in folder_file_list.txt and folder_size_list.txt
	fn='/home/ltolstoy/scripts/'+folder+'_file_list.txt'
	with open(fn,'r') as f:
		file_list=f.read().split(',')
	fns='/home/ltolstoy/scripts/'+folder+'_size_list.txt'
	with open(fns,'r') as f1:
		size_list=f1.read().split(',')
	return file_list, size_list

def get_content_s3(iterator):
	#Reads from s3 files in list (iterator)
	#iterator: list of files on s3 to read
	#lim: number of read file attempts
	#returns: list of json files contents
	#via https://stackoverflow.com/questions/2083987/how-to-retry-after-exception
	result = []
	lim = 5
	global logger
	host = socket.gethostname()
	for x in iterator:
		#for i in range(lim):  #limit read attempts to 'lim'
		i=0
		while i<lim : #was while True, unlimited number of attempts - not good
			i+=1
			try:
				content = sp.check_output([ 'aws', 's3', 'cp', x, '-']) # content of the file. x here is a full path: 's3://data..../.../NoETL/../193.json'
				#print(content)#content = '{"id":1, "name::"Aaa"}'  str
				result.append(content) #Note: result is a list of contents of json files
				#logger.info(" from {}:  SUCCESSFULLY read file {} on attempt {}".format(host,x,i))
			except: # leave log error message
				#result.append('error_reading_file {}'.format(x))
				logger.error(" from {}:  error while reading file {}, attempt {}".format(host,x,i)) #for log file
				continue
			break #break from while loop if file read succeed
	return [result]

def get_content_boto3(iterator):
	#Reads from s3 files in list (iterator). Need to get bucket from x (from iterator)
	#iterator: list of files on s3 to read: 
	#lim: number of read file attempts
	#returns: list of json files contents
	#via https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
	# and https://stackoverflow.com/questions/41263304/s3-connection-timeout-when-using-boto3
	import boto3
	#from botocore.client import Config

	#config = Config(connect_timeout=5, read_timeout=5, retries={'max_attempts': 0})

	#s3 = boto3.client('s3', config=config)
	s3 = boto3.resource('s3')

	result = []
	for x in iterator:
		x_split = x.split('/')
		bucket = x_split[2]  #'data-lake-us-east-2-549323063936-encrypted'
		path = '/'.join(x_split[3:]) #'Agrian/AdHoc/NoETL/list/8.json'
		obj = s3.Object(bucket, path)
		try:
			content = obj.get()['Body'].read() #.decode('utf-8')
			result.append(content) #Note: result is a list of contents of json files
		except Exception as e:
			logger.error("get_content_boto3 got error with {}:\n{}".format(x, e))
	return [result]



def drop_old_create_new_table(sqlContext, folder):
	#Func to drop existing Hive table, and create new, based on new file just uploaded to s3
	#Create table statements are saved already on local fs, just need to read corresponding one.
	#sqlContext - context ceated before
	#folder - like 'farm'
	#returns: message describing how folder was processed: 'Ok','Failed to drop table','Failed to create new table'
	p2statement = '/home/ltolstoy/scripts/'+folder+'_create_table.txt'
	table_name = folder+'_from_json'
	message = 'Ok'
	if os.path.exists(p2statement):
		with open(p2statement) as f:
			statement = f.read().strip()  #statement - all we need to create Hive table for folder. Remove trailing '\n'
			statement = statement.replace('`','\`')	# need to substitute backticks with '\`' in create table statement
			if statement[-2:] == ';\n':  #need to delete last ';' for sql context (but need ';' for regular Hive queries)
				statement = statement[:-2]
			elif statement[-1] == ';':  #need to delete last ';' for sql context (but need ';' for regular Hive queries)
				statement = statement[:-1]  #leave all but last symbol ';'
		
			if table_name in sqlContext.tableNames() : #check if table exists, and drop it
				print('{} : {} is in list of tables in DB. Now droping it'.
				format(str(datetime.datetime.now()).split('.')[0], 
				table_name))
				q='hive -S -e "DROP TABLE IF EXISTS '+ table_name+';"' #hive -e "drop table if exists farm_water_source_from_json;"
				#print("Executing {}".format(q))
				try:
					os.system(q)
					message = 'Ok'
				except Exception as e:
					print("Can't drop table {}: {}".format(table_name, e))
					message = 'Failed to drop table ' + table_name

			else:
				print('{}: {} is NOT in list of tables'.
				format(str(datetime.datetime.now()).split('.')[0], 
				table_name))

		#now create table
		try:
			#sqlContext.sql(statement) # gives an error for tables with 'agrian.xxxxx', even with backticks around!
			q='hive -S -e "'+ statement+'"'
			#print("Executing statement:\n {}".format(q))
			os.system(q)
		except Exception as e:
			print('Error while creating table {}: {}'.format(table_name, e))
			message = 'Failed to create new table ' + table_name
	else:
		print('There is no statement file {}, so cant create new Hive table, leaving old table unchanged'.format(p2statement))
		message='No statement file found! Left table unchanged.'
	print("drop_old_create_new_table  completed for {}. {}".format(folder, message))
	return message 

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

def concatenation_parallel(ps3, p, folder):
	#Function that concatenates the data from json files in path p , folder folder, to one big file stored locally
	#ps3: path to subfolder "s3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/"
	#p: path like "s3n://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/grower/*.json"
	#folder: like "grower"
	#return: message from drop_old_create_new_table() about the status of the folder update
	s0=time.time()
	warehouseLocation = "file:${system:user.dir}/spark-warehouse"
	apname = "concat_"+folder
	#conf = SparkConf().set("spark.ui.showConsoleProgress", "false") #to avoid ConsoleProgressBar output, for out.txt file
	SS = (SparkSession.builder
	.appName(apname)
	.config("spark.ui.showConsoleProgress", "false")
	.config("spark.sql.warehouse.dir", warehouseLocation)
	.config("spark.rpc.message.maxSize","384")
	.config("spark.port.maxRetries","40")
	.enableHiveSupport().getOrCreate())
	global logger
	SS.sparkContext.addPyFile('hdfs:///user/ltolstoy/logger.py')  #via https://stackoverflow.com/questions/40806225/pyspark-logging-from-the-executor
	import logger
	logger = logger.YarnLogger()
	sc = SS.sparkContext

	#sqlContext = SQLContext(SS)
	sqlContext = HiveContext(SS)
	
	file_list, size_list = make_file_list_boto( folder) #this is faster than _s4cmd_boto_uni even for attachment and field_event_layer
	#file_list, size_list = make_file_list_s4cmd_boto_uni( folder)  #for real processing, comment out for debug
	#save_lists(folder, file_list, size_list)  #to speed-up debug, save all file names and sizes in files
	#print('Saved file_list and size_list in local files')
	#sys.exit()
	#file_list, size_list = read_lists_from_files(folder) #for saved files, it's faster for debug
	#size_list = [int(x) for x in size_list]  #need to convert from strings back to int values
	#print('Read file_list and size_list from local files')
	#sys.exit()
	s1=time.time()
	t_report(folder+": getting all files info took", s0,s1)
	total_size=sum(size_list)
	print('{}: Found total files {}, with total size {} bytes'.format(folder, len(file_list), total_size) )
	#content_func='get_content_s3' # or 'get_content_boto3' # Idea is to use the same function for chunks and full file
	
	size_lim = 1.5e9 #1.5 Gb
	if total_size > size_lim: #if total_size>1.5Gb
		chunks, size_chunks = chunkIt_with_sizes(file_list, size_list, size_lim)
		chunks_num = len(chunks) # here I get number of chunks provided by function, not calculated by myself
		print('{}: total_size is larger than {}bytes, so splitting the whole data into {} chunks'.format(folder, 
		size_lim, chunks_num))

		for i, ch in enumerate(chunks): #ch is an array of file names ['0.json','1,json']
			size_chunk_cur = sum(size_chunks[i] ) #size of files in current chunk
			print('chunk {}/{} of {}: there are {} files in chunk, chunk size = {} bytes'.format( i+1,chunks_num,
			 folder, len(ch), size_chunk_cur ))
			print('chunk {}/{} of {}: biggest file {} bytes , smallest file {} bytes, average file size {} bytes'.format(i+1, 
			chunks_num,
			folder,
			max(size_chunks[i]), 
			min(size_chunks[i]), 
			int(np.mean(size_chunks[i])) ))
			s2=time.time()
			full_path_chunk=[ps3 + folder+ '/'+ f for f in ch]
			n_parts = np.maximum(int(1+ size_chunk_cur/(50000*1)), int(1+ len(ch)/10) )  #min of size_chunk_cur/(50k) or len(chunk_file_list)/10
			#Note: max() or min() fails sometimes, I think because it uses sql functions (instead of embedded max, min, etc). So need to say np.max specificaly

			print("chunk {}/{} of {}: Start parallelize into {} partitions".format(i+1,
			chunks_num, folder,n_parts))
			rdd_f = sc.parallelize(full_path_chunk, n_parts )  
			print("chunk {}/{} of {}: Done parallelize".format(i+1, chunks_num,folder))
			s3=time.time() 
			t_report("\tparallelize files from full_path_chunk took", s2,s3)
			print("\t{}: Starting map part".format(folder))
			#parts=rdd_f.mapPartitions(get_content_s3).collect()  #list of json files content [[{},{}],[{},{}],...]
			parts=rdd_f.mapPartitions(get_content_boto3).collect()
			print("\t{}: Done map part".format(folder))
			s4=time.time()
			t_report("\t{}: parts collection took".format(folder), s3,s4)

			all_files_chunk = [item for sublist in parts for item in sublist]
			if len(all_files_chunk) != len(ch):
				print('chunk {}/{}: Collected {} of {} files'.format(i+1, 
				chunks_num,
				len(all_files_chunk), 
				len(ch)))
			ss3 = boto3.resource('s3')
			bucket_name = 'data-lake-us-east-2-549323063936-validated'
			fpath = 'hive/tables/'+folder+'_all/'+folder+'_all_' + str(i) + '.json'   #save chunk i into validated bucket, folder_all folder
			object = ss3.Object(bucket_name, fpath)
			body = '\n'.join(all_files_chunk)
			sz = get_size(body)
			print('get_size of chunk {}/{} shows {} bytes for {} lines, compare to {} bytes (added {})'.format(i+1,
			chunks_num,
			 sz , 
			 len(all_files_chunk), 
			 size_chunk_cur, 
			 sz-size_chunk_cur))
			try:
				object.delete() #first delete old file with the same name
				object.put( Body= body) #then write new one
			except Exception as e:
					print('Error with object.put file {}: {}'.format(fpath, e)) 
			s5=time.time()
			print('\tdone writing joined chunk {} file of {}'.format(i+1, folder) )
			t_report("writing chunk took",s4,s5)
			t_report(folder+": total time for chunk is ",s2,s5)
			print('{0} {2}  ETE is {1:.1f} min\n'.format(folder, (chunks_num - i-1)*(s5-s2)/60, str(datetime.datetime.now()).split('.')[0] ) ) 
		print('Done with all {} chunks for {}'.format(i+1, folder)) 
		logger.info('Done with all {} chunks for {}'.format(i+1, folder))

	else:  # total_size is smaller that size_lim
		n_parts = np.minimum(int(1+ total_size/50000), int(1+ len(file_list)/10) )
		#n_parts=2
		print('{}: parts number {}'.format(folder, n_parts))
		full_path_list=[ps3 + folder+ '/'+ f for f in file_list] #creates list like ['s3://data../NoETL/55.json', 's3://data./NoETL/193.json']
		rdd_f = sc.parallelize(full_path_list, n_parts )  #parallelize list of files into 64cores*3 partitions
		print(folder+": Done parallelize")

		print(folder+ ": Starting map part ")
		#parts=rdd_f.mapPartitions(get_content_s3).collect()
		parts=rdd_f.mapPartitions(get_content_boto3).collect()  #list of json files content [[{},{}],[{},{}],...]
		print(folder+ ": Done map part ")
		s2=time.time()
		t_report("\t"+folder+" parts collection took", s1,s2)

		all_files = [item for sublist in parts for item in sublist]  #making single list instead of (list of lists). Flattening into 1 list
		if len(all_files) != len(file_list): #check if all files were read, or some are missing
			print('Collected {} of {} files'.format(len(all_files), len(file_list)))
		s3=time.time()
		print("\t"+folder+" done reading files")
		t_report("\t"+folder+" reading took",s0,s3)
		
		ss3 = boto3.resource('s3')
		bucket_name = 'data-lake-us-east-2-549323063936-validated'
		fpath = 'hive/tables/'+folder+'_all/'+folder+'_all.json'   #save into validated bucket, folder_all folder
		object = ss3.Object(bucket_name, fpath)
		body = '\n'.join(all_files)
		sz = get_size(body)
		print('{} get_size shows {} bytes for {} lines, compare to {} bytes (added {})'.format(folder, sz, len(all_files), total_size, sz-total_size))
		try:
			object.put( Body=body )
		except Exception as e:
				print('Error with object.put file {}: {}'.format(fpath, e)) 
		s4=time.time()
		print("\t"+folder+" done writing joined file for {}".format(folder))
		t_report("\t"+folder+" writing took",s3,s4)
		logger.info("done writing joined file for {}".format(folder))
	#now need to drop old Hive table and create new
	time.sleep(10)
	message = drop_old_create_new_table(sqlContext, folder)
	return message


def main():
	#For every folder provided as argument, run the concatenation procedure:
	# read bunch of json files from current folder, and save as one big file on local fs
	# to create schema with 
	#java -jar /Users/ltolstoy/Downloads/scripts/hive_query/hive-json-schema/target/json-hive-schema-1.0-jar-with-dependencies.jar
	print("--------------Start time ----------{}".format(str(datetime.datetime.now()).split('.')[0] ) )
	ps3 = "s3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/" #path to root bucket on s3
	s00 = time.time()
	#list of folders to include into processing
	folders2process = [] # obtained with " aws s3 ls s3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/" command
	#print "This is the name of the script: ", sys.argv[0]
	#print "Number of arguments: ", len(sys.argv)
	#print "The arguments are: " , str(sys.argv)
	#sys.exit()
	scriptname=sys.argv[0]
	if len(sys.argv)==1:
		print("Please run it with folder (or folders) as argument: ./concat_jsons_vX.py folder folder2")
		sys.exit()
	else:
		folders2process = sys.argv[1:] #exclude 1st=script name
		print("Got folders to process: {}".format(folders2process))
		#sys.exit()
	results = [] #list of messages for each folder
	for folder in folders2process:
		p = ps3 + folder  #current full path to folder to process, like "s3n://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/grower/"

		try:
			s0=time.time()
			print("{} Working with {} ".
			format(str(datetime.datetime.now()).split('.')[0], p))
			status = concatenation_parallel(ps3, p, folder) #BAD or OK
			if status == 'Ok':
				s1=time.time()
				t_report("-------"+folder+" processing took",s0,s1)
				results.append([folder, status])
			else:
				print("concatenation return status BAD")
				s1=time.time()
				t_report("-------"+folder+" processing took",s0,s1)
				results.append([folder, status])
		except:
			print("{} Something went wrong with folder {} ".format(str(datetime.datetime.now()).split('.')[0], folder))
	#end of script
	s11=time.time()
	#print ("{} --------------------- All folders Done".format( str(datetime.datetime.now()).split('.')[0]  ))
	for i in results:
		print(i) 
	#t_report("------- All folders processing took",s00,s11)

if __name__== "__main__":
		main()
