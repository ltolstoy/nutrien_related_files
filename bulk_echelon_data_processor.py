# Script to process all existing files in s3, for specified folder, and put it into Postgres DB.
# Should distinguish obfuscated data (for dev, sit, pre environments for farm,field, grower folders) and real data
# for prd env.
#
# 6/28/19 -  add ability to restore triggers in Postgres DB, after drop tables (farm, field, grower) - lines 340-351
# 8/20/2019 - special version with db password hardcoded, to upload (emergency) some datasets to db. line 281
# disabled recreating triggers part at line 355
# 9/23/2019 - with recreating Disabled for triggers (lines 357-369), and without db password hardcoded as it works now (line 280).
# Works in all environments (DEV, SIT, PRE, PRD)
# 9/24/19 - version for SIT with hardcoded pwd, rewritten parameters (need only environment), distinguish obf/real data path
# 10/25/2019 - regular version, no hack, distinguish obf/real data path
# 2/18/2020 - update Echelon Bulk processes to set the updated_at column
# 4/3/2020 - added switch to obfuscated_data foldere for organization too, line 909
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext, SparkSession
import sys  # for arguments
import numpy as np
import pandas as pd
import datetime
import time
import os
import io
import json
import boto3
from datetime import datetime

from sqlalchemy import create_engine, text
import argparse


def logging_prefix():
    return datetime.now().strftime('%m-%d-%Y %H:%M:%S.%f')


def log_processing_time(process, start, end):
    if end - start < 60:
        print('{0}: processing time for {1}: {2} seconds'.format(
            logging_prefix(), process, round(end - start)))
    else:
        print('{0}: processing time for {1}: {2} minutes'.format(
            logging_prefix(), process, round((end - start)/60)))


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
        if DEBUG:
            log('DEBUG: getting ssm client using local credentials')
        try:
            session = boto3.Session(
                profile_name=AWS_CREDENTIALS_PROFILE, region_name=REGION)
            ssm = session.client('ssm')
        except Exception as e:
            log('ERROR: exception getting SSM client. {}'.format(e))
            raise e
    else:
        if DEBUG:
            log('DEBUG: about to make boto3.client(\'ssm\') invocation in non-LOCAL environment')
        try:
            ssm = boto3.client('ssm', region_name=REGION)
        except Exception as e:
            log('ERROR: exception getting SSM client. {}'.format(e))
            raise e
    log('exiting get_ssm_client')
    return ssm


def get_bucket_and_key(file_uri):
    file_uri_parts = file_uri.split('/')
    # e.g. 'data-lake-us-east-2-549323063936-internal'
    bucket = file_uri_parts[0]
    key = '/'.join(file_uri_parts[1:])  # e.g. 'ars/product/DEV/USA/blah.json'
    return bucket, key


def chunkIt_with_sizes(files, sizes, lim):
    """
    Splits filelist, sizelist into parts with sizes ~ lim
    files: list of file names
    sizes: list of corresponding file sizes
    return: list of few lists of files and list of few lists of sizes
    """
    cur = 0
    out = []
    out_s = []
    sm = 0
    start, stop = 0, 0
    while stop < len(files):
        start = cur
        while (sm <= lim) & (stop < len(files)):
            sm += sizes[cur]
            cur += 1
            stop = cur
        if stop <= len(files):
            out.append(files[start:stop])
            out_s.append(sizes[start:stop])
        else:
            out.append(files[start:stop-1])
            out_s.append(sizes[start:stop-1])

        sm = 0  # drop sum for new loop
    return out, out_s


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


def make_file_list_boto(folder):
    """
    Function that queries s3 to get file size list and file names list from path p , folder folder
    As I know the bucket to look for files, and prefix
    folder: like "grower"
    return: list of file names , list of file sizes (int)
    """
    log('{}: using make_file_list_boto function'.format(folder))
    bucket = 'data-lake-us-east-2-549323063936-encrypted'
    pref = 'Agrian/AdHoc/NoETL/'  # so full path is bucket+'/' + prefix + folder

    file_list = []
    size_list = []
    # like Agrian/AdHoc/NoETL/grower/'. NOTE that '/' is important for correct prefix, othervise filed_event with have filed_event_layer files too! (as prefix is the same, without '/')
    p = pref+folder+'/'
    for item in get_matching_s3_objects(bucket, prefix=p, suffix='json'):
        # item['Key'] looks like 'Agrian/AdHoc/NoETL/list/xxx.json'
        path_as_list = item['Key'].split('/')

        if len(path_as_list) == 6:  # case of attachment or field_event_layer, where there are subfolders
            # append subfolder/fname for attachment and field_event_layer
            file_list.append(path_as_list[4] + '/'+path_as_list[5])
            size_list.append(item['Size'])  # size of file
        elif len(path_as_list) == 5:  # there are json files in 'Agrian/AdHoc/NoETL/', need to avoid those! So only go if there is 'Agrian/AdHoc/NoETL/folder/'
            file_list.append(path_as_list[4])  # append only fname
            size_list.append(int(item['Size']))  # size of file

    return file_list, size_list


def make_file_list_db(folder, connection):
    # Function that requests file info from echelon_filesinfo (format: fname, size, date, updated_at) instead of directly quering s3, which takes long time
    # folder: like "grower"
    # connection - previously created
    # returns: file_list- list of file names (including subfolder for attachment and field_event_layer), size_list - corresponding file sizes
    log('Entering make_file_list_db')
    table_n = 'echelon_filesinfo.'+folder
    q = 'select fname, size from '+table_n+';'  # our query
    try:
        df = pd.read_sql_query(q, connection)
    except Exception as e:
        print("Exception in make_file_list_db: {}".format(e))
    file_list = df['fname'].tolist()
    size_list = df['size'].tolist()
    # date_list = df['date'].tolist() #list of pandas.timestamp object, which is eqivalent of python datetime, and is interchangeable with it in most cases
    log('Exiting make_file_list_db with {} records in file_list'.format(len(file_list)))
    return file_list, size_list  # , date_list


def get_content_boto3(iterator):
    """
    Reads from s3 files in list (iterator). Need to get bucket from x (from iterator)
    iterator: list of files on s3 to read:
    lim: number of read file attempts
    returns: list of json files contents
    via https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
     and https://stackoverflow.com/questions/41263304/s3-connection-timeout-when-using-boto3
    """
    log('Entering get_content_boto3')
    s3 = get_s3_resource()
    result = []
    for x in iterator:
        if DEBUG:
            log('DEBUG: getting contents of file {}'.format(x))
        # x is like 's3://data-lake-us-east-2-549323063936-encrypted/Agrian/obfuscated_data/grower/xx.json'
        x_split = x.split('/')
        # e.g. 'data-lake-us-east-2-549323063936-encrypted'
        bucket = x_split[2]
        # e.g. 'Agrian/AdHoc/NoETL/list/8.json' or 'Agrian/obfuscated_data/grower/xx.json'
        path = '/'.join(x_split[3:])
        obj = s3.Object(bucket, path)
        attempts = 0
        max_attempts = 5
        while attempts < max_attempts:
            try:
                content = obj.get()['Body'].read().decode('utf-8')
                if DEBUG:
                    log('DEBUG: retrieved the body of the file')
                # Note: result is a list of contents of json files, distributed to current worker
                result.append(content)
                if DEBUG:
                    log('DEBUG: appended body to results list')
                break
            except Exception as e:
                attempts += 1
                if attempts == max_attempts - 1:
                    log("ERROR: get_content_boto3 got error with {}:\n{}".format(x, e))
    log('Exiting get_content_boto3 with {} results.'.format(len(result)))
    return [result]


def get_size(obj, seen=None):  # via https://goshippo.com/blog/measure-real-size-any-python-object/
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


def get_db_info(env_name):
    # Funcion to get DB connection parameters from AWS SSM
    # env_name: string, like 'dev', 'sit' , etc - provided as EMR step argument
    # returns: db_user, db_host, db_name, db_pwd
    global DEBUG
    log('Entering get_db_info')
    log('getting SSM client so we can pull db-connection info from SSM')
    ssm = get_ssm_client()

    try:
        log('retrieving db_pwd parameter')
        n = "/1/runtime/" + env_name + "/RECONCILER_PASS"
        response = ssm.get_parameter(Name=n, WithDecryption=True)
    except Exception as e:
        log('ERROR: exception retrieving db_pwd: {}'.format(e))
    db_pwd = response["Parameter"]["Value"]

    try:
        log('retrieving db_user parameter')
        n = "/1/runtime/" + env_name + "/RECONCILER_USER"
        response = ssm.get_parameter(Name=n, WithDecryption=True)
    except Exception as e:
        log('ERROR: exception retrieving db_user: {}'.format(e))
    db_user = response["Parameter"]["Value"]

    try:
        log('retrieving db_host parameter')
        n = "/1/runtime/" + env_name + "/RECONCILER_HOST"
        response = ssm.get_parameter(Name=n, WithDecryption=True)
    except Exception as e:
        log('ERROR: exception retrieving db_host: {}'.format(e))
    db_host = response["Parameter"]["Value"]

    try:
        log('retrieving db_name parameter')
        n = "/1/runtime/" + env_name + "/RECONCILER_DB"
        response = ssm.get_parameter(Name=n, WithDecryption=True)
    except Exception as e:
        log('ERROR: exception retrieving db_name: {}'.format(e))
    db_name = response["Parameter"]["Value"]
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


def get_db_connection(db_user, db_host, db_port, db_name, db_pwd):
    # Funcion to establish connection to Postgres db.
    # returns: engine, connection, cursor
    global DEBUG
    log('Entering get_db_connection')
    #log('getting SSM client so we can pull db-connection password from SSM')
    #ssm = get_ssm_client()
    #log('retrieving ssm_pw_key parameter')
    # try:
    #	response = ssm.get_parameter(Name=ssm_pw_key, WithDecryption=True)
    # except Exception as e:
    #	log('ERROR: exception retrieving ssm_pw_key. {}\nBut continue with hardcoded pwd'.format(e))
    #	#raise e  # for v4 emergency
    #db_pwd = response["Parameter"]["Value"]

    address = 'postgresql://'+db_user+':'+db_pwd+'@'+db_host+':'+db_port+'/'+db_name
    log('postgresql connection string: {}'.format(
        'postgresql://'+db_user+':*****@'+db_host+':'+db_port+'/'+db_name))

    try:
        # getting postgres engine, connection, cursor
        log('getting postgres engine')
        engine = create_engine(address)
    except Exception as e:
        log('Exception in recreate_table for engine : {}'.format(e))
        log('Exiting re-create table in ERROR state')
        return 'Bad'
    try:
        # getting postgres engine, connection, cursor
        log('getting postgres connection')
        connection = engine.raw_connection()
    except Exception as e:
        log('Exception in recreate_table for connection: {}'.format(e))
        log('Exiting re-create table in ERROR state')
        return 'Bad'

    try:
        log('getting postgres cursor')
        cursor = connection.cursor()
    except Exception as e:
        log('Exception in recreate_table for cursor: {}'.format(e))
        log('Exiting re-create table in ERROR state')
        return 'Bad'
    log('successful creation of postgres engine, connection, cursor')
    return engine, connection, cursor


def recreate_postgres_table(folder, engine, connection, cursor):
    """
    Function to drop and re-create  Postgresql db table for folder, preparing for new data
    folder: name of the folder, like 'grower', for table naming
    engine, connection, cursor - created in get_db_connection
    """
    global DEBUG
    log('Entering recreate_postgres_table')

    tables_dict = {'grower', 'farm', 'field',
                   'country', 'county', 'state', 'organization'}
    table_name = folder
    # this form is for queries , otherwise doesn't work. 'echelon_in.field'
    table_n = 'echelon_in.'+table_name
    # If table don't exist, create it
    if not engine.dialect.has_table(engine, table_name, schema='echelon_in'):
        log('table {} doesn\'t exist; going to create it'.format(table_n))
        if table_name in tables_dict:
            q = """CREATE TABLE """+table_n+""" (
		id TEXT NOT NULL PRIMARY KEY,
		json jsonb, appended_at TIMESTAMP, updated_at TIMESTAMP, nutrien_id UUID, npi_reconciled_at TIMESTAMP, is_rejected_by_npi BOOLEAN);"""
        else:
            q = """CREATE TABLE """+table_n+""" (
		id TEXT NOT NULL PRIMARY KEY,
		json jsonb, appended_at TIMESTAMP);"""
        log('table {} did not exist, creating it'.format(table_name))
        if DEBUG:
            log('DEBUG: Table-creation query: {}'.format(q))
        try:
            engine.execute(q)
            log('Successfully created {}'.format(table_n))
        except Exception as e:
            log('Exception in recreate_table for {}: {}'.format(folder, e))

    else:  # table exist, drop and create
        log('Table {} exists; dropping and recreating'.format(table_n))
        q = """DROP TABLE """+table_n+""";"""
        if DEBUG:
            log('DEBUG: Table-drop query: {}'.format(q))

        try:
            engine.execute(q)
            log('Successfully dropped {}'.format(table_n))
        except Exception as e:
            log('Exception in recreate_table for {}: {}'.format(folder, e))

        log('Re-creating table {}'.format(table_n))
        if table_name in tables_dict:
            q = """CREATE TABLE """+table_n+""" (
		id TEXT NOT NULL PRIMARY KEY,
		json jsonb, appended_at TIMESTAMP, updated_at TIMESTAMP, nutrien_id UUID, npi_reconciled_at TIMESTAMP, is_rejected_by_npi BOOLEAN);"""
        else:
            q = """CREATE TABLE """+table_n+""" (
		id TEXT NOT NULL PRIMARY KEY,
		json jsonb, appended_at TIMESTAMP);"""
        if DEBUG:
            log('DEBUG: Table-recreation query: {}'.format(q))
        try:
            engine.execute(q)
            log('Successfully re-created {}'.format(table_n))
        except Exception as e:
            log('Exception in recreate_table for {}: {}'.format(folder, e))

    # if folder in ['farm','field','grower']: #need to re-ceate triggers deleted when table was dropped. For now only for 3 special folders
    #	log('Re-creating trigger echelon_in_'+table_name+'_trigger for table {}'.format(table_n))
    #	q="""CREATE TRIGGER echelon_in_"""+table_name+"""_trigger
    #	AFTER INSERT OR UPDATE
    #	ON echelon_in."""+table_name+"""
    #	FOR EACH ROW
    #	EXECUTE PROCEDURE echelon."""+table_name+"""_upsert_function();"""
    #	if DEBUG: log('DEBUG: Trigger-recreation query: {}'.format(q))
    #	try:
    #		engine.execute(q)
    #		log('Successfully re-created trigger for {}'.format(table_name))
    #	except Exception as e:
    #		log('Exception in recreate_postgres_table while trying to recreate trigger for {}: {}'.format(folder, e))

    log('Exiting recreate_postgres_table')
    # return cursor , connection # for the already open connection


def write_to_postgres(all_files, folder, cursor, connection):
    """
    Function to write collected json data into Postgresql db table, in parts (to avoid freezing for ~1.5Gb chunks case)
    all_files: list of contents of collected json files ['{}','{}','{}]
    folder: name of the folder, like 'grower', for table naming
    cursor: cursor from opened in recreate_table connection
    import psycopg2
    from psycopg2.extras import Json
    via https://codereview.stackexchange.com/questions/169157/insert-data-to-postgresql
    """
    global DEBUG

    log('Entering write_to_postgres')
    # how many rows we  are planning to write to db
    rows_number = len(all_files)
    table_name = folder
    # this form is for queries , otherwise doesn't work. 'echelon_in.field'
    table_n = 'echelon_in.'+table_name
    data = []  # list of dictionaries
    empty_count = 0
    # To support data lake 2 https://agrium.atlassian.net/browse/CO-2128
    tables_dict = {'grower', 'farm', 'field',
                   'country', 'county', 'state', 'organization'}
    for raw_j in all_files:
        j = raw_j.encode('utf-8')
        if DEBUG:
            log('DEBUG: processing string {}'.format(j))
        # only process not-empty strings, by comparing str(j) to str '""', both not unicode
        if j != '""':
            try:
                tmp_dict = {"id": json.loads(j)[folder]['id'],
                            "json": j,
                            # dict is a representation of record in postgres
                            "appended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            # dict is a representation of record in postgres
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "nutrien_id": None,  # dict is a representation of record in postgres
                            "npi_reconciled_at": None,  # dict is a representation of record in postgres
                            "is_rejected_by_npi": False}  # dict is a representation of record in postgres
                data.append(tmp_dict)
                if DEBUG:
                    log('DEBUG: Successfully added tmp_dict to dictionaries list')
            except ValueError:
                log('ERROR: ValueError building json; source string was {}'.format(j))

        else:
            if DEBUG:
                log('DEBUG: empty string in all_files')
            empty_count += 1

    if DEBUG:
        log('DEBUG: Building query for insertion into postgres db')
    if table_name in tables_dict:
        query = """
				INSERT into """ + table_n + """
					("id", "json", "appended_at", "updated_at",
						"nutrien_id" , "npi_reconciled_at", "is_rejected_by_npi")
				VALUES
					(%(id)s, %(json)s, %(appended_at)s, %(updated_at)s, %(nutrien_id)s, %(npi_reconciled_at)s, %(is_rejected_by_npi)s)
				ON CONFLICT (id) DO UPDATE SET
                id=EXCLUDED.id, json=EXCLUDED.json, appended_at=EXCLUDED.appended_at, updated_at = now(), nutrien_id=EXCLUDED.nutrien_id, npi_reconciled_at=EXCLUDED.npi_reconciled_at, is_rejected_by_npi=EXCLUDED.is_rejected_by_npi;
			"""
    else:
        query = """
				INSERT into """ + table_n + """
					("id", "json", "appended_at")
				VALUES
					(%(id)s, %(json)s, %(appended_at)s)
				ON CONFLICT (id) DO NOTHING ;
				"""
    if DEBUG:
        log('DEBUG: formed the query')
        log('DEBUG: query: {}'.format(query))

    try:
        log('Executing query to write to postgres')
        cursor.executemany(query, data)
        log('Successful execution of query')
    except Exception as e:
        log('ERROR: Exception in write_to_postgres for {}: {}'.format(folder, e))
        log('Rolling back...')
        connection.rollback()
    log('Successful execution of the query; committing')
    connection.commit()
    log('Successful commit')
    if empty_count != 0:
        log('\t{} has {} empty files, skip them'.format(folder, empty_count))
    log("\tWrote to db {} rows, while all_files has {} rows, found empty {} rows".format(
        cursor.rowcount, rows_number, empty_count))
    log('Exiting write_to_postgres')


def concatenation_parallel(ps3, hive_table_creation_files_location, folder, size_lim, db_user, db_host, db_port, db_name, db_pwd, env_name):
    # Function that concatenates the data from json files in path p , folder folder, to one big file stored locally
    #
    log('Entering concatenation_parallel')
    log('using full_path_prefix={}, folder={}, env_name={}'.format(
        ps3 + folder + '/', folder, env_name))
    # put new strings here, it will be written to s3 'reports' folder after all
    report_message = []
    message = 'Beginning process to concatenate files'
    warehouseLocation = "file:${system:user.dir}/spark-warehouse"
    apname = "concat_"+folder
    SS = (SparkSession.builder
          .appName(apname)
          .config("spark.ui.showConsoleProgress", "false")
          .config("spark.sql.warehouse.dir", warehouseLocation)
          .config("spark.rpc.message.maxSize", "384")
          .config("spark.port.maxRetries", "40")
          .enableHiveSupport().getOrCreate())

    sc = SS.sparkContext
    log('Successfully got SparkSession')

    s0 = time.time()
    engine, connection, cursor = get_db_connection(
        db_user, db_host, db_port, db_name, db_pwd)
    # file_list, size_list = make_file_list_boto( folder)  # direct access to s3, slower that db
    file_list, size_list = make_file_list_db(folder, connection)
    s1 = time.time()
    log_processing_time('file info collection', s0, s1)
    if s1-s0 > 60:
        rep_str = logging_prefix()+': '+'file info collection took ' + \
            str(round((s1-s0)/60, 1)) + ' min'
    else:
        rep_str = logging_prefix()+': '+'file info collection took ' + \
            str(round(s1-s0, 1)) + ' sec'
    report_message.append(rep_str)
    total_size = sum(size_list)
    log('{}: Found total files {}, with total size {} bytes'.format(
        folder, len(file_list), total_size))
    rep_str = logging_prefix()+': '+'{}: Found total files {}, with total size {} bytes'.format(folder,
                                                                                                len(file_list), total_size)
    report_message.append(rep_str)
    # clean table before chunks or single data will be written there
    recreate_postgres_table(folder, engine, connection, cursor)

    # if total_size>size_lim, create chunks of files and process each chunk in turn
    if total_size > size_lim:
        chunks, size_chunks = chunkIt_with_sizes(
            file_list, size_list, size_lim)
        chunks_num = len(chunks)
        log('{}: total_size is larger than {:.1E} bytes, so splitting the whole data into {} chunks'.format(folder,
                                                                                                            size_lim, chunks_num))

        # ch is an array of file names ['0.json','1,json']
        for i, ch in enumerate(chunks):
            # size of files in current chunk
            size_chunk_cur = sum(size_chunks[i])
            log('chunk {}/{} of {}: there are {} files in chunk, chunk size = {} bytes'.format(i+1, chunks_num,
                                                                                               folder, len(ch), size_chunk_cur))
            log('chunk {}/{} of {}: biggest file {} bytes , smallest file {} bytes, average file size {} bytes'.format(i+1,
                                                                                                                       chunks_num,
                                                                                                                       folder,
                                                                                                                       max(
                                                                                                                           size_chunks[i]),
                                                                                                                       min(
                                                                                                                           size_chunks[i]),
                                                                                                                       int(np.mean(size_chunks[i]))))
            s2 = time.time()
            full_path_chunk = [ps3 + folder + '/' + f for f in ch]
            log('for debug: full_path_chunk[0]={}'.format(full_path_chunk[0]))
            # min of size_chunk_cur/(50k) or len(chunk_file_list)/10
            n_parts = np.maximum(
                int(1 + size_chunk_cur/(50000*1)), int(1 + len(ch)/10))
            # Note: max() or min() fails sometimes, I think because it uses sql functions (instead of embedded max, min, etc). So need to say np.max specificaly

            log("chunk {}/{} of {}: Start parallelize into {} partitions".format(i+1,
                                                                                 chunks_num, folder, n_parts))
            rdd_f = sc.parallelize(full_path_chunk, n_parts)
            log("chunk {}/{} of {}: Done parallelize".format(i+1, chunks_num, folder))
            s3 = time.time()
            log_processing_time(
                "\tparallelize files from full_path_chunk took ", s2, s3)
            log("{}: Starting map part".format(folder))

            parts = rdd_f.mapPartitions(get_content_boto3).collect()
            log("{}: Done map part".format(folder))
            s4 = time.time()
            log_processing_time(
                "\t{}: parts collection took ".format(folder), s3, s4)

            all_files_chunk = [item for sublist in parts for item in sublist]
            if len(all_files_chunk) != len(ch):
                log('chunk {}/{}: Collected {} of {} files'.format(i+1,
                                                                   chunks_num,
                                                                   len(all_files_chunk),
                                                                   len(ch)))
                rep_str = logging_prefix()+': '+'chunk {}/{}: Collected {} of {} files'.format(i+1,
                                                                                               chunks_num,
                                                                                               len(
                                                                                                   all_files_chunk),
                                                                                               len(ch))
                report_message.append(rep_str)
            ss3 = get_s3_resource()
            bucket_name = 'data-lake-us-east-2-549323063936-validated'
            # save chunk i into validated bucket, folder_all folder
            fpath = 'hive/tables/'+folder+'_all/' + \
                folder+'_all_' + str(i) + '.json'
            object = ss3.Object(bucket_name, fpath)
            body = '\n'.join(all_files_chunk)
            sz = get_size(body)

            try:
                log('Deleting hive table data at {}/{}'.format(bucket_name, fpath))
                object.delete()  # first delete old file with the same name
                log('Creating new hive table data at {}/{}'.format(bucket_name, fpath))
                object.put(Body=body)  # then write new one
                message = 'Successfully deleted then recreated Hive table for {}'.format(
                    fpath)
            except Exception as e:
                message = 'ERROR: Failed to delete or create Hive table for {}'.format(
                    fpath)
                log('Error Creating new hive table data at {}/{}: {}'.format(bucket_name, fpath, e))
            s5 = time.time()
            log('\tdone writing joined chunk {} file of {}'.format(i+1, folder))
            log_processing_time("writing chunk took ", s4, s5)

            try:
                log('Invoking write_to_postgres from \'if total_size>{}, create chunks\' block'.format(
                    size_lim))
                result = write_to_postgres(
                    all_files_chunk, folder, cursor, connection)
                log('Successful invocation of write_to_postgres')
                message = 'Successfully wrote to postgres table related to {}'.format(
                    folder)
            except Exception as epg:
                message = 'ERROR: Failure writing to postgres table related to {}'.format(
                    folder)
                log(message)
                log('ERROR: Exception in write_to_postgres invocation: {}'.format(epg))
                log('ERROR: WHAT SHOULD WE DO HERE??')

            s6 = time.time()
            log("{}: done writing to postgres for chunk {}".format(folder, i+1))
            log_processing_time(
                "\t"+folder+": postgres writing chunk " + str(i+1) + " data took ", s5, s6)
            log_processing_time(folder+": total time for chunk is ", s2, s6)
            log('{0} {2}  ETE is {1:.1f} min\n'.format(
                folder, (chunks_num - i-1)*(s6-s2)/60, str(datetime.now()).split('.')[0]))
            rep_str = logging_prefix()+': '+'total time for chunk is ' + \
                str(round(s6-s2, 1)) + 'sec'
            report_message.append(rep_str)
            message = 'Folder: {} : done writing to postgres for chunk {}'.format(
                folder, i+1)
        message = 'Done with all {} chunks for {}'.format(i+1, folder)
        log(message)

    else:  # total_size is smaller that size_lim
        n_parts = np.minimum(int(1 + total_size/50000),
                             int(1 + len(file_list)/10))
        # n_parts=2
        log('{}: parts number {}'.format(folder, n_parts))
        # creates list like ['s3://data../NoETL/55.json', 's3://data./NoETL/193.json']
        full_path_list = [ps3 + folder + '/' + f for f in file_list]
        log('for debug: full_path_list[0]={}'.format(full_path_list[0]))
        # parallelize list of files into 64cores*3 partitions
        rdd_f = sc.parallelize(full_path_list, n_parts)
        log(folder+": Done parallelize")

        log(folder + ": Starting map part ")
        # list of json files content [[{},{}],[{},{}],...]
        parts = rdd_f.mapPartitions(get_content_boto3).collect()
        log(folder + ": Done map part ")
        s2 = time.time()
        log_processing_time("\t"+folder+" parts collection took ", s1, s2)

        # making single list instead of (list of lists). Flattening into 1 list
        all_files = [item for sublist in parts for item in sublist]
        if len(all_files) != len(file_list):  # check if all files were read, or some are missing
            log('Collected {} of {} files'.format(
                len(all_files), len(file_list)))
            rep_str = logging_prefix()+': ' + \
                'Collected {} of {} files'.format(
                    len(all_files), len(file_list))
            report_message.append(rep_str)
        s3 = time.time()
        log("\t"+folder+" done reading files")
        log_processing_time("\t"+folder+" reading took ", s0, s3)

        ss3 = get_s3_resource()
        bucket_name = 'data-lake-us-east-2-549323063936-validated'
        # save into validated bucket, folder_all folder
        fpath = 'hive/tables/'+folder+'_all/'+folder+'_all.json'
        object = ss3.Object(bucket_name, fpath)
        body = '\n'.join(all_files)
        sz = get_size(body)
        try:
            log('Creating hive table data at {}/{}'.format(bucket_name, fpath))
            object.put(Body=body)
            message = 'Successfully created Hive table for {}'.format(fpath)
        except Exception as e:
            message = 'ERROR: Error Creating hive table data at {}/{}: {}'.format(
                bucket_name, fpath, e)
            log(message)
        s4 = time.time()
        log("\t"+folder+": done writing joined file for {}".format(folder))
        log_processing_time("\t"+folder+": writing took ", s3, s4)

        try:
            log('Invoking of write_to_postgres from \'total_size is smaller that size_lim\' block')
            write_to_postgres(all_files, folder, cursor, connection)
            log('Successful invocation of write_to_postgres')
            message = 'Successfully wrote to postgres table related to {}'.format(
                folder)
        except Exception as epg:
            message = 'ERROR: Exception in write_to_postgres for folder {} invocation: {}'.format(
                folder, epg)
            log(message)
            log('ERROR: WHAT SHOULD WE DO HERE??')

        s5 = time.time()
        log_processing_time("\t\t"+folder+": postgres writing took ", s4, s5)
        rep_str = logging_prefix()+': '+'postgres writing took ' + \
            str(round(s5-s4, 1)) + ' sec'
        report_message.append(rep_str)
        rep_str = logging_prefix()+': '+'total time for '+folder + \
            ' is ' + str(round(s5-s1, 1)) + ' sec'
        report_message.append(rep_str)
    log('Closing connection to postgres')
    s_final = time.time()
    if s_final-s0 > 60:
        rep_str = logging_prefix()+': '+'whole processing took ' + \
            str(round((s_final-s0)/60, 1)) + ' min'
    else:
        rep_str = logging_prefix()+': '+'whole processing took ' + \
            str(round(s_final-s0, 1)) + ' sec'
    report_message.append(rep_str)
    cursor.close()  # close connection only when all DB write operation (chunks or single file) is completed
    connection.close()
    time.sleep(10)
    # log(report_message) #this is to see why whole processing is not in report message?
    write_report(folder, report_message)
    log('Exiting concatenation_parallel')
    return message


def write_report(folder, report_message):
    """
    Function to save report_message in s3
    ps3rep_bucket: "emr-step-scripts-aws-account-silver"
    ps3rep_folder: "echelon_data_processing/reports/'
      "s3://emr-step-scripts-aws-account-silver/echelon_data_processing/reports/" path to folder for reports on s3
    folder: like "grower", to generate report filename
    report_message: few lines of text
    """
    log('Entering write_report')
    ps3rep_bucket = "emr-step-scripts-aws-account-silver"
    # path to folder for reports on s3
    ps3rep_folder = "echelon_data_processing/reports/"
    s3 = get_s3_client()
    if not s3:
        log("Can't get s3 client, exiting. AWS token expired, may be?")
        sys.exit(1)
    rep_fname = folder+'_'+datetime.now().strftime('%Y_%m_%d_at_%H_%M_%S') + \
        '.txt'  # like list_2019_01_30_at_11_01_59.txt
    report_message = '\n'.join(report_message)
    report_message = str.encode(report_message)  # convert into bytes

    try:
        s3.put_object(Body=report_message, Bucket=ps3rep_bucket,
                      Key=ps3rep_folder+rep_fname)
    except Exception as e:
        log("Exception in write_report: {}".format(e))
    log('Exiting write_report')


def main():
    """
    For every folder provided as argument, run the concatenation procedure:
    read bunch of json files from current folder, and save as one big file on local fs
    to create schema with
    """
    global DEBUG

    ap = argparse.ArgumentParser()
    ap.add_argument('-hive', '--hive_table_creation_files_location', required=True,
                    help='S3 location where hive table-creation scripts reside')
    ap.add_argument('-f', '--folders', required=True,
                    help='list of folders to be processed in comma-separated form; eg folder1,folder2,folder3')
    ap.add_argument('-csl', '--chunk_size_limit', required=False, default=1.0,
                    help='Processing chunk memory size limit, in GB. Defines the amount of memory to process in each chunk. The default is 1GB.')
    #ap.add_argument('-du', '--db_user', required=True, help='Database username used for executing db queries')
    #ap.add_argument('-dh', '--db_host', required=True, help='Database host, e.g. data-lake-aurora-postgres-gis.cluster-cflm0f1qhus9.us-east-2.rds.amazonaws.com')
    ap.add_argument('-dp', '--db_port', required=False,
                    default='5432', help='Database listening port')
    #ap.add_argument('-dn', '--db_name', required=True, help='Name of the database against which queries are run')
    #ap.add_argument('-pk', '--ssm_pw_key', required=True, help='Key for the database password in the SSM Parameter Store')
    ap.add_argument('-env', '--environment', required=True,
                    help='name of environments for script to run, one of [dev,sit,pre,prd]')
    ap.add_argument('-d', '--debug_logging', required=False, default=False,
                    help='Debug-level toggle; True triggers logging at DEBUG level')
    args = vars(ap.parse_args())

    folders2process = args['folders'].split(",")
    # json.loads does proper string-to-boolean conversion
    DEBUG = json.loads(args['debug_logging'].lower())
    #db_host = args['db_host']
    #db_name = args['db_name']
    #db_user = args['db_user']
    db_port = args['db_port']
    env_name = args['environment']
    db_host, db_name, db_user, db_pwd = get_db_info(
        env_name)  # all comes from AWS SSM
    ssm_pw_key = db_pwd
    hive_table_creation_files_location = args['hive_table_creation_files_location']

    log('Input params')
    log('=======================================================================================')
    log('folders to process: {}'.format(folders2process))
    log('hive_table_creation_files_location: {}'.format(
        hive_table_creation_files_location))
    log('processing chunk memory size limit: {}GB'.format(
        args['chunk_size_limit']))
    log('target database host: {}'.format(db_host))
    log('target database port: {}'.format(db_port))
    log('target database name: {}'.format(db_name))
    log('target database username: {}'.format(db_user))
    log('ssm key for db password: {}'.format(ssm_pw_key))
    log('debug logging?: {}'.format(DEBUG))
    log('=======================================================================================')

    log('Starting the magic....')
    size_lim = float(args['chunk_size_limit'])*1e9

    results = []  # list of messages for each folder
    exit_status = 0
    for folder in folders2process:
        if (env_name != 'prd') and (folder in ['farm', 'field', 'grower', 'organization']):
            ps3 = "s3://data-lake-us-east-2-549323063936-encrypted/Agrian/obfuscated_data/"
        else:
            # path to root bucket on s3
            ps3 = "s3://data-lake-us-east-2-549323063936-encrypted/Agrian/AdHoc/NoETL/"

        try:
            s0 = time.time()
            log("-----\tWorking with {} ".format((ps3 + folder)))
            status = concatenation_parallel(ps3, hive_table_creation_files_location, folder,
                                            size_lim, db_user, db_host, db_port, db_name, db_pwd, env_name)  # BAD or OK

            if status.find('ERROR') == -1:
                message = 'Success processing {}'.format(folder)
                log(message)
                s1 = time.time()
                log_processing_time('{}'.format(folder), s0, s1)
                results.append([folder, status])
                log('-'*20)
            else:
                message = 'Failure processing {}'.format(folder)
                log(message)
                s1 = time.time()
                log_processing_time('{}'.format(folder), s0, s1)
                results.append([folder, status])
                exit_status = 1
                log('-'*20)
        except Exception as e:
            log("ERROR: Exception processing folder {}: {} ".format(folder, e))
            exit_status = 1
            log('-'*20)
    if exit_status == 0:
        for i in results:
            print(i)

    log('Exiting with status: {}'.format(exit_status))
    sys.exit(exit_status)


if __name__ == "__main__":
    global LOCAL
    global AWS_CREDENTIALS_PROFILE
    global DEBUG
    global REGION
    LOCAL = False
    AWS_CREDENTIALS_PROFILE = 'silver'
    REGION = 'us-east-2'
    main()
