#!/anaconda3/bin/python
# or /usr/bin/python - this one doesn't have sqlalchemy
#script to test Postgres writing issues.
# I assume that oneLogin.sh script was run and we have current security token active, (1 hour time limit)

from __future__ import print_function
import json
import boto3
import datetime
import ast, sys
from sqlalchemy import create_engine, text
#import psycopg2
#from psycopg2.extras import Json
import pg8000

LOCAL = True
AWS_CREDENTIALS_PROFILE = 'silver'

def get_ssm_client():
	print('Getting SSM client')
	global LOCAL
	global AWS_CREDENTIALS_PROFILE
	if LOCAL:
		session = boto3.Session(profile_name=AWS_CREDENTIALS_PROFILE, region_name="us-east-2")
		ssm = session.client('ssm')
	else:
		ssm = boto3.client('ssm')

	return ssm

#ssm = boto3.client('ssm', region_name="us-east-2")  # low level API

def main():
	#For testing Postgres writing purposes only
	# deal with SSM
	#ssm = boto3.client('ssm', region_name="us-east-2")  # low level API
	ssm = get_ssm_client()
	response = ssm.get_parameter(Name='/nutrien/org-poc/dev/AGRIUM_DATA_LAKE_RECONCILIATION_GIS_URL', WithDecryption=True)
	address0 = response["Parameter"]["Value"]
	address = address0.replace('postgres', 'postgresql+pg8000',1) #try to use pg8000 instead of psycopg2
	#address = address0.replace('postgres', 'postgresql+psycopg2',1) #try to use psycopg2, via https://docs.sqlalchemy.org/en/latest/core/engines.html
	print("\nDB address from SSM response:\n {}\n{}".format(address0, address))
	[usr,pwd,host, port,dbname] = address.replace(':','|').replace('/','|').replace('@','|').split('|')[3:8]
	print("address parameters:\nusr = {}\npwd={}\nhost={}\nport={}\ndbname={}".format(usr,pwd,host,port,dbname))
	try:
		print("Creating engine")
		engine = create_engine(address,pool_pre_ping=True)
	except Exception as e:
		print("Exception while create_engine with pg8000:\n{}\n".format(e))	
	else:
		print("Creating engine Success!")
		print(engine)
	try:
		print("Creating connection")
		connection = engine.raw_connection() 		  #raw_connection()
	except Exception as e:
		print("Exception while engine.connect:\n{}\n".format(e))
	else:
		print("Making connention Success!")
		print(connection)
	try:
		print("Creating cursor")
		cursor = connection.cursor()
	except Exception as e:
		print("Exception while creating cursor:\n{}\n".format(e))
	else:
		print("Making cursor Success!")
		print(cursor)
	"""try:
		engine = create_engine(address)
		connection = engine.raw_connection()
		cursor = connection.cursor()
	except Exception as e:
		print('Exception in write_to_postgres_test: {}'.format( e))"""
	#print("----EXITING------")
	#sys.exit(0)
	
	
	folder ='aaaa'
	table_name = folder #test table in echelon_json is aaaa
	table_n = 'echelon_json.'+table_name # this form is for queries , otherwise doesn't work. 'echelon_json.field'
	#via https://stackoverflow.com/questions/33053241/sqlalchemy-if-table-does-not-exist
	if not engine.dialect.has_table(engine, table_name, schema='echelon_json'):
		print("Can't see table {}".format(table_n))
	else:
		print("Can see table {}".format(table_n))

	val1 = ( "30000000-aaaa-bbbb-cccc-dddddddddddd", 
	"""{"source":"from_local_standalone_script_v3"}""", 
	datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") ) # id, json, appended_at
	query1 = """
			INSERT into """ + table_n + """ 
				(id, json, appended_at) 
			VALUES 
				(%s, %s, %s)
			ON CONFLICT (id) DO UPDATE SET
			id=EXCLUDED.id, json=EXCLUDED.json, appended_at=EXCLUDED.appended_at;
		""" # this query will  update existing record in postgres with new data
		#Note ; at the end
	try:
		cursor.execute(query1, val1) #write just one row
	except Exception as e:
		print('\nException in write_to_postgres_test for {}: {}\n'.format(folder, e))
		connection.rollback()	
	connection.commit()
	print(cursor.rowcount, " was inserted into Postgres")
	# try to get it back
	try:
		cursor.execute("select id, json, appended_at from echelon_json.aaaa where id='30000000-aaaa-bbbb-cccc-dddddddddddd'")
	except Exception as e:
		print(e)
	print("The number obtained {}: ", cursor.rowcount)
	row = cursor.fetchone()
	print("Obtained from db: {}".format(row))


if __name__== "__main__":
		main()