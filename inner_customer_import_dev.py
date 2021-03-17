# Glue script to process inner data from Data Lake 2.0 s3 bucket s3://nutrien-ingest-silver-data-inner
# into Postgres DB nutrien_datalake_silver_data_reconciliation_gis, for Reconciler 2.0
# Use logging for error output. Reference https://medium.com/@ly.lee/transform-and-import-a-json-file-into-amazon-redshift-with-aws-glue-3371006e03ca
# Uses boto3 for Athena query run (to update list of partitions on s3)
# For customer DEV
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# Additional imports for Apache Spark DataFrame.
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import udf, explode, col, when
from pyspark.sql import functions as F  # for current_time()
from pyspark.sql.types import StringType
import logging
import logging.config
# Additional import for boto3 Athena communications
import boto3

# via https://stackoverflow.com/questions/48914324/how-do-i-write-messages-to-the-output-log-on-aws-glue/48921571#48921571
MSG_FORMAT = '%(asctime)s\t%(levelname)s -- %(processName)s %(filename)s:%(lineno)s -- %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)

log = logging.getLogger(__name__)  # factory method
log.setLevel(logging.DEBUG)
# Athena query part
client = boto3.client('athena', region_name='us-east-2')
data_catalog_table = "customer"
db = "inner_customer"  # glue data_catalog db, not Postgres DB
# this supposed to update all partitions for data_catalog_table, so glue job can upload new file data into DB
q = "MSCK REPAIR TABLE "+data_catalog_table
# output of the query goes to s3 file normally
output = "s3://aws-glue-temporary-000469811676-us-east-2/reports/"
response = client.start_query_execution(
    QueryString=q,
    QueryExecutionContext={
        'Database': db
    },
    ResultConfiguration={
        'OutputLocation': output,
    }
)
log.debug("Athena query result after updating partitions: {}".format(response))
# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# @type: DataSource
# @args: [database = "inner_customer", table_name = "dt_2020_02_03", transformation_ctx = "datasource0"]
# @return: datasource0
# @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="inner_customer", table_name="customer", transformation_ctx="datasource0")
datasource0.printSchema()  # in log, look for "|--"
df = datasource0.toDF()

log.debug("list of columns: {}".format(df.columns))
log.debug("number of rows: {}".format(df.count()))
df.show(20, False)

# create udf that obfuscate name column with "farmid" as it's unique
obf_customername = udf(lambda idd:
                       "customername_for_id_" +
                       str(idd),
                       StringType())  # creates string like "customername_for_id_1251742516"
obf_customeradr1 = udf(lambda idd:
                       "customeradr1_for_id_" +
                       str(idd),
                       StringType())  # creates string like "customeradr1_for_id_1251742516"
obf_customeradr2 = udf(lambda idd:
                       "customeradr2_for_id_" +
                       str(idd),
                       StringType())  # creates string like "customeradr2_for_id_1251742516"
obf_city = udf(lambda idd:
               "custcity_for_id_" +
               str(idd),
               StringType())  # creates string like "custcity_for_id_1251742516"
obf_bphone = udf(lambda idd:
                 "bphone_for_id_" +
                 str(idd),
                 StringType())  # creates string like "bphone_for_id_1251742516"
obf_cellphone = udf(lambda idd:
                    "cellphone_for_id_" +
                    str(idd),
                    StringType())
obf_zip = udf(lambda idd:
              "zip_for_id_" +
              str(idd),
              StringType())

# create additional columns to keep this obfuscated data
df = df.withColumn("obfuscated_customername",
                   obf_customername(df["legacycustomerid"]))
df = df.withColumn("obfuscated_customeradr1",
                   obf_customeradr1(df["legacycustomerid"]))
df = df.withColumn("obfuscated_customeradr2",
                   obf_customeradr2(df["legacycustomerid"]))
df = df.withColumn("obfuscated_city", obf_city(df["legacycustomerid"]))
df = df.withColumn("obfuscated_bphone", obf_bphone(df["legacycustomerid"]))
df = df.withColumn("obfuscated_cellphone",
                   obf_cellphone(df["legacycustomerid"]))
df = df.withColumn("obfuscated_zip", obf_zip(df["legacycustomerid"]))
# create additional column row_loaded_timestamp data
df = df.withColumn("rowloadedat", F.current_timestamp())
# fill all empty cells with NULL, for every column of df
for colmn in df.columns:
    df = df.withColumn(colmn, when(
        col(colmn) == '', None).otherwise(col(colmn)))
# Convert Apache Spark DataFrame back to AWS Glue DynamicFrame
datasource0 = DynamicFrame.fromDF(df, glueContext, "nested")

# @type: ApplyMapping
# @args: [mapping = [("legacycustomerid", "long", "legacycustomerid", "long"), ("customerstatus", "string", "customerstatus", "string"), ("customergroup", "string", "customergroup", "string"), ("customergroupdescription", "string", "customergroupdescription", "string"), ("customergroup1", "string", "customergroup1", "string"), ("customergroup1description", "string", "customergroup1description", "string"), ("customergroup2", "string", "customergroup2", "string"), ("customergroup2description", "string", "customergroup2description", "string"), ("customergroup3", "string", "customergroup3", "string"), ("customergroup3description", "string", "customergroup3description", "string"), ("sapcustomercompanycodes", "string", "sapcustomercompanycodes", "string"), ("sapcustomercompanycodedescription", "string", "sapcustomercompanycodedescription", "string"), ("customername", "string", "customername", "string"), ("sapcustomerid", "string", "sapcustomerid", "string"), ("customerbillingaddressline1", "string", "customerbillingaddressline1", "string"), ("customerbillingaddressline2", "string", "customerbillingaddressline2", "string"), ("billingcity", "string", "billingcity", "string"), ("billingstateprovince", "string", "billingstateprovince", "string"), ("billingpostal", "string", "billingpostal", "string"), ("billingcounty", "string", "billingcounty", "string"), ("billingcountry", "string", "billingcountry", "string"), ("billingphonenumber", "string", "billingphonenumber", "string"), ("cellphonenumber", "string", "cellphonenumber", "string"), ("currentcreditlimit", "double", "currentcreditlimit", "double"), ("homebranch", "long", "homebranch", "long"), ("defaultsalesrepid", "string", "defaultsalesrepid", "string"), ("nickname", "string", "nickname", "string"), ("legalflag", "long", "legalflag", "long"), ("legalflagdescription", "string", "legalflagdescription", "string"), ("baddebt", "string", "baddebt", "string"), ("creditstatus", "string", "creditstatus", "string"), ("extractdate", "string", "extractdate", "string"), ("extractedsystem", "string", "extractedsystem", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=[
    ("legacycustomerid", "string", "legacycustomerid", "string"),
    ("customerstatus", "string", "customerstatus", "string"),
    ("customergroup", "string", "customergroup", "string"),
    ("customergroupdescription", "string", "customergroupdescription", "string"),
    ("customergroup1", "string", "customergroup1", "string"),
    ("customergroup1description", "string", "customergroup1description", "string"),
    ("customergroup2", "string", "customergroup2", "string"),
    ("customergroup2description", "string", "customergroup2description", "string"),
    ("customergroup3", "string", "customergroup3", "string"),
    ("customergroup3description", "string", "customergroup3description", "string"),
    ("sapcustomercompanycodes", "string", "sapcustomercompanycodes", "string"),
    ("sapcustomercompanycodedescription", "string", "sapcustomercompanycodedescription",
     "string"),
    ("obfuscated_customername", "string", "customername", "string"),
    ("sapcustomerid", "string", "sapcustomerid", "string"),
    ("obfuscated_customeradr1", "string", "customerbillingaddressline1", "string"),
    ("obfuscated_customeradr2", "string", "customerbillingaddressline2", "string"),
    ("obfuscated_city", "string", "billingcity", "string"),
    ("billingstateprovince", "string", "billingstateprovince", "string"),
    ("obfuscated_zip", "string", "billingpostal", "string"),
    ("billingcounty", "string", "billingcounty", "string"),
    ("billingcountry", "string", "billingcountry", "string"),
    ("obfuscated_bphone", "string", "billingphonenumber", "string"),
    ("obfuscated_cellphone", "string", "cellphonenumber", "string"),
    ("currentcreditlimit", "string", "currentcreditlimit", "string"),
    ("homebranch", "string", "homebranch", "string"),
    ("defaultsalesrepid", "string", "defaultsalesrepid", "string"),
    ("nickname", "string", "nickname", "string"),
    ("legalflag", "string", "legalflag", "string"),
    ("legalflagdescription", "string", "legalflagdescription", "string"),
    ("baddebt", "string", "baddebt", "string"),
    ("creditstatus", "string", "creditstatus", "string"),
    ("extractdate", "string", "extractdate", "string"),
    ("extractedsystem", "string", "extractedsystem", "string"),
    ("rowloadedat", "timestamp", "rowloadedat", "timestamp"),
    ("dt", "string", "sourcefile", "string")
], transformation_ctx="applymapping1")
# @type: ResolveChoice
# @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
# @return: resolvechoice2
# @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1, choice="make_cols", transformation_ctx="resolvechoice2")
# @type: DropNullFields
# @args: [transformation_ctx = "dropnullfields3"]
# @return: dropnullfields3
# @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2, transformation_ctx="dropnullfields3")
# @type: DataSink
# @args: [catalog_connection = "nutrien_datalake_silver_data_reconciliation_gis", connection_options = {"dbtable": "dt_2020_02_03", "database": "nutrien_datalake_silver_data_reconciliation_gis"}, transformation_ctx = "datasink4"]
# @return: datasink4
# @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3, catalog_connection="nutrien_datalake_silver_data_reconciliation_gis", connection_options={
                                                           "dbtable": "internal_in_v2.customer", "database": "nutrien_datalake_silver_data_reconciliation_gis"}, transformation_ctx="datasink4")
job.commit()
