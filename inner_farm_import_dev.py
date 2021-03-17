# Glue script to process inner data from Data Lake 2.0 s3 bucket s3://nutrien-ingest-silver-data-inner
# into Postgres DB nutrien_datalake_silver_data_reconciliation_gis, for Reconciler 2.0
# Use logging for error output. Reference https://medium.com/@ly.lee/transform-and-import-a-json-file-into-amazon-redshift-with-aws-glue-3371006e03ca
# For farm DEV
import sys
import datetime
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

# via https://stackoverflow.com/questions/48914324/how-do-i-write-messages-to-the-output-log-on-aws-glue/48921571#48921571
MSG_FORMAT = '%(asctime)s\t%(levelname)s -- %(processName)s %(filename)s:%(lineno)s -- %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)

log = logging.getLogger(__name__)  # factory method
log.setLevel(logging.DEBUG)

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# @type: DataSource
# @args: [database = "inner_farm", table_name = "farm", transformation_ctx = "datasource0"]
# @return: datasource0
# @inputs: []
# via https://stackoverflow.com/questions/56836447/how-to-access-data-in-subdirectories-for-partitioned-athena-table
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="inner_farm", table_name="farm", transformation_ctx="datasource0", additional_options={"recurse": True})
datasource0.printSchema()  # in log, look for "|--"
df = datasource0.toDF()

log.debug("list of columns: {}".format(df.columns))
log.debug("number of rows: {}".format(df.count()))
df.show(20, False)

# create udb that obfuscate name column with "farmid" as it's unique
obf_farmname = udf(lambda idd:
                   "farmname_for_id_" +
                   str(idd),
                   StringType())  # creates string like "name_for_id_1251742516"
obf_customername = udf(lambda idd:
                       "customername_for_id_" +
                       str(idd),
                       StringType())  # creates string like "accountname_for_id_1251742516"
obf_state = udf(lambda idd:
                "state_for_id_" +
                str(idd),
                StringType())  # creates string like "state_for_id_1251742516"
obf_county = udf(lambda idd:
                 "county_for_id_" +
                 str(idd),
                 StringType())  # creates string like "county_for_id_1251742516"

# create additional columns to keep this obfuscated data
df = df.withColumn("obfuscated_farmname", obf_farmname(df["farmid"]))
df = df.withColumn("obfuscated_customername", obf_customername(df["farmid"]))
df = df.withColumn("obfuscated_state", obf_state(df["farmid"]))
df = df.withColumn("obfuscated_county", obf_county(df["farmid"]))
# create additional column row_loaded_timestamp data
df = df.withColumn("rowloadedat", F.current_timestamp())
# fill all empty cells with NULL, for every column of df
for colmn in df.columns:
    df = df.withColumn(colmn, when(
        col(colmn) == '', None).otherwise(col(colmn)))
# Convert Apache Spark DataFrame back to AWS Glue DynamicFrame
datasource0 = DynamicFrame.fromDF(df, glueContext, "nested")

# @type: ApplyMapping
# @args: [mapping = [("customerid", "long", "customerid", "long"), ("customername", "string", "customername", "string"), ("state", "string", "state", "string"), ("county", "string", "county", "string"), ("status", "string", "status", "string"), ("country", "string", "country", "string"), ("farmid", "string", "farmid", "string"), ("farmname", "string", "farmname", "string"), ("totalfarmarea", "double", "totalfarmarea", "double"), ("totalfarmareauom", "string", "totalfarmareauom", "string"), ("totalfarmareauomdescription", "string", "totalfarmareauomdescription", "string"), ("sourcesystemid", "string", "sourcesystemid", "string"), ("sourcesystem", "string", "sourcesystem", "string"), ("extractdate", "string", "extractdate", "timestamp"), ("extractedsystem", "string", "extractedsystem", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame=datasource0, mappings=[
    # interesting: in customerid col, "long" to "long" outputs NULL in a whole column!
    ("customerid", "string", "customerid", "string"),
    # ("customername", "string", "customername", "string"),
    # ("state", "string", "state", "string"),
    # ("county", "string", "county", "string"),
    ("status", "string", "status", "string"),
    ("country", "string", "country", "string"),
    ("farmid", "string", "farmid", "string"),
    # ("farmname", "string", "farmname", "string"),
    ("totalfarmarea", "double", "totalfarmarea", "double"),
    ("totalfarmareauom", "string", "totalfarmareauom", "string"),
    ("totalfarmareauomdescription", "string",
     "totalfarmareauomdescription", "string"),
    ("sourcesystemid", "string", "sourcesystemid", "string"),
    ("sourcesystem", "string", "sourcesystem", "string"),
    ("extractdate", "string", "extractdate", "string"),
    ("extractedsystem", "string", "extractedsystem", "string"),
    ("obfuscated_farmname", "string", "farmname", "string"),
    ("obfuscated_customername", "string", "customername", "string"),
    ("obfuscated_state", "string", "state", "string"),
    ("obfuscated_county", "string", "county", "string"),
    ("rowloadedat", "timestamp", "rowloadedat", "timestamp"),
    ("dt", "string", "sourcefile", "string")
], transformation_ctx="applymapping1")  # @type: ResolveChoice
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
# @args: [catalog_connection = "nutrien_datalake_silver_data_reconciliation_gis", connection_options = {"dbtable": "farm", "database": "nutrien_datalake_silver_data_reconciliation_gis"}, transformation_ctx = "datasink4"]
# @return: datasink4
# @inputs: [frame = dropnullfields3]
# datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3, catalog_connection="datalake-silver", connection_options={
#    "dbtable": "internal_in.farm", "database": "postgres"}, transformation_ctx="datasink4")
# writing to both DBs from one job doesn't work
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3, catalog_connection="nutrien_datalake_silver_data_reconciliation_gis", connection_options={
    "dbtable": "internal_in_v2.farm", "database": "nutrien_datalake_silver_data_reconciliation_gis"}, transformation_ctx="datasink4")
# datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3, catalog_connection="nutrien_datalake_sit_data_reconcilliation_gis", connection_options={
#    "dbtable": "internal_in.farm", "database": "nutrien_datalake_sit_data_reconciliation_gis"}, transformation_ctx="datasink5")
job.commit()
