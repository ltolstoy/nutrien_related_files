import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# @type: DataSource
# @args: [database = "inner_salesrep", table_name = "dt_2020_02_14", transformation_ctx = "datasource0"]
# @return: datasource0
# @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="inner_salesrep", table_name="dt_2020_02_14", transformation_ctx="datasource0")
# @type: Map
# @args: [f = AddTimestamp, transformation_ctx = "mapped_df"]
# @return: mapped_df
# @inputs: [frame = datasource0]


def AddTimestampAndObfuscate(r):
    for col in r:
        if r[col] == '':
            r[col] = None
    obf_id = r["employeeid"]
    r["firstname"] = "firstname_for_id_" + obf_id
    r["lastname"] = "lastname_for_id_" + obf_id
    r["employeeemailid"] = "employeeemailid_for_id_" + obf_id
    r["salesrepphone"] = "salesrepphone_for_id_" + obf_id
    r["salesrepcell"] = "salesrepcell_for_id_" + obf_id
    r["workphonenmbr"] = "workphonenmbr_for_id_" + obf_id
    r["workcity"] = "workcity_for_id_" + obf_id
    r["rowloadedat"] = datetime.today()
    return r


mapped_df = Map.apply(frame=datasource0, f=AddTimestampAndObfuscate)
# @type: ApplyMapping
# @args: [mapping = [("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string"), ("employeeid", "string", "employeeid", "string"), ("status", "string", "status", "string"), ("employeeemailid", "string", "employeeemailid", "string"), ("salesrepphone", "string", "salesrepphone", "string"), ("salesrepcell", "string", "salesrepcell", "string"), ("jobdescription", "string", "jobdescription", "string"), ("workphonenmbr", "string", "workphonenmbr", "string"), ("workaddressline2", "string", "workaddressline2", "string"), ("workaddressline1", "string", "workaddressline1", "string"), ("workcity", "string", "workcity", "string"), ("workstate", "string", "workstate", "string"), ("workpostal", "string", "workpostal", "string"), ("workcountry", "string", "workcountry", "string"), ("workcounty", "string", "workcounty", "string"), ("defaultlocationid", "string", "defaultlocationid", "string"), ("defaultlocationname", "string", "defaultlocationname", "string"), ("legacysalesrepid", "string", "legacysalesrepid", "string"), ("extractdate", "string", "extractdate", "string"), ("extractedsystem", "string", "extractedsystem", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = mapped_df]
applymapping1 = ApplyMapping.apply(frame=mapped_df, mappings=[
    ("firstname", "string", "firstname", "string"),
    ("lastname", "string", "lastname", "string"),
    ("employeeid", "string", "employeeid", "string"),
    ("status", "string", "status", "string"),
    ("employeeemailid", "string", "employeeemailid", "string"),
    ("salesrepphone", "string", "salesrepphone", "string"),
    ("salesrepcell", "string", "salesrepcell", "string"),
    ("jobdescription", "string", "jobdescription", "string"),
    ("workphonenmbr", "string", "workphonenmbr", "string"),
    ("workaddressline2", "string", "workaddressline2", "string"),
    ("workaddressline1", "string", "workaddressline1", "string"),
    ("workcity", "string", "workcity", "string"),
    ("workstate", "string", "workstate", "string"),
    ("workpostal", "string", "workpostal", "string"),
    ("workcountry", "string", "workcountry", "string"),
    ("workcounty", "string", "workcounty", "string"),
    ("defaultlocationid", "string", "defaultlocationid", "string"),
    ("defaultlocationname", "string", "defaultlocationname", "string"),
    ("legacysalesrepid", "string", "legacysalesrepid", "string"),
    ("extractdate", "string", "extractdate", "string"),
    ("extractedsystem", "string", "extractedsystem", "string"),
    ("rowloadedat", "timestamp", "rowloadedat", "timestamp")
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
# @args: [catalog_connection = "nutrien_datalake_silver_data_reconciliation_gis", connection_options = {"dbtable": "dt_2020_02_14", "database": "nutrien_datalake_silver_data_reconciliation_gis"}, transformation_ctx = "datasink4"]
# @return: datasink4
# @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame=dropnullfields3, catalog_connection="nutrien_datalake_silver_data_reconciliation_gis", connection_options={
                                                           "dbtable": "internal_in_v2.salesrep", "database": "nutrien_datalake_silver_data_reconciliation_gis"}, transformation_ctx="datasink4")
job.commit()
