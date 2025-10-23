import dlt 
import os

from utilities import utils
from pyspark.sql.functions import *

#CATALOG = os.getenv("DLT_CATALOG", "dev_catalog")
#SCHEMA01 = os.getenv("DLT_SCHEMA01", "gold")
#SCHEMA02 = os.getenv("DLT_SCHEMA02","silver")

expectations ={
    "rule_1" :"date_key IS NOT NULL"
}



@dlt.table
@dlt.expect_all_or_drop(expectations)
def Dimartist_stg():
    df = spark.readStream.table(f"dev_catalog.silver.dimdate")
    return df


dlt.create_streaming_table("dimdate_DLT")
dlt.create_auto_cdc_flow(
    target ="dimartist_DLT",
    source = "dimartist_STG",
    keys = ["date_key"],
    sequence_by= "date ",
    stored_as_scd_type =2
)