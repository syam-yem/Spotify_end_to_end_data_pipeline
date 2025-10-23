import dlt 
import os

from utilities import utils
from pyspark.sql.functions import *

#CATALOG = os.getenv("DLT_CATALOG", "dev_catalog")
#SCHEMA01 = os.getenv("DLT_SCHEMA01", "gold")
#SCHEMA02 = os.getenv("DLT_SCHEMA02","silver")

expectations ={
    "rule_1" :"track_id IS NOT NULL"
}



@dlt.table
@dlt.expect_all_or_drop(expectations)
def Dimtrack_stg():
    df = spark.readStream.table(f"dev_catalog.silver.dimtrack")
    return df


dlt.create_streaming_table("dimtrackDLT")
dlt.create_auto_cdc_flow(
    target ="dimtrackDLT",
    source = "Dimtrack_stg",
    keys = ["track_id"],
    sequence_by= "updated_at",
    stored_as_scd_type =2,
    name =None,
    once = False
)