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
def factstream_stg():
    df = spark.readStream.table(f"dev_catalog.silver.factstream")
    return df


dlt.create_streaming_table("factstream")
dlt.create_auto_cdc_flow(
    target ="factstream",
    source = "factstream_stg",
    keys = ["stream_id"],
    sequence_by= "stream_timestamp",
    stored_as_scd_type =1
)