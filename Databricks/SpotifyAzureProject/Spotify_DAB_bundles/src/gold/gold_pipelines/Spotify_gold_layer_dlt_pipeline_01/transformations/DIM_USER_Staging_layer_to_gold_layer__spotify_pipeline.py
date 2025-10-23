import dlt 
import os

from utilities import utils
from pyspark.sql.functions import *

#CATALOG = os.getenv("DLT_CATALOG", "dev_catalog")
#SCHEMA01 = os.getenv("DLT_SCHEMA01", "gold")
#SCHEMA02 = os.getenv("DLT_SCHEMA02","silver")

expectations ={
    "rule_1" :"user_id IS NOT NULL"
}



@dlt.table
@dlt.expect_all_or_drop(expectations)
def Dimuser_stg():
    df = spark.readStream.table(f"dev_catalog.silver.dimuser")
    df = df.withColumn("Royality_flg",utils.royality_flag(col("subscription_type")))
    return df


dlt.create_streaming_table("dimuserDLT")
dlt.create_auto_cdc_flow(
    target ="dimuserDLT",
    source = "Dimuser_stg",
    keys = ["user_id"],
    sequence_by= "updated_at",
    stored_as_scd_type =2,
    name =None,
    once = False
)