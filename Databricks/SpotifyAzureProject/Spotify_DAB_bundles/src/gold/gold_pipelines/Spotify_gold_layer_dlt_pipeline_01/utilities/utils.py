from pyspark.sql.functions import udf
from pyspark.sql.types import *


@udf(returnType=StringType())
def royality_flag(column):
    """ if the subscription_type is premium then the user is eligible for royality_free_pass"""
    if column.strip() =="Premium":
        return  "Eligible"
    else:
        return "Not Eligible"
     
