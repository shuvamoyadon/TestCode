from pyspark.sql.types import *
from pyspark.sql.functions import *


def get_Guild(spark, pDF):
    pDF.createOrReplaceTempView('SourceTab')
    GuildDF = spark.sql(
        "select Response , Guild from (select respid as Response,case when variable like '%qguild%' then Value end as Guild from SourceTab) innr where Guild is not null")

    return GuildDF