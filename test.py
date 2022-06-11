import sys
import re
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
from pyspark.sql.functions import  date_format
from pyspark.sql import functions as F
import datetime
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


class Test():
    os.environ['SPARK_CLASSPATH'] = "C:\\Users\\shumondal\\Downloads\\ojdbc8.jar"
    os.environ['HADOOP_HOME'] = "C:\\Users\\shumondal\\Desktop\\allmyfiles\\data-master\\winutils-master\hadoop-2.8.0"
    def __init__(self):
        pass

    def returnData(self):
        #sc = SparkContext()
        #glueContext = GlueContext(sc)
        #spark = glueContext.spark_session
        #print('I am called')
        spark = SparkSession.builder.appName('sim_transactions_test').config("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']).getOrCreate()
        return spark


