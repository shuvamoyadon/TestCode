from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
from pyspark.sql.functions import  date_format
from pyspark.sql import functions as F
import datetime
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql.types import *

os.environ ['HADOOP_HOME']= "C:\\Users\\shumondal\\Desktop\\allmyfiles\\data-master\\winutils-master\hadoop-2.8.0"
os.environ['SPARK_CLASSPATH']="C:\\Users\\shumondal\\Downloads\\ojdbc8.jar"
print(os.environ['SPARK_CLASSPATH'])
spark = SparkSession.builder.appName('sim_transactions_test').config("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']).getOrCreate()


Oracle_CONNECTION_URL="jdbc:oracle:thin:@//database-1.clpjnjrvfps8.us-east-1.rds.amazonaws.com:1521/ORCL"
ora_tmp = spark.read.format('jdbc')\
    .option("url",Oracle_CONNECTION_URL)\
    .option("user","admin",)\
    .option("password","iamhere1")\
    .option("dbtable","ALL_ALL_TABLES")\
    .option("driver","oracle.jdbc.OracleDriver").load().select("TABLE_NAME").filter(col("owner") =="ADMIN")

table_name=ora_tmp.rdd.map(lambda x: x[0]).collect()
for name in table_name:
    print(name)