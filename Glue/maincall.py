import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
#import Test
from awsglue.dynamicframe import DynamicFrame
import json
import boto3
from Glue import ETLJobs
from sparkGlueInitialize import StartSpark
import os
from pyspark.sql import SparkSession
# class Spark(StartSpark):
#     def __init__(self):
#         pass

#     def callspark(self):
#         return Test.returnData(self)

# obj=StartSpark()
# spark,glueContext=obj.returnData()
# s3_obj =boto3.client('s3')
#
# s3_clientobj = s3_obj.get_object(Bucket='shuvabuc9096', Key='config.json')
# s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
# data=json.loads(s3_clientdata)
# print(data)

# print('Glue',glueContext)
# print('spark',spark)

# with open("/home/config.json", "r") as jsonfile:
#     data = json.load(jsonfile)
#     print("Read successful")
# #print(data)

# ETL=ETLJobs(spark,glueContext)
# ETL.extract(data)
# spark.sql("select * from test").show(1)
#spark.sql("select responseid,respid,qFMNO from test").write.partitionBy("respid").parquet("s3://shuvabuc9093/otpt",mode="overwrite")

os.environ ['HADOOP_HOME']= "C:\\Users\\shumondal\\Desktop\\allmyfiles\\data-master\\winutils-master\hadoop-2.8.0"
os.environ['SPARK_CLASSPATH']="C:\\Users\\shumondal\\Downloads\\ojdbc8.jar"
print(os.environ['SPARK_CLASSPATH'])
spark = SparkSession.builder.appName('sim_transactions_test').config("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']).getOrCreate()
with open("C:\\Users\\shumondal\\PycharmProjects\\AWS\\Glue\\config.json", "r") as jsonfile:
    data = json.load(jsonfile)

print(data)
ob=ETLJobs(spark)

