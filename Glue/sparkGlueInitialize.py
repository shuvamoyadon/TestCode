import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row

class StartSpark():
    #os.environ['SPARK_CLASSPATH'] = "C:\\Users\\shumondal\\Downloads\\ojdbc8.jar"
    #os.environ['HADOOP_HOME'] = "C:\\Users\\shumondal\\Desktop\\allmyfiles\\data-master\\winutils-master\hadoop-2.8.0"
    def __init__(self):
        pass

    def returnData(self):
        sc = SparkContext().getOrCreate()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        print('I am called')
        #spark = SparkSession.builder.appName('sim_transactions_test').config("spark.driver.extraClassPath", os.environ['SPARK_CLASSPATH']).getOrCreate()
        return spark,glueContext