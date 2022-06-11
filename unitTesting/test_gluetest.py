import unittest
from gluetest import get_Guild
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
import os
import pandas as pd

class SparkETLTestCase(unittest.TestCase):

    def equivalent_type(self,f):
        if f == 'datetime64[ns]':
            return TimestampType()
        elif f == 'int64':
            return LongType()
        elif f == 'int32':
            return IntegerType()
        elif f == 'float64':
            return DoubleType()
        elif f == 'float32':
            return FloatType()
        else:
            return StringType()

    def define_structure(self,string, format_type):
        try:
            typo =self.equivalent_type(format_type)
        except:
            typo = StringType()
        return StructField(string, typo)

    # Given pandas dataframe, it will return a spark's dataframe.

    def pandas_to_spark(self,pandas_df, spark):
        columns = list(pandas_df.columns)
        types = list(pandas_df.dtypes)
        struct_list = []
        for column, typo in zip(columns, types):
            struct_list.append(self.define_structure(column, typo))
        p_schema = StructType(struct_list)
        return spark.createDataFrame(pandas_df, p_schema)

    @classmethod
    def setUpClass(cls):
        os.environ[
            'HADOOP_HOME'] = "C:\\Users\\shumondal\\Desktop\\allmyfiles\\data-master\\winutils-master\hadoop-2.8.0"
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("PySpark-unit-test")
                     .config('spark.port.maxRetries', 1)
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_gluetest(self):
        text = self.spark.read.format('csv').options(header='true', inferSchema='true').load(
            "C:\\Users\\shumondal\\Downloads\\test.csv")

        pDF = text.toPandas().dropna(axis=1, how='all')
        p = pDF.melt(id_vars=['responseid', 'respid'])
        org = self.pandas_to_spark(p, self.spark)
        transformed_df =get_Guild(self.spark, org)
        transformed_df.printSchema()

        expected_schema = StructType([
            StructField('Response', IntegerType(), True),
            StructField('Guild', StringType(), True)
        ])

        expected_data = [(1234, "Basic Materials"),
                         (67891, "Basic Materials")]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # Apply transforamtion on the input data frame


        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))


if __name__ == '__main__':
    unittest.main()