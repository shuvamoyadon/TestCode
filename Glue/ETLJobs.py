from pyspark.sql.types import *
from pyspark.sql import Row
from py4j.protocol import Py4JJavaError
import logging


class ETLJobs():
    def __init__(self, spark):
        self.spark = spark
        #self.glueContext = glueContext

    def readData(self, fileFormat, filepath, tablename, partitionNumber=10, options={}):
        if fileFormat == "csv":
            try:
                df = self.spark.read.format(fileFormat).options(**options).load(
                    "{}{}".format(filepath, '/*')).repartition(
                    partitionNumber)
                df.createOrReplaceTempView(tablename)
            except Exception as err:
                raise Exception(err.__class__.__name__ + 'from extract Data- Error reading file')
        else:
            try:
                df = self.spark.read.parquet(filepath + '/*')
                df.createOrReplaceTempView(tablename)
            except Exception as err:
                raise Exception(err.__class__.__name__ + 'from extract Data- Error reading file')

    def extract(self, config):
        filepath = config["filepath"]
        fileFormat = config["format"]
        defaultFormat = config["defaultFormat"]
        numberOfPartitionEachFile = config["numOfPartitionForeachfile"]
        tableName = config["tableName"]
        Option = config["options"]
        cnt = 0
        if len(filepath) > 0:
            for file in filepath:
                if len(fileFormat) > 0:
                    if numberOfPartitionEachFile:
                        partitionNumber = numberOfPartitionEachFile.pop(0)
                        self.readData(fileFormat, file, tableName[cnt], partitionNumber, Option)
                        cnt = cnt + 1
                    else:
                        logging.info("Partition not given,will be used default partition")
                        self.readData(fileFormat, file, tableName[cnt], options=Option)
                        cnt = cnt + 1

                else:
                    logging.info(
                        "File Format not given.Default format will be used and the same formatted File given in input level")
                    self.readData(defaultFormat, file, tableName[cnt])
        else:
            logging.info(
                "Empty Data in dataframe. File Path is not given to load data. Please give the input path for reading files")
            df = self.spark.createDataFrame([], StructType([]))
            df.show()

    def transform(self):
        pass

    def load(df, outputFilePath, writeFormat, PartitionKey, mode):
        if outputFilePath:
            if df.take(1):
                if writeFormat:
                    pass
                else:
                    pass

            else:
                logging.info("Dataframe is empty.")

        else:
            logging.info("OutPutFilePath not given.OutputFile location is required to generate the output of table")