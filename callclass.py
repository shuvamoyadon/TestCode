import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
#import Test
from awsglue.dynamicframe import DynamicFrame
import json
from classfile import Test

class Ma(Test):
    def __init__(self):
        pass

    def callspark(self):
        return Test.returnData(self)

a=Ma()
spark,glueContext=a.callspark()

print('Glue',glueContext)
print('spark',spark)

with open("config.json", "r") as jsonfile:
    data = json.load(jsonfile)
    print("Read successful")
print(data["filename"])
