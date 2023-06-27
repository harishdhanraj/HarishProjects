'''import os
import sys

import pyspark
from pyspark import SparkContext


from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr, explode,array,struct,regexp_replace,trim,split
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,DateType
# Create SparkSession and env

if __name__ == "__main__":
 os.environ['PYSPARK_PYTHON'] = sys.executable
 os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

 if __name__ == "__main__":
     os.environ['PYSPARK_PYTHON'] = sys.executable
     os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("ERROR")
rdd = sc.textFile("C:/tmp/wordcount.txt")

rdd1 = rdd.flatMap(lambda x: x.split(" "))
rdd2 = rdd1.map(lambda x: (x, 1))
rdd3 = rdd2.reduceByKey(lambda x,y: x+y)
rdd3.repartition(5)
rdd4 =rdd3.sortBy(lambda x: x[1], ascending=False).take(5)
print(rdd3.getNumPartitions())
print(rdd4)
json=spark.read.json("C:/tmp/file.json")
print(type(json))
json.printSchema()
json.show(truncate=False)'''
list=[1,0,2,0,5,8,2,1,1]
list.sort(reverse=False)
print(list)
for i in list:
    if i==0:
        list.remove(i)
        list.append(i)
print(list)
d = {"brand": "Ford", "model": "Mustang","year": 1964 }
print(type(d))
print(d["brand"])
d["year"]=2018
print(d.get("year"))
d.update({"name":"harish"})
print(d)
