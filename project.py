# Import pySpark
import os
import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr, explode,array,struct,regexp_replace,trim,split, spark_partition_id,monotonically_increasing_id,schema_of_csv
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,DateType


# Create SparkSession and env

if __name__ == "__main__":
 os.environ['PYSPARK_PYTHON'] = sys.executable
 os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Download spark-xml_2.12-0.12.0.jar from Maven Repositry according to the scala version my scala version is 2.12 add to the config

 spark = SparkSession.builder.config("spark.jars", "C:\Program Files\spark-3.2.3-bin-hadoop3.2\jars\spark-xml_2.12-0.12.0.jar") \
          .config("spark.driver.extraClassPath", "C:\Program Files\spark-3.2.3-bin-hadoop3.2\jars\spark-xml_2.12-0.12.0.jar") \
     .getOrCreate()

 sc = SparkSession.sparkContext
 spark.sparkContext.setLogLevel("ERROR")

 df=spark.read.option("rowTag","book").option("rootTag","catalog").format("xml").load("C:/tmp/sample.xml")

 df.persist()

#To remove /n use regexp_replace()
df1 =df.withColumn('description',regexp_replace("description",'\n',' '))
df2=df1.withColumn('description',regexp_replace('description', "\\s + ", " "))
df3=df2.withColumn('FirstName',split('author',",").getItem(0))\
       .withColumn('LastNmae',split('author',",").getItem(1))

df3.createOrReplaceTempView("XMLtable")
spark.sql("select * from XMLtable").show()

print(df3.storageLevel.useMemory)
print(df3.rdd.getNumPartitions())
df4=df3.repartition(1)
print(df4.rdd.getNumPartitions())
#df4.withColumn("Partition_id",spark_partition_id()).groupby("Partition_id").count().show()
#df4.withColumn("mono",monotonically_increasing_id()).withColumn("partition_id",spark_partition_id()).write.csv("C:/tmp/csv1")

#df3.write.parquet("C:/tmp/sampleoutput1",mode="overwrite",partitionBy="genre")
#df3.write.orc("C:/tmp/sampleoutput",mode="overwrite",partitionBy="genre")

dat=spark.read.csv("C:/tmp/SHIPPED.csv",inferSchema=True)
dat1=dat.repartition(3)
print(dat1.rdd)








# Prepare Data


 #arrayData = [('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),\
  #   ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),\
   #  ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),\
    # ('Washington', None, None),\
     #('Jefferson', ['1', '2'], {})]
#df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
#df.printSchema()
#df.cache()
#df2 = df.select(df.name, (df.properties))

#DF=df2.show()


#customSchema = StructType([
  #StructField("id", IntegerType(), False),
  #StructField("author", StringType(), False),
  #StructField("description", StringType(), False),
  #StructField("genre", StringType(), False),
  #StructField("price", DoubleType(), False),
  #StructField("publish_date", DateType(), False),
  #StructField("title", StringType(), False)])