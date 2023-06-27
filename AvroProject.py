import os
import sys
import pyspark
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import expr, explode,array,struct,regexp_replace,trim,split
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,DateType

import os
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

#Download spark-xml_2.12-0.12.0.jar from Maven Repositry according to the scala version my scala version is 2.12 add to the config
spark = SparkSession.builder.getOrCreate()
sc = SparkSession.sparkContext
spark.sparkContext.setLogLevel("ERROR")


