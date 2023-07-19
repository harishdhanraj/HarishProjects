#spark_sql_ETL_2.py start (Bread & Butter2)
#4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets (level 2)
#above and beyond
#6. how to standardize the code or how create generic/reusable frameworks (level 3) - masking engine, reusable transformation, data movement automation engine, quality suite, audit engine, data observability, data modernization...

#1. SDLC life cycle (Agile/Scrum/RAD/V/WF) -> requirement -> dev -> peer code review -> unit testing -> shift code to UAT/SIT-> Deployed
# -> Execution (Integration testing) -> signoff -> Productionise
#2. To Productionise: Dev -> packaged ->shipped (DEV) -> Deployed/Submitted -> Tested -> Package -> shift (github->jenkins) -> Deploy in Prod (cloud) -> Orchestration & Scheduling -> Monitoring and log analysis (PS team)

#lets take our scenario (Roles and Responsibilities of a DE)
#1. We completed creating a spark application to do some ETL/ELT operation in the Pycharm/Intellij/Eclipse IDE running in Windows/Mac - Yes
#2. From Windows PC -> move the code to Dev Cluster using a tool called WinScp
#3. Submit the code using spark-submit in Dev Cluster to test the job submission in local mode,
# but the code got failed because of the dependent modules are not provided
# What are the dependencies this spark_sql_ETL_ELT_2.py has -
# dependency0 - source data dependency - using the arguments using sys.argv
# dependency1 - from sparkapp.pyspark.utils.common_functions import cls_common_udf - using shutil we build the zip file
# dependency2 (already admin will takecare) - connecting with hive - hive-site.xml kept in the /usr/local/spark/conf/ and mysql connector jar kept in /jars path
# dependency3 - connecting with external db (MYSQL/Oracle/TD/Reshift) - needed driver/connector jar file which should be provided using any one of these 3 options
# option1 (priority1): mention in the spark session as a config("spark.jars","/home/hduser/install/mysql-connector-java3.jar")
# option2 (priority2): keep this jar in some other location and pass it as an arg using --jars mysql-connector-java2.jar
# option3 (priority3): keep in /usr/local/spark/jars/mysql-connector-java1.jar
# dependency4 - connection properties driver, connection url, username, password (hardcoded for now)
# hence I am going to provide the dependent programs in the form of archive because it should occupy less space when
# shipped to all executors at the time of job submission
# shutil.make_archive("/home/hduser/common_functions","zip", root_dir="/home/hduser/PycharmProjects/IzProject/")


##### LIFE CYCLE OF ETL and Data Engineering PIPELINEs
# VERY VERY IMPORTANT PROGRAM IN TERMS OF EXPLAINING/SOLVING PROBLEMS GIVEN IN INTERVIEW , WITH THIS ONE PROGRAM YOU CAN COVER ALMOST ALL DATAENGINEERING FEATURES
'''
Starting point -(Data Governance (security) - Tagging, categorization, classification, masking)
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for further Transformation/Enrichment, Egress/Outbound, analytics & Reporting
a. Data Discovery (every layers ingestion/transformation/consumption) - (Data Governance (security) - Tagging, categorization, classification) (EDA) (Data Exploration) - Performing an (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
b. Combining Data + Schema Evolution/Merging (Structuring)
c. Validation, Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Preprocessing, Preparation
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace),
d. Standardization, De Duplication and Replacement & Deletion of Data to make it in a usable format (Dataengineers/consumers)

2. Data Enrichment - Makes your data rich and detailed
a. Add, Remove, Rename, Modify/replace
b. split, merge/Concat
c. Type Casting, format & Schema Migration

3. Data Customization & Processing - Application of Tailored Business specific Rules
a. User Defined Functions
b. Building of Frameworks & Reusable Functions

4. Data Curation
a. Curation/Transformation
b. Analysis/Analytics & Summarization -> filter, transformation, Grouping, Aggregation/Summarization

5. Data Wrangling - Gathering, Enriching and Transfomation of pre processed data into usable data
a. Lookup/Reference
b. Enrichment
c. Joins
d. Sorting
e. Windowing, Statistical & Analytical processing

6. Data Publishing & Consumption - Enablement of the Cleansed, transformed and analysed data as a Data Product.
a. Discovery,
b. Outbound/Egress,
c. Reports/exports
d. Schema migration
'''

print("***************1. Data Munging *********************")
print("""
1. Data Munging - Process of transforming and mapping data from Raw form into Tidy format with the
intent of making it more appropriate and valuable for a variety of downstream purposes such for analytics/visualizations/analysis/reporting
a. Data Discovery (EDA) - Performing (Data Exploration) exploratory data analysis of the raw data to identify the attributes and patterns.
b. Data Structurizing - Combining Data + Schema Evolution/Merging (Structuring)
c. Validation, 
Cleansing, Scrubbing - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies
Preprocessing, Preparation, Validation, 
Cleansing (removal of unwanted datasets eg. na.drop),
Scrubbing (convert of raw to tidy na.fill or na.replace),
d. Standardization - Column name/type/order/number of columns changes/Replacement & Deletion of columns to make it in a usable format
""")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# define spark configuration object
spark = SparkSession.builder\
   .appName("Very Important SQL End to End App") \
   .config("spark.eventLog.enabled", "true") \
   .config("spark.eventLog.dir", "file:///tmp/spark-events") \
   .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
   .config("spark.jars", "/usr/local/hive/lib/mysql-connector-java.jar") \
   .enableHiveSupport()\
   .getOrCreate()

#.config("spark.jars", "/usr/local/hive/lib/mysql-connector-java.jar")\
# .config("spark.jars", jdbc_lib)\
#spark.conf.set("hive.metastore.uris","thrift://127.0.0.1:9083")
#.config("hive.metastore.uris", "thrift://127.0.0.1:9083") \
# to connect with Remote metastore, but we can't do it in Organization when we develop the pyspark app using Pycharm running in Windows

# Set the logger level to error
spark.sparkContext.setLogLevel("ERROR")

print("a. Raw Data Discovery (EDA) (passive) - Performing an (Data Exploration) exploratory data analysis on the raw data to identify the properties of the attributes and patterns.")
#I will first take some sample data or actual data and analyse about the columns, datatype, values, nulls, duplicates(low/high cardinality), format
#statistical analysis - min/max/difference/mean(mid)/counts

#mode- permissive (default)- permit all the data including the unclean data
#mode- failfast - as soon as you see some unwanted data, fail our program
#mode- dropmalformed - when there are unclean data (doesn't fit with the structure (customschema)/columns are lesser than the defined(custom schema)/identified(inferschema)) don't consider them
custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive')
custdf1.printSchema()
custdf1.show(20,False)

#don't use count rather use len(collect)
print(custdf1.count())
print(len(custdf1.collect()))

custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive',inferSchema=True)
custdf1.printSchema()
#Significant attribute is custid (_c0)
#Data in the first column has string, but it has to be integer, ensure that with he below function
custdf1.where("upper(_c0)<>lower(_c0)").count()
custdf1.show(20,False)
#null check
custdf1.where("_c0 is null").count()
#Data in the first column has null, may create challenges in the further data processing
#duplicate check
custdf1.select("_c0").distinct().count()
#Data in the first column has duplicate

#Let us stop processing this data, since it is unclean
custdf1=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='failfast',inferSchema=True)
custdf1.printSchema()
custdf1.show(20,False)
#inferschema required 5 columns, but only 4 columns are found, so program is failing
#4000006,Patrick,Song,24
#trailer_data:end of file


custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True),
                              StructField("corrupt_data",StringType(),True)])#workaround1 - adding the corrupt_data derived column

custdf3=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='permissive',schema=custstructtype1,columnNameOfCorruptRecord="corrupt_data")
custdf3.printSchema()
custdf3.show(20,False)
custdf3.cache()#workaround2
corruptdt_to_reject_df4=custdf3.select("corrupt_data").where("corrupt_data is not null")
corruptdt_to_reject_df4.write.mode("overwrite").csv("file:///home/hduser/corruptdata")

#TYPE MISMATCH, MISSING COLUMNS - custom schema requires id column should be integer, but it is string and also it requires 5 columns, but only 4 columns are found, so program is failing
#ten,Elsie,Hamilton,43,Pilot
#4000006,Patrick,Song,24
#trailer_data:end of file

custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

#Let us clean and get the right data for further consideration
#drop the unwanted/culprit data while creating the df
#culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)
custdf_clean.printSchema()
custdf_clean.show(20,False)

#How to filter out the corrupted/malformed data alone, to send it to the source system (custom way)
custdf1.select("_c0").subtract(custdf_clean.select(col("id").cast("string"))).show(10,False)

#statistical analysis (Clean data)
custdf_clean.describe().show(20,False)
custdf_clean.summary().show(20,False)

#I realized null values in the key column, wanted to apply the contraint to fail my program incase if the structure is not fulfilled
custdf_clean.rdd.toDF(custstructtype1).show()
custdf_clean.unionByName()
#Exploring of data CONCLUSION - identifying the data quality - failfast(SHOW STOPPER)/permissive(VERACITY)/dropmalformed(CLEANUP), nulls, dups, type mismatch, missing of columns, statistics, rejection of corrupted data

print("b. Combining Data + Schema Evolution/Merging (Structuring)")
print("b.1. Combining Data - Reading from a path contains multiple pattern of files")
'''
mkdir ~/sparkdata/src1
mkdir ~/sparkdata/src1/src2
mkdir ~/sparkdata/src3
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/nyse1.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/nyse2.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/nyse3.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/src2/bse1.csv
cp ~/sparkdata/nyse.csv ~/sparkdata/src1/src2/bse2.csv
'''
spark.read.csv("file:///home/hduser/sparkdata/src1/").count()#all the files in the base path
spark.read.csv("file:///home/hduser/sparkdata/src1/nyse*").count()#all the files in the base+first level of subdir also
spark.read.csv("file:///home/hduser/sparkdata/src1/",recursiveFileLookup=True,sep='~').count() #all files in all sub directories
print("b.2. Combining Data - Reading from a multiple different paths contains multiple pattern of files")
#subdirectories contains multiple pattern of files nyse, bse, nse...
#cp /home/hduser/sparkdata/src1/nyse1.csv src2/
spark.read.csv("file:///home/hduser/sparkdata/src1/",recursiveFileLookup=True,sep='~',pathGlobFilter="nyse[1-2].csv").count()
#The above function will recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv

#main main directories and subdirs contains multiple pattern of files nyse, bse, nse...
spark.read.csv(path=["file:///home/hduser/sparkdata/src1/","file:///home/hduser/sparkdata/src3/"],recursiveFileLookup=True,sep='~',pathGlobFilter="nyse[1-2].csv").count()
#The above function will search in the main and recurse the subdirectories and find the patter of nyse1.csv and nyse2.csv

print("b.3. Schema Merging (Structuring) - Schema Merging data with different structures (we know the structure of both datasets)")
'''

cat ~/sparkdata/stockdata2/bse2.csv
stock~value~maxrate~minrate~dur_mth~incr
TIT~450.3~710~400~6~-10
SUN~754.62~900~500~6~12.0
TMB~1000.2~1210.50~700~6~100.0

cat /home/hduser/sparkdata/stockdata/bse2.csv
stock~value~incr~maxrate~cat
HUL~450.3~-10.0~710~retail
RAN~754.62~12.0~900~pharma
CIP~1000.2~100.0~1210.50~pharma
'''

#Merging data with different structure

#Below methodology leads to a wrong result
df1=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/","file:///home/hduser/sparkdata/stockdata2/"],sep='~',header=True)
df1.show()

#alternative right approach is using unionByName function to achive the above schema merging feature
stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True,header=True)
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata2/"],sep='~',inferSchema=True,header=True)
merged_stock_df=stock_df.unionByName(stock1_df,allowMissingColumns=True)
merged_stock_df.printSchema()
merged_stock_df.show()

#what if if any one of the data has different data type (value is string), but the same column name
'''
cat /home/hduser/sparkdata/stockdata/bse2.csv
stock~value~incr~maxrate~cat
HUL~450.3~neg~710~retail
RAN~754.62~pos~900~pharma
CIP~1000.2~pos~1210.50~pharma
'''

stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True,header=True)
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata2/"],sep='~',inferSchema=True,header=True)
merged_stock_df=stock_df.unionByName(stock1_df,allowMissingColumns=True)
merged_stock_df.printSchema()
merged_stock_df.show()


'''
BSE~HUL~450.3~-10.0
BSE~CIP~754.62~12.0
BSE~TIT~1000.2~100.0

NYSE~CLI~35.3~1.1~EST
NYSE~CVH~24.62~2~EST
NYSE~CVL~30.2~11~EST
'''
stock_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/"],sep='~',inferSchema=True).toDF("excname","stockname","value","incr")
stock1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata1/"],sep='~',inferSchema=True).toDF("excname","stockname","value","incr","tz")
merge_df=stock_df.withColumn("tz",lit("")).union(stock1_df)#this is a workaround to convert both DFs with same number of columns

stock_df.unionByName(stock1_df,allowMissingColumns=True)#modern and auto way of merging the data

#Merging data with different structure from difference souces
#create table stock(excname varchar(100),stockname varchar(100),stocktype varchar(100));
#insert into stock values('NYSE','INT','IT');
dfdb1=spark.read.jdbc(url="jdbc:mysql://localhost:3306/custdb?user=root&password=Root123$",table='stock',properties={'driver':'com.mysql.jdbc.Driver'})
merge_df_structured=dfdb1.unionByName(stock1_df,allowMissingColumns=True)#structurizing

#Can we merge data between different sources
stock1_df.write.json("file:///home/hduser/sparkdata/stockdatajson2/")
stock1_json_df=spark.read.json(["file:///home/hduser/sparkdata/stockdatajson2/"])
merged_stock_df=stock_df.unionByName(stock1_json_df,allowMissingColumns=True)
merged_stock_df.printSchema()

print("b.3. Schema Evolution (Structuring) - source data is evolving with different structure")
#If we receive data from different source or in different time from different sources/same sources of different number of columns each time,
#we can perform schema migration from rawdf to parquetdf/orcdf in application1 and schema evolution in application2
'''
/home/hduser/sparkdata/stockdata
ls -lrt
-rw-rw-r--. 1 hduser hduser 110 Jul 12 07:34 bse1.csv
-rw-rw-r--. 1 hduser hduser 101 Jul 12 08:15 bse2.csv
cat bse1.csv
stock~value~incr~maxrate~cat
HUL~450.3~neg~710~retail
RAN~754.62~pos~900~pharma
CIP~1000.2~pos~1210.50~pharma

cat bse2.csv 
stock~value~incr~maxrate~ipo
HUL~450.3~neg~710~300
RAN~754.62~pos~900~500
CIP~1000.2~pos~1210.50~200
'''
#SCHEMA MIGRATION - write a spark application1 that run once in an hour to schema migrate the hourly data from csv to parquet/orc
stock_hr1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse1.csv"],sep='~',inferSchema=True,header=True)#hour1 data schema migration
stock_hr1_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='overwrite')

stock_hr2_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse2.csv"],sep='~',inferSchema=True,header=True)#hour2 data schema migration
stock_hr2_df=stock_hr2_df.withColumn("incr",col("incr").cast("string"))#workaround for the datatype mismatch issue discussed below
#direct solution is use unionByName option
stock_hr2_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='append')

stock_hr1_df.write.orc("file:///home/hduser/sparkdata/stockorc/")
stock_hr2_df.write.orc("file:///home/hduser/sparkdata/stockorc/",mode='append')


#SCHEMA EVOLUTION - write another spark application2 that run once in a day to schema evolution by reading the parquet/orc data
evolved_stock_hr6_df=spark.read.parquet("file:///home/hduser/sparkdata/stockpar/",mergeSchema=True)
spark.read.orc("file:///home/hduser/sparkdata/stockorc/",mergeSchema=True).show()

#Some limitations:
#1. Column name/order/number of columns can be different, but type has to be same otherwise if type is different mergeschema will fail
#Failed to merge fields 'incr' and 'incr'. Failed to merge incompatible data types string and int

#to overcome the above challenge as a workaround
stock_hr1_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse1.csv"],sep='~',header=True)#hour1 data schema migration
stock_hr1_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='overwrite')
stock_hr2_df=spark.read.csv(["file:///home/hduser/sparkdata/stockdata/bse2.csv"],sep='~',header=True)#hour2 data schema migration
stock_hr2_df.write.parquet("file:///home/hduser/sparkdata/stockpar/",mode='append')
merge_df_parquet=spark.read.parquet("file:///home/hduser/sparkdata/stockpar/",mergeSchema=True)

stoctstruct = StructType([StructField("stock", StringType(), False),
                              StructField("value", StringType(), False),
                              StructField("incr", StringType(), True),
                              StructField("maxrate", StringType(), True),
                              StructField("cat", StringType(), True),
                              StructField("ipo", StringType(), True)])

merge_df_parquet_structured=spark.createDataFrame(merge_df_parquet.rdd,stoctstruct)


print("c.1. Validation (active)- DeDuplication")

custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

#Let us clean and get the right data for further consideration
#drop the unwanted/culprit data while creating the df
#culprit data in this file custsmodified are - _c0 has null, duplicates, datatype mismatch, number of columns mismatch are lesser than 5 for 2 rows
custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)
custdf_clean.printSchema()
custdf_clean.show(20,False)

#Record level de-duplication - retain only one record and eliminate all duplicate records
dedup_df=custdf_clean.distinct()#or dropDuplicates()
dedup_df.where("id=4000001").show(20,False)
dedup_dropduplicates_df=custdf_clean.dropDuplicates()
dedup_dropduplicates_df.where("id=4000001").show(20,False)

#column's level de-duplication - retain only one record and eliminate all duplicate records with custid is duplicated
#dedup_df=custdf_clean.distinct()#or dropDuplicates()
#dedup_df=custdf_clean.dropDuplicates(subset=["id"])#or dropDuplicates()
dedup_dropduplicates_df.dropDuplicates(subset=["id"]).where("id=4000003").show()

#want to retain a particular dup data
dedup_dropduplicates_df=custdf_clean.sort("custage",ascending=False).dropDuplicates(subset=["id"])
dedup_dropduplicates_df.where("id=4000003").show()

#can you write a spark sql to achive the same functionality? let us know writing DSL or SQL is good here?
#DSL wins

#custdf_clean.createGlobalTempView("gview1")#global temp view has the scope across the spark session within the application
#spark.stop()
#spark=SparkSession.builder.getOrCreate()

#Writing the above deduplication using SQL
custdf_clean.createOrReplaceTempView("custview1")
spark.sql("""select distinct * from custview1""").where("id in (4000001,4000003)").show(20,False)
spark.sql("""select id,custfname,custlname,custage,custprofession from custview1 
            where id in (4000001,4000003) 
group by id,custfname,custlname,custage,custprofession""").show(20,False)

#column level dedup using sql (analytical & windowing functions) - very very important for interview
spark.sql("""select id,custfname,custlname,custage,custprofession from 
                          (select id,custfname,custlname,custage,custprofession,
                          row_number() over(partition by id order by custage desc) rownum 
                          from custview1 
                          where id in (4000001,4000002,4000003))tmp 
            where rownum =1 """).show(20,False)

dedup_dropduplicates_df_sql=spark.sql("""select id,custfname,custlname,custage,custprofession from 
                                                   (select id,custfname,custlname,custage,custprofession,
                                                    row_number() over(partition by id order by custage desc) rownum 
                                                    from custview1)tmp 
                                     where rownum =1 """)

print("c.2. Data Preparation (Cleansing & Scrubbing) - Identifying and filling gaps & Cleaning data to remove outliers and inaccuracies")
print("Data Cleansing (removal of unwanted/dorment (data of no use) datasets eg. na.drop, drop malformed)")
print("Dropping the malformed records with all of the columns is null (null will be shown when "
      "datatype mismatch, format mismatch, column missing, blankspace in the data itself or null in the data itself)")

print("Dropping the null records with all of the columns are null ")
print(dedup_dropduplicates_df.na.drop("all").count())
print("Dropping the null records with any one of the column is null ")
print(dedup_dropduplicates_df.na.drop("any").count())
print("Dropping the null records with custid and custprofession is null")
print(dedup_dropduplicates_df.na.drop("all",subset=["id","custprofession"]).count())
print("Dropping the null records with custid or custprofession is null ")
dedup_dropduplicates_df.na.drop("any",subset=["id","custprofession"]).count()

dedup_dropna_clensed_df=dedup_dropduplicates_df.na.drop("any",subset=["id"])

print("set threshold as a second argument if non NULL values of particular row is less than thresh value then drop that row")
dedup_dropduplicates_df.na.drop("any",subset=["id","custprofession"],thresh=2).count()

#understanding thresh
'''
NYSE~~
NYSE~CVH~
NYSE~CVL~30.2
~~~
'''
dft1=spark.read.csv("file:///home/hduser/sparkdata/nyse_thresh.csv",sep='~').toDF("exch","stock","value")
dft1.na.drop("any",thresh=1).show()#drop any row that contains upto zero (<1) not null columns
# (retain all the rows that contains any minimum one not null column)
dft1.na.drop("any",thresh=2).show()#drop any row that contains upto one (<2) not null columns or retain rows with minimum 2 columns with not null
dft1.na.drop("any",thresh=3).show()#drop any row that contains upto two (<3) not null columns or retain rows with minimum 3 columns with not null
dft1.na.drop("any",thresh=4).show()#drop any row that contains upto three (<4) not null columns or retain rows with minimum 4 columns with not null

#na.drop - Parameters:
#how: This parameter is used to determine if the row or column has to remove or not.
#‘any’ – If any of the column value in Dataframe is NULL then drop that row.
#‘all’ – If all the values of particular row or columns is NULL then drop.
#thresh: If non NULL values of particular row or column is less than thresh value then drop that row or column.
#subset: If the given subset column contains any of the null value then drop that row or column.

print("Scrubbing (convert of raw to tidy na.fill or na.replace)")
# Writing DSL
print("If need to write the na.drop functionalities in SQL, is it easy or hard?        ")
print("fill the null with default values ")
dedup_dropna_clensed_df.where("custprofession is null").count()
dedup_dropna_clensed_df.where("custlname is null").show()
dedup_dropfillna_clensed_scrubbed_df1=dedup_dropna_clensed_df.na.fill("na",subset=["custlname","custprofession"])
dedup_dropfillna_clensed_scrubbed_df1.show()
dedup_dropfillna_clensed_scrubbed_df1.where("custprofession = 'na'").show()

print("Replace (na.replace) the key with the respective values in the columns "
       "(another way of writing Case statement)")
prof_dict={"Therapist":"Physician","Musician":"Music Director","na":"prof not defined"}
dedup_dropfillreplacena_clensed_scrubbed_df1=dedup_dropfillna_clensed_scrubbed_df1.na.replace(prof_dict,subset=["custprofession"])
dedup_dropfillreplacena_clensed_scrubbed_df1.show()
#Writing equivalent SQL - for learning the above functionality more relatively & easily and to understand which way is more feasible
dedup_dropduplicates_df_sql.createOrReplaceTempView("dropduplicates_df_sql_view")
dedup_dropfillna_clensed_scrubbed_df1_sql=spark.sql("""select id,custfname,custlname,custage,
                                                        case when custprofession='Therapist' then 'Physician' 
                                                        when custprofession='Musician' then 'Music Director'
                                                        when custprofession='na' then 'prof not defined' 
                                                        else custprofession end as custprofession
                                                        from (select id,custfname,coalesce(custlname,'na') custlname,custage,coalesce(custprofession,'na') custprofession 
                                                     from dropduplicates_df_sql_view 
                                                     where id is not null)temp""")#.where("custprofession='prof not defined'")


print("d.1. Data Standardization (column) - Column re-order/number of columns changes (add/remove/Replacement)  to make it in a usable format")
#select can be used in the starting stage to
#re order or choose the columns or remove or replace using select or add
#re ordering of columns
#DSL Functions to achive reorder/add/removing/replacing respectively select,select/withColumn,select/drop,select/withColumn
#select - reorder/add/removing/replacement/rename
#withColumn - add
#withColumn - Replacement of columns custlname with custfname
#withColumnRenamed - rename of a given column with some other column name
#drop - remove columns

#select - reorder/add/removing/replacement/rename
reord_add_rem_repl_ren_df1=dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession",col("custage").alias("age"),col("custlname").alias("custfname"),lit("retailsystem").alias("sourcesystem"))

reord_df2=dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage","custlname","custfname")#re-order select is the best option
reord_df2.show(10,False)
#apply some transformations using all the above columns .....

#select is not supposed to be used, rather use withColumn, drop, withColumnRenamed
# number of columns changes
# adding of columns (we don't do here, rather in the enrichment we do)
srcsys='Retail'
reord_added_df3=reord_df2.withColumn("srcsystem",lit(srcsys))

#replacement of column(s)
reord_added_replaced_df4=reord_added_df3.withColumn("custfname",col("custlname"))#preffered way if few columns requires drop
#we can do rename/duplicating column/derivation of a column also using withColumn, we will see further down
dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage",col("custlname").alias("custfname"),"srcsystem")
dedup_dropfillreplacena_clensed_scrubbed_df1.select("id","custprofession","custage",upper("custlname").alias("custfname"),"srcsystem")

# removal of columns
chgnumcol_reord_df5=reord_added_replaced_df4.drop("custlname")#preffered way if few columns requires drop
chgnumcol_reord_df6_1=reord_added_replaced_df4.select("id","custprofession","custage","custfname","srcsystem")

#achive replacement and removal using withColumnRenamed
chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumnRenamed("custage","age").show()#preffered way if few columns requires drop
#above function withColumnRenamed("custlname","custfname") did 2 things, 1 - replacement of custfname with custlname and 2 - dropped custlname because
#both the columns exists in the df
#above function withColumnRenamed("custage","age") did 2 things, 1 - created a new column called age and 2 - dropped custage because only custage exists in the df
chgnumcol_reord_df5.withColumnRenamed("custlname","custfname").withColumn("age",col("custage")).drop("custage").show()#equivalent to the above function
#columns will be reordered

#equivalent SQL
dedup_dropfillna_clensed_scrubbed_df1_sql.createOrReplaceTempView("dedup_dropfillna_clensed_scrubbed_view")
chgnumcol_reord_df6_1_sql=spark.sql("""select id,custprofession,custage,custlname as custfname,'Retail' as srcsystem 
from dedup_dropfillna_clensed_scrubbed_view""")#found to be ease of use

#conclusion of functions used:
#yet to add

print("********************data munging completed****************")

print("*************** Data Enrichment (values)-> Add, Rename, combine(Concat), Split, Casting of Fields, Reformat, "
      "replacement of (values in the columns) - Makes your data rich and detailed *********************")
munged_df=chgnumcol_reord_df5
#select,drop,withColumn,withColumnRenamed
#Adding of columns (withColumn/select) - for enriching the data
enrich_addcols_df6=munged_df.withColumn("curdt",current_date()).withColumn("loadts",current_timestamp())#both are feasible
#or
enrich_addcols_df6_1=munged_df.select("*",current_date().alias("curdt"),current_timestamp().alias("loadts"))#both are feasible

#Surrogate key - is a seqnumber column i can add on the given data set if the dataset doesn't contains natural key or
# if i want add one more local surrogate key for better processing of data
#munged_df.orderBy("custage",ascending=True).withColumn("skey",monotonically_increasing_id()).show()

#Rename of columns (withColumnRenamed/select/withColumn & drop) - for enriching the data
enrich_ren_df7=enrich_addcols_df6.withColumnRenamed("srcsystem","src")#preferrable way (delete srcsystem and create new column src without changing the order)
enrich_ren_df7_1=enrich_addcols_df6_1.select("id","custprofession","custage","custfname",col("srcsystem").alias("src"),"curdt","loadts")#not much preferred
enrich_ren_df7_2=enrich_addcols_df6_1.withColumn("src",col("srcsystem")).drop("srcsystem")#costly effort

#Concat to combine/merge/melting the columns
enrich_combine_df8=enrich_ren_df7.select("id","custfname","custprofession",concat("custfname",lit(" is a "),"custprofession").alias("nameprof"),"custage","src","curdt","loadts")
#try with withColumn (that add the derived/combined column in the last)
enrich_combine_df8=enrich_ren_df7.withColumn("nameprof",concat("custfname",lit(" is a "),"custprofession")).drop("custfname")

#Splitting of Columns to derive custfname
enrich_combine_split_df9=enrich_combine_df8.withColumn("custfname",split("nameprof",' ')[0])

#Casting of Fields
enrich_combine_split_cast_df10=enrich_combine_split_df9.withColumn("curdtstr",col("curdt").cast("string")).withColumn("year",year(col("curdt"))).withColumn("yearstr",substring("curdtstr",1,4))

#Reformat same column value or introduce a new column by reformatting an existing column (withcolumn)
enrich_combine_split_cast_reformat_df10=enrich_combine_split_df9.withColumn("curdtstr",col("curdt").cast("string")).withColumn("year",year(col("curdt"))).withColumn("curdtstr",concat(substring("curdtstr",3,2),lit("/"),substring("curdtstr",6,2))).withColumn("dtfmt",date_format("curdt",'yyyy/MM/dd hh:mm:ss'))

#try with ansi SQL
#enrich_combine_split_cast_reformat_df10
#SQL equivalent using inline view/from clause subquery
spark.sql("""select id,custprofession,custage,src,curdt,loadts,nameprof,split(nameprof,' ')[0] as custfname,
             concat(substr(curdt,3,2),'/',substr(curdt,6,2)) curdtstr,year(curdt) year,
             date_format(curdt,'yyyy/MM/dd hh:mm:ss') dtfmt
                   from(
                        select id,custprofession,custage,srcsystem as src,
                               cast (current_date() as string) curdt,
                               current_timestamp() loadts,
                               concat(custfname,' is a ',custprofession) nameprof                        
                        from mungedview1
                       ) temp""").show()

#****************Data Enrichment Completed Here*************#

print("***************3. Data Customization & Processing -> Apply User defined functions and utils/functions/modularization/reusable framework creation *********************")
munged_enriched_df=enrich_combine_split_cast_reformat_df10
print("Data Customization can be achived by using UDFs - User Defined Functions")
print("User Defined Functions must be used only if it is Inevitable (un avoidable), because Spark consider UDF as a black box doesn't know how to "
      "apply optimization in the UDFs - When we have a custom requirement which cannot be achieved with the existing built-in functions.")
#Interview Question: Hi Irfan, Whether you developed any UDF's in your project?
# Yes, but very minumum numbers we have and I developed 1/2 UDFs - (data masking, filteration (CPNI,GSAM),data profiling/classification, custom business logic like promo ratio calculation, customer intent identification)
# But, we should avoid using UDFs until it is Inevitable
#some realtime examples when we can't avoid using UDFs

#case1: How to avoid using UDFs
#step1: Create a function (with some custom logic) or download a function/library of functions from online writtern in python/java/scala...
convertToUpperPyLamFunc=lambda prof:prof.upper()

#step2: Import the udf from the spark sql functions library
from pyspark.sql.functions import udf

#step3: Convert the above function as a user defined function (which is DSL ready)
convertToUpperDFUDFFunc=udf(convertToUpperPyLamFunc)

#step3: Convert and Register (in Spark Metastore) the above function as a user defined function (which is SQL ready)
spark.udf.register("convertToUpperDFSQLFunc",convertToUpperPyLamFunc)

#step4: Apply the UDF to the given column(s) of the DF using DSL program
customized_munged_enriched_df=munged_enriched_df.withColumn("custprofession",convertToUpperDFUDFFunc("custprofession"))
customized_munged_enriched_df.show(2)

#using builtin(predefined) function (avoid applying the above 4 steps and get the result directly)
predefined_munged_enriched_df=munged_enriched_df.withColumn("custprofession",upper("custprofession"))
predefined_munged_enriched_df.show(2)

#SQL Equivalent
customized_munged_enriched_df.createOrReplaceTempView("view1")
spark.sql("select id,convertToUpperDFSQLFunc(custprofession) custprofession,custage,src,curdt,loadts,nameprof,custfname,curdtstr,year from view1").show(2)
spark.sql("select id,upper(custprofession) custprofession,custage,src,curdt,loadts,nameprof,custfname,curdtstr,year from view1").show(2)

#In the above case, usage of built in function is better - built in is preferred, if not available go with UDF

#Quick Usecase:
#Write a python def function to calculate the age grouping/categorization of the people based on the custage column, if age<13 - childrens, if 13 to 18 - teen, above 18 - adults
#derive a new column called age group in the above dataframe (using DSL)
#Try the same with the SQL also

#Below utils/functions/modularization/reusable framework are suggested to Standardize the code
def convertFileToDF(sparksess,loc,de,md):
 df=sparksess.read.option("delimiter",de).csv(loc,mode=md)
 return df


df1=convertFileToDF(spark,"file:///home/hduser/hive/data/custsmodified",',','permissive')

print("*************** Core Data Processing/Transformation (Level1) (Pre wrangling) Curation/Processing -> filter, transformation, Grouping, Aggregation/Summarization,Analysis/Analytics *********************")
#Transformation Functions -> select, filter, sort, group, aggregation, having, transformation/analytical function, distinct...
pre_wrangled_customized_munged_enriched_df=customized_munged_enriched_df.select("id","custprofession","custage","src","curdt")\
    .where("convertToUpperDFSQLFunc(custprofession)='FIREFIGHTER' or custprofession='WRITER'")\
    .groupBy("custprofession")\
    .agg(avg("custage").alias("avgage"))\
    .where("avgage>49")\
    .orderBy("custprofession")

pre_wrangled_customized_munged_enriched_df
print("*************** Core Data Processing/Transformation (Level2) Data Wrangling -> Join, Lookup & Enrichment, Denormalization, Summarization, Windowing, Analytical *********************")
print("*************** Data Persistance -> Discovery, Outbound, Reports, exports, Schema migration  *********************")
