#Very High level
#How we are going to learn Spark SQL (Ingestion/ETL/ELT):
#HOW to write a typical spark application

#spark_sql_EL_1.py start
#1. how to create dataframes from RDD using named list/reflection (Row object) (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
#2. how to create DF from different (builtin)sources by inferring the schema automatically or manually defining the schema (very important)
#3. how to store the transformed data to targets(builtin)
#spark_sql_EL_1.py end

#spark_sql_ETL_2.py start
#4. how to apply transformations using DSL(DF) and SQL(view) (main portition level 1)
#5. how to create pipelines using different data processing techniques by connecting with different sources/targets (level 2)
#above and beyond
#6. how to create generic/reusable frameworks (level 3) - masking engine, reusable transformation, data movement automation engine, quality suite, audit engine, data observability, data modernization...
#spark_sql_ETL_2.py end

#7. how to the terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment ...
#8. performance tuning
#9. Deploying spark applications in Cloud
#10. Creating cloud pipelines using spark SQL programs

#HOW to write a typical spark application

from pyspark.sql.session import SparkSession
spark=SparkSession.builder.master("local[2]").appName("WD30 Spark Core Prog learning").enableHiveSupport().getOrCreate()
sc=spark.sparkContext
#sqlContext=SQLContext(sc)
sc.setLogLevel("ERROR")
####1. creating DF from rdd - starts here######
#1. (not much important) how to create (representation) dataframes from RDD using named list/reflection (Row object)
# (not preferred much (unless inevitable)/least bothered/not much important because we preferably create direct DF rather than RDD) and
#How to convert an rdd to dataframe?Not so much important, because we don't create rdd initially to convert to df until it is in evitable
#1. Create an RDD, Iterate on every rdd1 of rdd, split using the delimiter,
rdd1=sc.textFile("file:///home/hduser/sample.dat").map(lambda x:x.split(","))
#2. Iterate on every splitted elements apply the respective, datatype to get the SchemaRDD
schema_rdd2=rdd1.map(lambda x:(int(x[0]), x[1], int(x[2])))
#filter_rdd=schema_rdd2.filter(lambda x:x[2]>100000)
#print(filter_rdd.collect())#rdd programming
#created using reflection
#schema_rdd2.collect()
#create dataframe further, to simplify the operation performed on the data
#3. Create column list as per the the structure of data
collist=["id","name","sal"]
#from pyspark.sql import Row
#row_rdd=rdd1.map(lambda cols:Row(id=int(cols[0]), name=cols[1], sal=int(cols[2])))
# 4. Convert the schemaRDD to DF using toDF or createDataFrame apply the column names by calling the column lists
df1=spark.createDataFrame(schema_rdd2,collist)#this createDF will convert the schemardd --> row_rdd (internally) --> DF
#or
df1=schema_rdd2.toDF(collist)
#[Row(id=1, name='irfan', sal=100000), Row(id=2, name='inceptez', sal=200000)]
#or alternatively, create row object directly and run dsl queries
#using concept of reflection (reflect each row of the schema rdd to the Row class to get the row objects created)

from pyspark.sql.types import Row
rdd1=sc.textFile("file:///home/hduser/sample.dat").map(lambda x:x.split(","))
row_rdd1=rdd1.map(lambda cols:Row(id=int(cols[0]), name=cols[1], sal=int(cols[2])))#if we reprepresent data from notepad to xls (colname and dtype)
df_rowrdd=spark.createDataFrame(row_rdd1)
#df1=spark.read.csv("file:///home/hduser/sample.dat")# all the above 4 steps are done by this one function (csv)
df1.select("*").where ("sal >100000").show()#dsl (spark domain specific language) query
df_rowrdd.select("*").where ("sal >100000").show()#dsl (spark domain specific language) query

#Rather writing the code like above (either schemardd+collist or rowrdd to DF) (create rdd then converting to DF),
# preferable write the code (directly create DFs) as given below
df1=spark.read.option("inferschema","true").csv("file:///home/hduser/sample.dat").toDF("id","name","sal")
df1.select("*").where ("sal >100000").show()#dsl programming

df1.createOrReplaceTempView("sample_view")#sql programming (familiar)
spark.sql("select * from sample_view where sal>100000").show()

#A typical example for forcefully creating rdd then df then temp view query.
#1. create an rdd to convert the data into structured from unstructured
#2. represent rdd as dataframe
#3. write dsl queries
#or
#4. represent df as tempview (since sql is simple and more familiar)
#5. write sql queries
rdd1=sc.textFile("file:///home/hduser/mrdata/courses.log").flatMap(lambda x:x.split(" ")).map(lambda x:(x,))#functional transformation
collst=["coursename"]
#we cant avoid creating rdd, hence create rdd and represent as DF (ASAP)
df1=spark.createDataFrame(rdd1,collst)
df1.select("coursename").distinct().show()#dsl (declarative)
df1.createOrReplaceTempView("course_view")
spark.sql("select distinct coursename from course_view").show()#sql (declarative)

#named_col_list + schemaRdd -> toDF/createDataFrame
#Row + schemaRdd = RowRDD (Reflection) -> toDF/createDataFrame
####1. creating DF from rdd - ends here######

#How to write a typical python based spark application??
#hierarchy of writing code in python? pkg.subpkg.module.class.methods/function #framework developers have did this
#What we are going to do as a developer - pkg.subpkg.module.main(method).programs to run (leverage the classes/function developed already) (instantiating the class and use the functions)

#2. how to create DF from different sources (using different options) by inferring the schema automatically or manually defining the schema (very important)
####creating DF directly from different sources - starts here (very important)######
#when we create spark session object in the name of spark (it comprise of sparkcontext, sqlcontext, hivecontext)
#where we need spark context - to write rdd programs (manually or automatically)
#where we need sql context - to write dsl/sql queries
#where we need hive context - to write hql queries
#sc=spark.sparkContext#for spark sql program, sc is indirectly used

#creating DFs directly
#1. spark session object importance
#2. The methods with options we can use under read and write module/method of the spark session
#3. csv module important option -> (option/options/inline arguments/format & load) inferschema, header, delimiter/sep
df1=spark.read.csv("file:///home/hduser/sample.dat")#indirectly calling sqlContext that inturn wraps sparkContext
df1.select("*").show()#column names starts with _c0, _c1... with default delimiter , with default datatype String

#2. Create dataframes using the modules (builtin (csv, orc, parquet, jdbc,table) / external (cloud, api, nosql))
# or programatically
#****Possible way of calling the csv function with different options
#option(opt1,value1), options(opt1=value,opt2=value), csv(filename,opt1=value,opt2=value), format(csv).option/options.load("file")
delfile_df1=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv")#this will create df with default comma delimiter
delfile_df2=spark.read.option("delimiter","~").csv("file:///home/hduser/sparkdata/nyse.csv")#this will create df with custom ~ delimiter
print(delfile_df1.printSchema())#column names will _c0,c1 and datatype string
#multiple options to call the default functions like csv
delfile_df3=spark.read.option("delimiter","~").option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse.csv")#this will create df with custom ~ delimiter and automatically infer schema (dataype)
#or if we have multiple options to be used, then go with the below ways
delfile_df4=spark.read.options(delimiter="~",inferschema=True).csv("file:///home/hduser/sparkdata/nyse.csv")
delfile_df5=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep='~',inferSchema=True)

#the below way of creating df is (not suggested) for the built in modules such as csv, json, orc ... legacy approach
delfile_df6=spark.read.format("csv").options(delimiter="~",inferschema=True).load("file:///home/hduser/sparkdata/nyse.csv")
#column names will _c0,c1 and datatype automatically inferred String, String, double

# How to define column names for the dataframe at the time of creation or how to change the column names (later)
#if header is not available, i will go simply toDF("column list")
delfile_df7=spark.read.csv("file:///home/hduser/sparkdata/nyse.csv",sep='~',inferSchema=True).toDF("exchange","stock","value")
#column names will "exchange","stock","value" and datatype inferred automatically
delfile_df7.printSchema()

#if header is available, i will go simply with header=true
delfile_df8_header=spark.read.option("header","true").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df8_header.printSchema()#single column

#if i dont mention header or header,false?
delfile_df8_1_header=spark.read.option("header","false").option("inferschema","true").option("delimiter","~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df8_1_header.printSchema()#
#it consider the first row and then assume the remaining also as the same
#it wont consider the first line as header, rather as a data
#column names will not be considered, so default _c0,_c1 .. will be used

#or
#preference 2 if one/two regular option are going to be used
delfile_df8_header=spark.read.option("header","true").option("inferschema","true").option("delimiter","~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df8_header.printSchema()#multiple columns
#or
#preference 1 if regular options are going to be used
delfile_df9_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df9_header.printSchema()#multiple columns
#or
#preference 1 if additional options are going to be used
delfile_df10_header=spark.read.csv("file:///home/hduser/sparkdata/nyse_header.csv",header="true",inferSchema="true",sep="~")
delfile_df10_header.printSchema()#multiple columns
#or
#preference 3 if other sources we are going to use
delfile_df11_header=spark.read.format("csv").options(header="true",inferschema="true",delimiter="~").load("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df11_header.printSchema()#multiple columns

#if header is available in the data, but need to change MOST of the column names
#toDF(colname1,colname2)
#exchange_name~stock_name~closing_rate
#NYSE~CLI~35.3
#NYSE~CVH~24.62
#NYSE~CVL~30.2
delfile_df12_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df12_header.printSchema()
#exchange_name~stock_name~closing_rate -> exchange~stock~rate
delfile_df12_1_header=spark.read.options(header="true",inferschema="true",delimiter="~")\
    .csv("file:///home/hduser/sparkdata/nyse_header.csv").toDF("exchange","stock","rate")
delfile_df12_1_header.printSchema()

#if header is available but need to change few of the column names
#withColumnRenamed
delfile_df13_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df13_header.printSchema()
delfile_df13_1_withColumnRenamed=delfile_df13_header.withColumnRenamed("exchange_name","exch")#to rename one
delfile_df13_1_withColumnRenamed.printSchema()
delfile_df13_2_withColumnRenamed=delfile_df13_header.withColumnRenamed("exchange_name","exch").withColumnRenamed("closing_rate","close_rate")#to rename one
delfile_df13_2_withColumnRenamed.printSchema()
#alias
from pyspark.sql.functions import col
delfile_df13_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
delfile_df13_1_alias=delfile_df13_header.select(col("exchange_name").alias("exch"),"stock_name",col("closing_rate").alias("close_rate"))#DSL #to rename one or more columns
delfile_df13_1_alias.printSchema()
#or
#convert df to tempview and write ansi sql
delfile_df13_header.createOrReplaceTempView("view1_header")
delfile_df13_2_alias=spark.sql("select exchange_name exch,stock_name,closing_rate as close_rate from view1_header")#familiar sql
delfile_df13_2_alias.printSchema()

#If header is available need to change few column name after applying some transformations/aggregations then use .alias()
#do aggregation
delfile_df13_header=spark.read.options(header="true",inferschema="true",delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
aggr_df14_1_alias=delfile_df13_header.groupby("exchange_name").agg(min(col("closing_rate")).alias("min_close_rate"))#dsl way
aggr_df14_1_alias.show()#alias is used just for proper column name definition
aggr_df14_1_alias.write.saveAsTable("hive_tbl11")#alias is mandatory for the column name definition as per hive table columname standard
#If we want to save the df in hive, then Alias/renaming of the column is mandatory - provided if the column names are not as per the hive standard

#If header is available need to change few column name after applying some transformations/aggregations then use SQL functions also
delfile_df13_header.createOrReplaceTempView("view1")
df_to_hive_json_orc=spark.sql("select exchange_name,min(closing_rate) as min_close_rate from view1 group by exchange_name")#ISO/ANSI SQL
df_to_hive_json_orc.write.saveAsTable("hive_tbl12")
df_to_hive_json_orc.write.json("file:///home/hduser/narashimanjson2")
df_to_hive_json_orc.write.orc("file:///home/hduser/narashimanorc")

#****very very important item to know for sure by everyone...
#I don't wanted to infer the schema (columname & datatype & null (constraint)), rather I want to define the custom schema?

#performance optimization - dont use inferschema unless it is inevitable like the given example below
#If header is not available need to define column names and the DATATYPE also - we can simply go with inferschema and toDF to achieve it,
#but not a preferred/optimistic solution
#inferschema="true",
df14_noheader_inferschema=spark.read.options(delimiter="~",inferschema=True).csv("file:///home/hduser/sparkdata/nyse.csv").toDF("exch_name","stock_name","closed_rate")
#inferschema - must be avoided to use in the dataframe creation
#we can avoid scanning the whole data to infer the schema - read all records and identify the data type
#applies some default datatype like double, which will not allow me to define cust datatypes (It may leads to incorrect datatype)
#When do we use inferschema then -
#           If the volume of data is small and number of columns are more
#           If we are good with the format what inferschema produces, then we can use.
#           For Testing, data validation, allowing all column values to be identified (eg. do we have any non integer type in integer column)
df14_noheader_inferschema1=spark.read.options(delimiter="~",inferschema=True).csv("file:///home/hduser/sparkdata/nyse_vinoth.csv").toDF("exch_name","stock_name","closed_rate")
df14_noheader_inferschema1.where("upper(closed_rate)<>lower(closed_rate)").show()
#inferschma indentifies entire COLUMN as STRING even if one column in a record is number
#inferschma indentifies entire column as string even if one record with the given integer column is string

#If header is not available need to define column names and the DATATYPE also - we need to go with custom schema, use schema option to achieve this
#Structure Type & Structure Fields
from pyspark.sql.types import StructType,StructField,StringType,FloatType#better way
#from pyspark.sql.types import *#not preferred way
#format for defining structure
#strct=StructType([StructField("col1name",DataType(),False),StructField("col2name",DataType(),True),StructField("col3name",DataType(),False)])
strct=StructType([StructField("exch_name",StringType(),False),StructField("stock_name",StringType(),True),StructField("close_rate",FloatType(),True)])
df14_noheader_customschema=spark.read.schema(strct).options(delimiter="~").csv("file:///home/hduser/sparkdata/nyse.csv")
df14_noheader_customschema.printSchema()
df14_noheader_customschema.show()
#If header is available need to define column name and the DATATYPE also - we need to go with custom schema, use schema option to achieve this
from pyspark.sql.types import StructType,StructField,StringType,FloatType#better way
strct=StructType([StructField("exch_name",StringType(),False),StructField("stock_name",StringType(),True),StructField("close_rate",FloatType(),True)])
df15_header_customschema=spark.read.schema(strct).options(header=True,delimiter="~").csv("file:///home/hduser/sparkdata/nyse_header.csv")
df15_header_customschema.printSchema()
df15_header_customschema.show()

#I have only few columns (5) only to process, do you want to create structtype for all the columns (100 columns) in the given source data?


#Reading/importing & Writing/exporting the data from Databases (Venkat's usecase)

#writing the df into files/tables

#spark sql can handle semi structured data


#reading orc data


#reading parquet data


#read data from hive table
#use the below way of reading/writing/creating hive tables if you are going use the below hive table only in spark applications

#use the below way of reading/writing/creating hive tables if you are going use the below hive table in both spark application and hive promt also

#hive usecase -
# usecase1: raw(csv) -> DE curation -> hive table -> new spark app DS model -> hive table -> visualization
#DE team will do the below steps in spark application 1
from pyspark.sql.functions import *
df_csv1=spark.read.option("header","True").option("delimiter","~").option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse.csv")
# approach1 for handling usecase1 is load the csv data in a df -> apply transformations -> saveAsTable() -> DS app will use spark.read.table()
df_transformed=df_csv1.withColumn("stock",lower(col("stock"))).withColumn("numeric_closerate",col("closerate").cast("int")).withColumn("curdt",current_date())
df_transformed.write.mode("overwrite").saveAsTable("hive_curated_table_for_DS")

#ds team is complaining the query is not performing well, so DE team stores the data applying partitions
df_transformed.write.partitionBy("stock").mode("overwrite").saveAsTable("hive_curated_table_for_DS_part1")
datascience_part_df1=spark.read.table("hive_curated_table_for_DS_part1")
ds_df2=datascience_part_df1.where("stock='hul'").dropDuplicates().na.drop("any")

#DS team will do the below steps in spark application 2
datascience_df1=spark.read.table("hive_curated_table_for_DS")
ds_df2=datascience_df1.dropDuplicates().na.drop("any")
ds_df2.write.saveAsTable("hive_final_table_for_visualization")

# approach2 for handling usecase2 is for creating external tables and to make my end users leverage all hql functionalities, which will not be supported
# by the spark saveAsTable function
# load the csv data in a df ->convert the DF into Tempview ->  apply transformations using sql->
# spark.sql("create table as select"/"insert select")
# -> users may analyse the data in hive cli as an external table  or new spark app (DS model) -> hive table -> visualization
df_csv1=spark.read.option("header","True").option("delimiter","~").option("inferschema","true").csv("file:///home/hduser/sparkdata/nyse.csv")
df_csv1.createOrReplaceTempView("df_csv1_tv")
spark.sql("create table hive_curated_table_for_DS1 as "
          "select exchange,lower(stock) as stock,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt "
           "from df_csv1_tv")
# or if the target already present, use the below way..
#spark.sql("create table hive_curated_table_for_DS2 (exchange string,stock string,closerate decimal(10,2),numeric_closerate int,curdt date)")
spark.sql("insert overwrite table hive_curated_table_for_DS2 "
          "select exchange,lower(stock) as stock,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt "
           "from df_csv1_tv")

spark.sql("create table hive_curated_table_for_DS_part_hive (exchange string,closerate decimal(10,2),numeric_closerate int,curdt date) partitioned by (stock string)")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table hive_curated_table_for_DS_part_hive partition(stock)"
          "select exchange,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt ,lower(stock) as stock "
           "from df_csv1_tv")

# Important interview question? Can we create external table in hive using spark? if so how to create?
#yes, by only using the hive native sql and not by using spark dsl functions
spark.sql("create external table ext_hive_curated_table_for_DS_part_hive (exchange string,closerate decimal(10,2),"
          "numeric_closerate int,curdt date) partitioned by (stock string) location 'hdfs://localhost:54310/user/hduser/ext_hive_curated_table_for_DS_part_hive'")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("insert overwrite table ext_hive_curated_table_for_DS_part_hive partition(stock)"
          "select exchange,closerate,"
          "cast(closerate as int) numeric_closerate,current_date() as curdt ,lower(stock) as stock "
           "from df_csv1_tv")


#read data from other database tables
sqldf = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/custdb").option("driver","com.mysql.jdbc.Driver").option("dbtable","custprof").option("user", "root").option("password", "Root123$").load()

#we have another way to read/write the data from RDBMS
#can we tranfer data between one spark app to another ? not directly, but by persisting the app1 data to fs/hive/db table in a serialized format
# raw(csv) -> DE curation -> parquet/hive table -> new spark app DS model -> hive table -> visualization