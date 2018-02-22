#
# NB might need to pip install findspark, pyspark first
# Required to allow pyspark to run inside Jupyter
#
# At the time of writing, pyspark does not work with python 3.6
# therefore using version 3.5.2 
#

import findspark
findspark.init()
import pyspark

sc = pyspark.SparkContext(appName="read-big-file")


from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .getOrCreate()


df = spark.read.format("com.databricks.spark.csv") \
    .option("header", "false").option("inferSchema", "true") \
    .option("delimiter", '|').load("file:///d:/tmp/iholding/issueholding.txt")

df.printSchema()

# Output of above printSchema() command is shown below
# This is not part of the script
root
 |-- _c0: integer (nullable = true)
 |-- _c1: integer (nullable = true)
 |-- _c2: integer (nullable = true)
 |-- _c3: timestamp (nullable = true)
 |-- _c4: integer (nullable = true)
 |-- _c5: integer (nullable = true)
 |-- _c6: integer (nullable = true)
 |-- _c7: double (nullable = true)
 |-- _c8: double (nullable = true)
 |-- _c9: integer (nullable = true)
 |-- _c10: string (nullable = true)
 |-- _c11: integer (nullable = true)
 |-- _c12: integer (nullable = true)
 |-- _c13: string (nullable = true)


df.show(10)

# Output of above show() command is shown below
# This is not part of the script

+-----+---+-------+--------------------+---+------+---+----+-------+------+----+----+----+----+
|  _c0|_c1|    _c2|                 _c3|_c4|   _c5|_c6| _c7|    _c8|   _c9|_c10|_c11|_c12|_c13|
+-----+---+-------+--------------------+---+------+---+----+-------+------+----+----+----+----+
|18511|  1|2587198|2004-03-31 00:00:...|  0|100000|  0|1.97|0.49988|100000|null|null|null|null|
|18511|  2|2587940|2004-03-31 00:00:...|  0|240000|  0|0.78|0.27327|240000|null|null|null|null|
|18511|  3|2588002|2004-03-31 00:00:...|  0|453200|  0| 4.7|0.76109|453200|null|null|null|null|
|18511|  4|2588016|2004-03-31 00:00:...|  0|340000|  0|2.96|0.38092|340000|null|null|null|null|
|18511|  5|2589768|2004-03-31 00:00:...|  0|211500|  0| 6.7|0.01652|211500|null|null|null|null|
|18511|  6|2590566|2004-03-31 00:00:...|  0|  4273|  0|1.09|1.35573|  4273|null|   5|   5|null|
|18511|  7|2591388|2004-03-31 00:00:...|  0|343181|  0|4.28|0.92817|343181|null|null|null|null|
|18511|  8|2591976|2004-03-31 00:00:...|  0|129100|  0|0.38|0.63429|129100|null|  16|  16|null|
|18511|  9|2594536|2004-03-31 00:00:...|  0| 36196|  0|0.92|2.25445| 36196|null|   3|   3|null|
+-----+---+-------+--------------------+---+------+---+----+-------+------+----+----+----+----+



# When we do a partitionbykey each field gets output except the key field,
# therefore we add an "issue" field on to the end of the dataframe that's a copy of the 
# second field (the one we want to partition on) so that all of the original fields 
# get output when we do the partitioning

newdf = df.withColumn("period", df[1])

newdf.printSchema()

# Output of above printSchema() command is shown below
# This is not part of the script

root
 |-- _c0: integer (nullable = true)
 |-- _c1: integer (nullable = true)
 |-- _c2: integer (nullable = true)
 |-- _c3: timestamp (nullable = true)
 |-- _c4: integer (nullable = true)
 |-- _c5: integer (nullable = true)
 |-- _c6: integer (nullable = true)
 |-- _c7: double (nullable = true)
 |-- _c8: double (nullable = true)
 |-- _c9: integer (nullable = true)
 |-- _c10: string (nullable = true)
 |-- _c11: integer (nullable = true)
 |-- _c12: integer (nullable = true)
 |-- _c13: string (nullable = true)
 |-- period: integer (nullable = true)
 

newdf.show(10)

# Output of above show() command is shown below
# This is not part of the script

+-----+---+-------+--------------------+---+------+---+----+-------+------+----+----+----+----+------+
|  _c0|_c1|    _c2|                 _c3|_c4|   _c5|_c6| _c7|    _c8|   _c9|_c10|_c11|_c12|_c13|period|
+-----+---+-------+--------------------+---+------+---+----+-------+------+----+----+----+----+------+
|18511|  1|2587198|2004-03-31 00:00:...|  0|100000|  0|1.97|0.49988|100000|null|null|null|null|    1 |
|18511|  2|2587940|2004-03-31 00:00:...|  0|240000|  0|0.78|0.27327|240000|null|null|null|null|    2 |
|18511|  3|2588002|2004-03-31 00:00:...|  0|453200|  0| 4.7|0.76109|453200|null|null|null|null|    3 |
|18511|  4|2588016|2004-03-31 00:00:...|  0|340000|  0|2.96|0.38092|340000|null|null|null|null|    4 |
|18511|  5|2589768|2004-03-31 00:00:...|  0|211500|  0| 6.7|0.01652|211500|null|null|null|null|    5 |
|18511|  6|2590566|2004-03-31 00:00:...|  0|  4273|  0|1.09|1.35573|  4273|null|   5|   5|null|    6 |
|18511|  7|2591388|2004-03-31 00:00:...|  0|343181|  0|4.28|0.92817|343181|null|null|null|null|    7 |
|18511|  8|2591976|2004-03-31 00:00:...|  0|129100|  0|0.38|0.63429|129100|null|  16|  16|null|    8 |
|18511|  9|2594536|2004-03-31 00:00:...|  0| 36196|  0|0.92|2.25445| 36196|null|   3|   3|null|    9 |
+-----+---+-------+--------------------+---+------+---+----+-------+------+----+----+----+----+------+


# Save thedata to various files, overwriting any exsisting data 
#
# d:/tmp/myfiles
#       -> period=1 
#               -> part-r-00000
#               -> part-r-00001
#               -> part-r-00002
#       -> period=2 
#               -> part-r-00000
#               -> part-r-00001
#               -> part-r-00002
#
#              etc ....
#
newdf.write.partitionBy("period") \
    .format("com.databricks.spark.csv") \
    .option("header", "false") \
    .option("delimiter", '|').mode("overwrite").save("d://tmp/myfiles")


# If we just wanted one output file per issue groupbykey we could 
# change the above command slightly to include the coalesce option,
# like :

newdf.coalesce(1).write.partitionBy("period") \
    .format("com.databricks.spark.csv") \
    .option("header", "false") \
    .option("delimiter", '|').mode("overwrite").save("d://tmp/myfiles")

