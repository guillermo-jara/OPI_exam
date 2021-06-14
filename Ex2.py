# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 01:41:38 2021

@author: enano
"""


import findspark
import pyspark
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
import pyspark.sql.functions as F
from pyspark import SparkContext

findspark.init()

sc=SparkContext("local", "Ex2")
spark=SparkSession(sc)

df=spark.read.csv('file:///M:/Documentos/OPI/demo.csv', header=True, inferSchema=True)

country_dict=df.select(F.col('country')).drop_duplicates()

window=Window.orderBy(F.col('country'))
country_dict=country_dict.withColumn('id_country', F.row_number().over(window))

country_dict.coalesce(1).write.format('com.databricks.spark.csv').save('country_dict.csv',header = 'true')






country_dict=spark.read.csv('file:///M:/Documentos/OPI/country_dict.csv', header=True, inferSchema=True)
df1=df.join(country_dict, how='inner', on='country')



