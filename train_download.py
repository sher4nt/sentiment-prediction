import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

path_in, path_out = sys.argv[1], sys.argv[2]

schema = StructType([
    StructField("label", IntegerType()),
    StructField("features", ArrayType(DoubleType())),
])

df = spark.read.parquet(path_in)
df = df.select(['label'] + [col("features")[i] for i in range(100)])
df.toPandas().to_csv(path_out, index=False)