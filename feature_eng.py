import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import *
from pyspark.ml.functions import vector_to_array


spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')


schema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("label", IntegerType(), nullable=True),
    StructField("reviewText", StringType(), nullable=True),
])

path_in, path_out = sys.argv[1], sys.argv[2]

test_flag = False
if 'test' in path_out:
    test_flag = True

df = (spark.read
      .option("header", "true")
      .schema(schema)
      .json(path_in)).fillna({"reviewText": "missingreview"})

tokenizer = Tokenizer(inputCol='reviewText', outputCol='words')
hasher = HashingTF(numFeatures=100, inputCol=tokenizer.getOutputCol(), outputCol='hashed')

pipeline = Pipeline(stages=[
    tokenizer,
    hasher,
])

df = pipeline.fit(df).transform(df)
df = df.withColumn('features', vector_to_array('hashed'))

if test_flag:
    df.select('id', 'features').write.mode('overwrite').save(path_out)
else:
    df.select('label', 'features').write.mode('overwrite').save(path_out)