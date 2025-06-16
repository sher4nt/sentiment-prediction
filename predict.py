import sys
import pandas as pd
import numpy as np
from joblib import load
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *

test_path, pred_out, model_path = (
    sys.argv[1],
    sys.argv[2],
    sys.argv[3]
)

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

schema = StructType([
    StructField("id", IntegerType()),
    StructField("features", ArrayType(DoubleType())),
])

df = spark.read.parquet(test_path)

model = load(model_path)
sparkModel = spark.sparkContext.broadcast(model)

@pandas_udf(DoubleType())
def predict_udf(features_series):
    X = np.stack(features_series.values)
    return pd.Series(model.predict(X))

df = df.withColumn('prediction', predict_udf('features'))
df.select("id", "prediction").write.mode("overwrite").csv(pred_out)