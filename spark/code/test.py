from pyspark.sql import SparkSession
#import pandas as pd
import time
import os
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import numpy

#SparkContext.stop(sc)
time.sleep(80)
path = r"/spark/jars/spark-cassandra-connector_2.12-3.2.0.jar"
if os.path.exists(path):
    print('Драйвер обнаружен')

spark = SparkSession.builder \
    .appName("clustering") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.jars.packages", r"/spark/jars/spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.jars", r"/spark/jars/spark-cassandra-connector_2.12-3.2.0.jar") \
    .config("spark.jars", r"/spark/jars/java-driver-core-shaded-4.13.0.jar") \
    .config("spark.jars", r"/spark/jars/spark-cassandra-connector-assembly_2.12-3.2.0.jar") \
    .config("spark.jars.packages", "com.github.jnr:jnr-posix:3.1.15") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.memory.offHeap.size", "6g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .getOrCreate()
# .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
#     .config('spark.dse.continuousPagingEnabled', False) \
#     .config('dataproc.conscrypt.provider.enable', False) \
#      .config("spark.jars", r"/spark/jars/spark-cassandra-connector_2.12-3.3.0.jar") \
#    .config("spark.jars", r"/spark/jars/spark-cassandra-connector-driver_2.12-3.3.0.jar") \
#    .config("spark.jars", r"/spark/jars/java-driver-core-shaded-4.13.0.jar") \
df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="data", keyspace="test") \
    .load()
#.option("directJoinSetting", "on") \

#df = df.coalesce(1)
#print(df.rdd.getNumPartitions())
#print(df.distinct().count())

#print(df.toPandas())
df_clust = df.select([c for c in df.columns if c in ['creator', 'pnns_groups_1', 'pnns_groups_2']])

indexer = StringIndexer(inputCols=['creator', 'pnns_groups_1', 'pnns_groups_2'],
                        outputCols=['creator_ind', 'pnns_groups_1_ind', 'pnns_groups_2_ind'])
some = indexer.fit(df_clust)
df_clust_ind = some.transform(df_clust)
df_clust_ind = df_clust_ind.drop(*('creator', 'pnns_groups_1', 'pnns_groups_2'))
df_clust_ind.limit(5).show()

vec_assembler = VectorAssembler(inputCols=df_clust_ind.columns,
                                outputCol='features')
df_clust_feat = vec_assembler.transform(df_clust_ind)
df_clust_feat.limit(5).show()

scaler = StandardScaler(inputCol="features",
                        outputCol="scaledFeatures",
                        withStd=True,
                        withMean=False)
scalerModel = scaler.fit(df_clust_feat)
df_clust_feat = scalerModel.transform(df_clust_feat)
df_clust_feat.limit(5).show()


evaluator = ClusteringEvaluator(predictionCol='prediction',
                                featuresCol='scaledFeatures',
                                metricName='silhouette',
                                distanceMeasure='squaredEuclidean')
silhouette_score = []
for i in range(2, 10):
    kmeans = KMeans(k=i)
    model = kmeans.fit(df_clust_feat)
    predictions = model.transform(df_clust_feat)
    score = evaluator.evaluate(predictions)
    silhouette_score.append(score)
print('Оценка для значения кластеров', silhouette_score)


kmeans = KMeans(k=8)
model = kmeans.fit(df_clust_feat)
predictions = model.transform(df_clust_feat)
predictions.limit(5).show()

#schema = StructType([StructField("result", IntegerType())])
#temp_df = spark.createDataFrame(predictions.select('prediction'), schema=schema)

temp_df = predictions.select([c for c in predictions.columns if c in ['prediction']])
temp_df.limit(5).show()

df_result = df.select([c for c in df.columns if c in ['id']])

df_result = df_result.withColumn("row_idx", monotonically_increasing_id())
temp_df = temp_df.withColumn("row_idx", monotonically_increasing_id())
df_result = df_result.join(temp_df, df_result.row_idx == temp_df.row_idx).drop("row_idx")

#df_result = df_result.withColumn("result", temp_df("prediction"))
df_result.limit(5).show()

#plt.plot(range(2, 10), silhouette_score)
#plt.xlabel('Количество кластеров')
#plt.ylabel('Оценка')
#plt.title('График изависимости оценки от кол-ва кластеров')
#plt.show()

df_result = df_result.withColumnRenamed("prediction", "clust")

query = df_result.write.format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "test") \
    .option("table", "result") \
    .option("confirm.truncate", "true") \
    .option("header", "true") \
    .mode("overwrite") \
    .save()
spark.stop()
# # mode("append")
# df = pandas.read_json(bson_file, lines = True, chunksize = 1000)
# for ind in df:
#    print(ind)
# bson_data = bson.loads(bson_file.read())
# print(df)     


