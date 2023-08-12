from cassandra.cluster import Cluster
import create_fundament
import write_data
import time
from pyspark.sql import SparkSession
import os


if __name__ == '__main__':
	print("Waiting for base deployment")

	path = r"/spark/jars/spark-cassandra-connector_2.12-3.2.0.jar"
	if os.path.exists(path):
		print('Driver detected')

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



	time.sleep(60)
	start_time = time.time()
	create_fundament.create_env_baza()
	write_data.write_data(spark)
	print("---------------------------- %s seconds ----------------------------" % (time.time() - start_time))
	print("Data handling is complete")
	spark.stop()