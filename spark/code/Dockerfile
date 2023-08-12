FROM bde2020/spark-python-template:3.2.0-hadoop3.2

COPY test.py /app/
#RUN pyspark \
#  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
#  --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions
COPY /driver_32/ /spark/jars/
COPY /jar/ /spark/jars/
RUN apk update
RUN apk add make automake gcc g++ subversion python3-dev
RUN python3 -m pip install -U pip
RUN pip3 install numpy
#RUN pip3 install matplotlib

#RUN pip3 install cython
#RUN pip3 install --upgrade setuptools
#RUN pip3 install pandas

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/test.py