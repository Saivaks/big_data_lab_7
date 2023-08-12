FROM bde2020/spark-python-template:3.2.0-hadoop3.2

WORKDIR /app

COPY requirements.txt requirements.txt
COPY /driver_32/ /spark/jars/
COPY /jar/ /spark/jars/

RUN pip3 install cassandra-driver
RUN pip3 install bson
COPY . .

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/main.py