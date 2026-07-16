FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

USER airflow
RUN pip install pyspark==3.5.1

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin