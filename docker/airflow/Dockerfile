FROM apache/airflow:2.3.0

USER root

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater original version 3.0.2
ENV SPARK_VERSION=3.1.2 \
HADOOP_VERSION=3.2 \
PYTHONHASHSEED=1

# Download and uncompress spark from the apache archive

RUN apt update && \
    apt-get install -y wget

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

# Create SPARK_HOME env var
ENV SPARK_HOME=/opt/spark 
RUN export SPARK_HOME
ENV PATH $PATH:/opt/spark/bin


USER airflow

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install yfinance && pip install pendulum