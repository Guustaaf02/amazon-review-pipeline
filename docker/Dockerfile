# Dockerfile básico com PySpark
FROM openjdk:11

# Instalações básicas
RUN apt-get update && apt-get install -y curl python3 python3-pip

# Instala Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz | tar -xz -C /opt/ && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark


ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Instala Python dependências
COPY ../requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Define diretório de trabalho
WORKDIR /app
