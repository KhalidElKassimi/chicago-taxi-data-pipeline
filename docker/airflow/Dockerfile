FROM apache/airflow:2.10.5-python3.8

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.8
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

USER root
RUN apt-get update \
     && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark-jars \
     && curl -fsSL -o /opt/spark-jars/hadoop-aws-3.3.4.jar \
         https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
     && curl -fsSL -o /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
         https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
     && curl -fsSL -o /opt/spark-jars/wildfly-openssl-1.0.7.Final.jar \
         https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

USER airflow
RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINTS_URL}" \
    apache-airflow-providers-apache-spark==4.11.0 \
    pyspark==3.5.4 \
    minio==7.2.10

ENV PATH="/home/airflow/.local/bin:${PATH}"