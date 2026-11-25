FROM apache/spark:3.5.1-python3
USER root
RUN pip install --no-cache-dir clickhouse-connect==0.8.11
# Kafka bağlayıcısı imaja gömülür; çalışırken internetten indirilmez (tekrarlanabilirlik).
ARG M=https://repo1.maven.org/maven2
ADD --chmod=644 $M/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD --chmod=644 $M/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
COPY metrics_job.py /app/
ENV PYTHONUNBUFFERED=1
USER spark
