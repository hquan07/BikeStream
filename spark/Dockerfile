# File: spark/Dockerfile
FROM bitnamilegacy/spark:3.5

USER root
RUN pip install pyspark==3.5.3
USER 1001

COPY stream_processor.py /app/stream_processor.py
COPY metrics_aggregator.py /app/metrics_aggregator.py

CMD ["spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.3", \
     "/app/stream_processor.py"]
