FROM python:3.11-slim

RUN apt-get update && apt-get install -y openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Install compatible versions
RUN pip install --no-cache-dir pyspark==3.5.4 delta-spark==3.2.1 pandas pyarrow pyyaml

WORKDIR /app

COPY pipeline/ ./pipeline/
COPY config/ ./config/

ENV PYTHONPATH=/app

ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.2.1 pyspark-shell"

CMD ["python", "-m", "pipeline.run_all"]