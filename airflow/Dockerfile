FROM apache/airflow:2.10.4-python3.11

# Docker CLI — Spark jobs run via `docker run` against the mounted host socket.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
