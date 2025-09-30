FROM apache/airflow:2.9.2-python3.10

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

COPY . /.

RUN uv pip compile /pyproject.toml -o /requirements.txt && \
    uv pip install --no-cache -r /requirements.txt

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV DBT_PROFILES_DIR=/opt/dbt

WORKDIR /opt/airflow
