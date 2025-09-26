FROM apache/airflow:2.9.2-python3.10

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Switch to root to install git
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Copy dependency file and set proper permissions
COPY pyproject.toml /pyproject.toml

RUN uv pip compile /pyproject.toml -o /requirements.txt && \
    uv pip install --system --no-cache -r /requirements.txt

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV DBT_PROFILES_DIR=/opt/dbt

WORKDIR /opt/airflow
