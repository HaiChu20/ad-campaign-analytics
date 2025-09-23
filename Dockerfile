FROM apache/airflow:2.9.2-python3.10

# Switch to root to install git
USER root
RUN apt-get update && apt-get install -y git && apt-get clean

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV DBT_PROFILES_DIR=/opt/dbt

WORKDIR /opt/airflow
