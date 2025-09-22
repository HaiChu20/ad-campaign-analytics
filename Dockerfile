FROM apache/airflow:2.9.2-python3.10

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV DBT_PROFILES_DIR=/opt/dbt

WORKDIR /opt/airflow
