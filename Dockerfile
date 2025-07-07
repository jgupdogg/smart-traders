FROM apache/airflow:2.10.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements first for better Docker layer caching
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy source code
COPY --chown=airflow:root ./src /opt/airflow/src
COPY --chown=airflow:root ./airflow/dags /opt/airflow/dags
COPY --chown=airflow:root ./airflow/config /opt/airflow/config

# Set Python path to include our source code
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"

# Set working directory
WORKDIR /opt/airflow