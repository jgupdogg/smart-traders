# Use same base as Airflow for consistency
FROM apache/airflow:2.8.1-python3.11

# Switch to root to install Java and system dependencies
USER root

# Install Java 17 (required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /app

# Copy application code
COPY main.py /app/main.py

# Run the persistent webhook processor
CMD ["python", "main.py"]