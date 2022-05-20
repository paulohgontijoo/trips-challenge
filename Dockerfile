# Get image from Docker locally
FROM spark-base/spark-py:1.0.0

# Set user root
USER root

# Set working directory
WORKDIR /app

# Copy requirements from host to container
COPY  requirements.txt .

# Install requirements
RUN pip3 install -r requirements.txt

# Copy app to container
COPY main.py .