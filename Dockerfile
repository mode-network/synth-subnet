FROM ubuntu/python:3.10-22.04

RUN apt-get update && apt-get install -y software-properties-common

RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -- -y

# Set work directory
WORKDIR /app

# Copy the application code
COPY . /app

# Install Python dependencies
#COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Run the application
#CMD ["python", "src/main.py"]
