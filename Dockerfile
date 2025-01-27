FROM ubuntu:20.04

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y software-properties-common && \
    apt-get install -y curl && \
    apt-get install -y bash

RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    export PATH="$HOME/.cargo/bin:$PATH"

# Set environment variable to make Cargo available in PATH
ENV PATH="/root/.cargo/bin:${PATH}"

RUN apt-get install -y nodejs npm python3.10 python3.10-distutils pkg-config make python3.10-venv

# Set work directory
WORKDIR /app

# Copy the application code
COPY . /app

RUN python3.10 -m ensurepip --upgrade
RUN python3.10 -m pip install --upgrade pip

RUN python3.10 -m pip install -r requirements.txt

ENV PYTHONPATH="."

# Run the application
RUN chmod +x entrypoint-validator.sh
ENTRYPOINT ["./entrypoint-validator.sh"]
