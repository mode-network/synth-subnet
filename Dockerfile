FROM ubuntu:22.04

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y software-properties-common && \
    apt-get install -y curl && \
    apt-get install -y bash && \
    apt-get update && \
    apt-get install -y python3.11 python3.11-distutils python3.11-venv python3-pip pkg-config make

RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update -y && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    export PATH="$HOME/.cargo/bin:$PATH"

# Set environment variable to make Cargo available in PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Set work directory
WORKDIR /app

# Copy the application code
COPY . /app

RUN python3.11 -m pip install -r requirements.txt

ENV PYTHONPATH="."

# Run the application
RUN chmod +x entrypoint-validator.sh
ENTRYPOINT ["./entrypoint-validator.sh"]
