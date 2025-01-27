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

RUN apt-get install -y nodejs npm python3.10 python3.10-distutils pkg-config make

# Set work directory
WORKDIR /app

# Copy the application code
COPY . /app

RUN apt-get install -y python3.10-venv && \
    python3.10 -m venv bt_venv

RUN bash -c "source bt_venv/bin/activate" && \
    bt_venv/bin/pip install -r requirements.txt

ENV PYTHONPATH="."

# Run the application
RUN chmod +x entrypoint-validator.sh
ENTRYPOINT ["./entrypoint-validator.sh"]
