FROM python:3.11-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
        curl \
        bash \
        nodejs \
        npm \
        pkg-config \
        make \
        gcc \
        libpq-dev \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

RUN pip install --upgrade pip && pip install bittensor-cli

WORKDIR /app

RUN git clone https://github.com/mode-network/synth-subnet.git . && \
    pip install -r requirements.txt

ENV PYTHONPATH="."

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8091

ENTRYPOINT ["./entrypoint.sh"]
