FROM python:3.10-slim

# Set non-interactive environment
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Update and install git and necessary dependencies
RUN apt-get update && \
    apt-get install -y git

# Verify Python and pip installation
RUN python3.10 -m venv venv

ENV PATH=/app/venv/bin:$PATH

# Set the working directory
WORKDIR /root/.bittensor/subnets/snpOracle

COPY . /root/.bittensor/subnets/snpOracle

# Install dependencies
RUN python -m pip install -e .

RUN git config --global --add safe.directory /root/.bittensor/subnets/snpOracle