FROM python:3.12

RUN apt-get update && apt-get install -y \
    jq \
    ripgrep \
    procps \
    tmux \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://chatgpt.com/codex/install.sh | sh

# Codex config
RUN mkdir -p /root/.codex
COPY config.toml /root/.codex/config.toml

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
