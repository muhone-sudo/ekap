# syntax=docker/dockerfile:1
FROM python:3.11-slim AS base

# Install tini for PID 1 management
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user hulusi (UID 1000)
RUN useradd --uid 1000 --create-home --shell /bin/bash hulusi

WORKDIR /app

# Install Python dependencies
COPY services/radar/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && playwright install chromium --with-deps

# Copy shared packages (build context must be repository root)
COPY shared /app/shared

# Copy service source
COPY services/radar/src/ /app/src/
COPY services/radar/selectors.json /app/selectors.json

# Ensure outputs directory exists and is owned by hulusi
RUN mkdir -p /app/outputs && chown -R hulusi:hulusi /app

USER hulusi

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "src.main"]
