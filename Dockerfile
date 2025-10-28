# SOAR Data Pipeline - Production Docker Image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r soar && useradd -r -g soar soar

# Create application directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml .
COPY ops/requirements.txt ./ops/

# Install Python dependencies
RUN pip install --no-cache-dir -r ops/requirements.txt

# Copy source code
COPY src/ ./src/
COPY ops/ ./ops/
COPY pipelines/ ./pipelines/

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/metadata && \
    chown -R soar:soar /app

# Switch to non-root user
USER soar

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.path.insert(0, 'src'); import config; print('OK')" || exit 1

# Default command
CMD ["python", "-m", "pipelines.aqs.run_aqs_service"]

# Labels
LABEL org.opencontainers.image.title="SOAR Data Pipeline" \
      org.opencontainers.image.description="EPA AQS Air Quality Data Pipeline" \
      org.opencontainers.image.vendor="Oregon DEQ" \
      org.opencontainers.image.source="https://github.com/OR-Dept-Environmental-Quality/soar_pipeline" \
      org.opencontainers.image.licenses="MIT"