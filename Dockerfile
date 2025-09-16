# Production Dockerfile for Media Tracker Bot
FROM python:3.11-slim

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PROJECT_ROOT=/app \
    CONFIG_DIR=/app/config \
    DATA_DIR=/app/data \
    CACHE_DIR=/app/cache \
    LOG_DIR=/app/logs \
    STATIC_DIR=/app/static \
    DATABASE_PATH=/app/data/tasks.db

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create app user
RUN useradd --create-home --shell /bin/bash app

# Set work directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Create directory structure
RUN mkdir -p /app/{config,data,cache,logs,static,src} \
    /app/data/reports \
    /app/cache/crawl_results && \
    chown -R app:app /app

# Switch to app user
USER app

# Copy application files
COPY --chown=app:app src/ /app/src/
COPY --chown=app:app config/ /app/config/
COPY --chown=app:app static/ /app/static/

# Create default files
RUN touch /app/data/tasks.db && \
    echo '{}' > /app/data/task_queue.json

# Create default .env
RUN echo "OPENAI_API_KEY=your_key_here" > /app/.env && \
    echo "GROQ_API_KEY=your_key_here" >> /app/.env && \
    echo "DEFAULT_MODEL_PROVIDER=openai" >> /app/.env

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start application
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]