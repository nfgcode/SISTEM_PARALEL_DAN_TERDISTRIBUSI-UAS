FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-root user and group
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -m -s /bin/bash appuser

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the aggregator package code
COPY . /app/aggregator/

# Set ownership of the application code to the appuser
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Expose port (only used by the API service)
EXPOSE 8080

# Default command
CMD ["uvicorn", "aggregator.app:app", "--host", "0.0.0.0", "--port", "8080"]
