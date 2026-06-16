FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-root user and group
RUN groupadd -g 10002 appgroup && \
    useradd -u 10002 -g appgroup -m -s /bin/bash appuser

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the publisher application code
COPY . /app/publisher/

# Set ownership of the application code to the appuser
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Run publisher script
CMD ["python", "-m", "publisher.publisher"]
