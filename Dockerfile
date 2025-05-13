FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install git for potential editable dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy files needed for installing dependencies
COPY pyproject.toml README.md ./

# Install dependencies
# Use pip with --no-cache-dir to keep image size smaller
RUN pip install --no-cache-dir -e .

# Copy source code
COPY src/ ./src/

# Create a non-root user to run the application
RUN useradd -m -u 1000 mcpuser
USER mcpuser

# Set entry point
ENTRYPOINT ["python", "-m", "mcp_mqtt_proxy"]

# Default command
# Users can override these arguments when running the container
CMD ["--help"]
