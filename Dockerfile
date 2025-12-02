# Use the official slim Python 3.11 image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install build and runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
ENV POETRY_HOME=/opt/poetry
ENV POETRY_VERSION=2.1.1
ENV PATH="/opt/poetry/bin:$PATH"
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy poetry files and install dependencies
COPY pyproject.toml ./

# Configure Poetry and install dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --only main --no-interaction

# Copy application files into the container
COPY app .

# Create the Streamlit configuration directory
RUN mkdir -p /root/.streamlit

# Copy the Streamlit configuration file
COPY .streamlit/config.toml /root/.streamlit/config.toml

# Expose the Streamlit port
EXPOSE 8501

# Create data directory for Qdrant
RUN mkdir -p /data/qdrant

# Command to start the application
CMD ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]
