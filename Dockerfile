FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# postgresql-client: for pg_dump backups (optional profile)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        build-essential \
        postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY scripts /app/scripts
COPY infra /app/infra
COPY README.md /app/README.md
COPY Makefile /app/Makefile

# Default: API service
CMD ["python", "-m", "app.services.api"]
