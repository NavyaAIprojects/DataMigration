FROM python:3.11-slim

WORKDIR /app

# Install system deps for pymssql
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev gcc g++ && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Create directories for uploads and reports
RUN mkdir -p uploads reports

ENV PORT=8000
EXPOSE 8000

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
