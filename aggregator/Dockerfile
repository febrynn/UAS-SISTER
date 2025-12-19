FROM python:3.11-slim

WORKDIR /app

# Instal curl agar healthcheck di docker-compose bisa jalan
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pastikan folder src disalin dengan benar agar struktur api/... terbaca
COPY src/ .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]