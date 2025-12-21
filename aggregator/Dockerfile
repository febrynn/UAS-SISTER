FROM python:3.11-slim

WORKDIR /app

# Instal curl agar healthcheck jalan
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Karena sekarang main.py ada di dalam folder src, kita panggil path lengkapnya
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]