FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir redis requests python-dotenv

# COPY folder src ke dalam folder src di container
COPY src/ ./src/

# Sekarang path ini valid
CMD ["python", "src/main.py"]