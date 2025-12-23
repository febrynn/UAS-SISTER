# UTS Event System

## Overview
The UTS Event System is a multi-service architecture designed to handle event publishing, processing, and storage. It consists of several components that work together to ensure reliable event handling, deduplication, and data persistence.

## Architecture
The system is composed of the following services:

- **Aggregator**: An API service that publishes and accesses events. It processes events internally through a consumer.
- **Publisher**: A service that generates and simulates events, including the ability to send duplicate events to the aggregator.
- **Broker**: An optional message broker (Redis) that facilitates communication between services.
- **Storage**: A persistent database (PostgreSQL) that stores processed events and related data.

## Features
- **Event Model**: Events are represented in JSON format with the following structure:
  ```json
  {
    "topic": "string",
    "event_id": "string-unique",
    "timestamp": "ISO8601",
    "source": "string",
    "payload": { ... }
  }
  ```
- **API Endpoints**:
  - `POST /publish`: Accepts single or batch events and validates the schema.
  - `GET /events?topic=...`: Retrieves a list of unique processed events.
  - `GET /stats`: Provides statistics on received events, unique processed events, duplicates dropped, topics, and uptime.

## Installation
1. Clone the repository:
   ```
   git clone <repository-url>
   cd uts-event-system
   ```

2. Build and run the services using Docker Compose:
   ```
   docker-compose up --build
   ```

## Testing
Unit and integration tests are provided to ensure the functionality of the system. Tests can be run using:
```
pytest
```

## Contributing
Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
## 🚀 Cara Menjalankan Aplikasi

### 1. Setup Lingkungan Lokal (Opsional)
Agar tidak ada error highlight di VS Code dan bisa menjalankan unit test, install dependencies dulu:

```bash
pip install -r aggregator/requirements.txt
# Jika ada folder publisher
pip install -r publisher/requirements.txt

##jalankan docker
docker compose up --build -d
docker compose up
##membuka swagger
http://localhost:8080/docs

### 1. Setup Lingkungan Lokal (Opsional)
LINK YT : https://youtu.be/-omMRRwRXQA 
LINK Drive : https://drive.google.com/drive/folders/1z6RNPEfj8igsFi8Lc1JE12nwng4_jnRi?usp=sharing