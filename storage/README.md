# Aggregator Data System (UAS Sistem Terdistribusi)

Sistem aggregator berbasis Microservices untuk menangani pengiriman data event dengan fitur **Idempotency** dan **Deduplikasi** menggunakan FastAPI, Redis, dan PostgreSQL.

## 📋 Prasyarat
Pastikan di komputer sudah terinstall:
- **Docker Desktop** (harus dalam keadaan aktif/running).
- **Python 3.11+** (untuk menjalankan test script lokal).

---

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