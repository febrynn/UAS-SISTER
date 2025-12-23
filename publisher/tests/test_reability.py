import sys
import os
import pytest
import time
import uuid
import psycopg2
import json
import redis
import threading
import concurrent.futures
from datetime import datetime
from fastapi.testclient import TestClient

# ===========================================================================
# 1. SETUP PATH & ENV
# ===========================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

# Override Env ke Localhost
os.environ["BROKER_URL"] = "redis://localhost:6379"
os.environ["DB_HOST"] = "localhost"
os.environ["DB_NAME"] = "postgres"
os.environ["DB_USER"] = "postgres"
os.environ["DB_PASS"] = "Febriani_20"

import src.main 
from src.main import app

# ===========================================================================
# 2. WORKER SIMULATOR (Sama seperti sebelumnya)
# ===========================================================================
def run_worker_simulator(stop_event):
    try:
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe("events")
        conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="Febriani_20")
        conn.autocommit = True
        cursor = conn.cursor()

        while not stop_event.is_set():
            message = pubsub.get_message(timeout=0.5)
            if message and message['type'] == 'message':
                data = json.loads(message['data'])
                # INSERT dengan ON CONFLICT DO NOTHING (Penting untuk Uji Duplikasi)
                query = """
                    INSERT INTO processed_events (event_id, topic, source, payload, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                """
                cursor.execute(query, (
                    data['event_id'], data['topic'], data['source'],
                    json.dumps(data['payload']), data['timestamp']
                ))
            time.sleep(0.01) # Sleep sangat kecil untuk performa tinggi
    except Exception as e:
        print(f"Worker Error: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
        if 'r' in locals(): r.close()

# ===========================================================================
# 3. FIXTURES
# ===========================================================================
@pytest.fixture(scope="module")
def setup_system():
    # Setup DB
    conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="Febriani_20")
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(255) UNIQUE NOT NULL,
            topic VARCHAR(255) NOT NULL,
            source VARCHAR(255),
            payload JSONB,
            timestamp TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Setup Redis Client untuk App
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    src.main.redis_client = r # Monkeypatch
    
    # Jalankan Worker Simulator
    stop_event = threading.Event()
    worker_thread = threading.Thread(target=run_worker_simulator, args=(stop_event,))
    worker_thread.daemon = True
    worker_thread.start()
    
    yield cursor # Return cursor DB untuk verifikasi
    
    # Cleanup
    stop_event.set()
    worker_thread.join(timeout=1)
    cursor.close()
    conn.close()
    r.close()

# ===========================================================================
# 4. SKENARIO UJI RELIABILITAS
# ===========================================================================

def test_5_2_1_uji_duplikasi(setup_system):
    """
    Skenario 5.2.1: Mengirim payload identik berulang kali.
    Harapan: Sistem Idempotent (Hanya 1 data yang masuk DB, sisanya diabaikan).
    """
    db_cursor = setup_system
    client = TestClient(app)
    
    unique_id = str(uuid.uuid4())
    event_id = f"dup-test-{unique_id}"
    payload = {
        "topic": "reliability_test",
        "event_id": event_id, # ID SAMA
        "timestamp": datetime.now().isoformat(),
        "source": "pytest-duplication",
        "payload": {"status": "duplicate_check"}
    }

    print(f"\n[5.2.1] Mengirim 5 payload IDENTIK dengan ID: {event_id}")
    
    # Kirim 5 kali
    for i in range(5):
        client.post("/publish", json=payload)
        print(f"   -> Kirim ke-{i+1} Sukses")

    print("   -> Menunggu worker memproses...")
    time.sleep(2)

    # Verifikasi: Cek jumlah data di DB dengan ID tersebut
    db_cursor.execute("SELECT COUNT(*) FROM processed_events WHERE event_id = %s", (event_id,))
    count = db_cursor.fetchone()[0]

    print(f"   -> Jumlah data di DB: {count}")
    
    # ASSERT: Harus tetap 1, tidak boleh 5
    assert count == 1, f"GAGAL! Ada {count} data duplikat masuk. Seharusnya cuma 1."
    print("✅ SUKSES 5.2.1: Sistem berhasil menangani duplikasi (Idempotency).")


def test_5_2_2_uji_konkurensi(setup_system):
    """
    Skenario 5.2.2: Simulasi request paralel secara masif.
    Harapan: Semua request unik berhasil diproses tanpa error/lost data.
    """
    db_cursor = setup_system
    client = TestClient(app)
    
    TOTAL_REQUESTS = 50  # Jumlah request paralel
    print(f"\n[5.2.2] Mengirim {TOTAL_REQUESTS} request secara PARALEL (Concurrent)...")

    # Fungsi pengirim request tunggal
    def send_request(index):
        evt_id = f"conc-test-{uuid.uuid4()}"
        payload = {
            "topic": "concurrency_test",
            "event_id": evt_id,
            "timestamp": datetime.now().isoformat(),
            "source": "pytest-concurrency",
            "payload": {"idx": index}
        }
        client.post("/publish", json=payload)
        return evt_id

    # Eksekusi Paralel menggunakan ThreadPool
    start_time = time.time()
    sent_ids = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit 50 request sekaligus
        futures = [executor.submit(send_request, i) for i in range(TOTAL_REQUESTS)]
        for future in concurrent.futures.as_completed(futures):
            sent_ids.append(future.result())

    duration = time.time() - start_time
    print(f"   -> Selesai mengirim dalam {duration:.2f} detik.")
    
    print("   -> Menunggu worker menyelesaikan antrean (3 detik)...")
    time.sleep(3) # Beri waktu worker mengejar ketertinggalan

    # Verifikasi: Hitung berapa yang masuk DB
    # Kita pakai LIKE 'conc-test-%' untuk menghitung semua data dari tes ini
    db_cursor.execute("SELECT COUNT(*) FROM processed_events WHERE source = 'pytest-concurrency'")
    count_db = db_cursor.fetchone()[0]
    
    print(f"   -> Data terkirim: {TOTAL_REQUESTS}")
    print(f"   -> Data masuk DB: {count_db}")

    # Toleransi selisih (kadang test runner selesai sebelum worker kelar semua)
    # Tapi idealnya harus sama persis.
    assert count_db == TOTAL_REQUESTS, f"GAGAL! Data hilang. Kirim {TOTAL_REQUESTS}, Masuk {count_db}"
    print("✅ SUKSES 5.2.2: Sistem stabil menangani konkurensi.")