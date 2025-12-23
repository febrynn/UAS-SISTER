import sys
import os
import pytest
import time
import uuid
import psycopg2
import json
import redis
import threading
from datetime import datetime
from fastapi.testclient import TestClient

# ===========================================================================
# 1. SETUP PATH & ENV
# ===========================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

# Override env agar semua komponen (API & Worker) pakai Localhost
os.environ["BROKER_URL"] = "redis://localhost:6379"
os.environ["DB_HOST"] = "localhost"
os.environ["DB_NAME"] = "postgres"
os.environ["DB_USER"] = "postgres"
os.environ["DB_PASS"] = "Febriani_20"

import src.main 
from src.main import app

# ===========================================================================
# 2. HELPER: WORKER SIMULATOR
# ===========================================================================
def run_worker_simulator(stop_event):
    """
    Fungsi ini berjalan di background thread.
    Tugasnya MENIRU worker asli: Baca Redis -> Masukkan ke DB.
    """
    # Koneksi DB & Redis khusus untuk thread worker ini
    try:
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe("events")  # Subscribe ke channel 'events'

        conn = psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password="Febriani_20"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        print("👷 [Worker Simulator] Berjalan... Menunggu pesan dari Redis...")

        while not stop_event.is_set():
            message = pubsub.get_message(timeout=1)
            if message and message['type'] == 'message':
                data = json.loads(message['data'])
                print(f"👷 [Worker Simulator] Menerima event: {data.get('event_id')}")

                # LOGIKA INSERT KE DB (Meniru worker asli)
                query = """
                    INSERT INTO processed_events (event_id, topic, source, payload, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING;
                """
                cursor.execute(query, (
                    data['event_id'],
                    data['topic'],
                    data['source'],
                    json.dumps(data['payload']),
                    data['timestamp']
                ))
                print(f"👷 [Worker Simulator] Sukses insert DB: {data.get('event_id')}")
            
            time.sleep(0.1) # Biar CPU tidak 100%

    except Exception as e:
        print(f"👷 [Worker Simulator] Error: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
        if 'r' in locals(): r.close()

# ===========================================================================
# 3. FIXTURES
# ===========================================================================

@pytest.fixture(scope="module")
def db_cursor():
    """Fixture DB - Konek ke Localhost DAN Buat Tabel"""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password="Febriani_20"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Buat Tabel jika belum ada
        create_table_query = """
        CREATE TABLE IF NOT EXISTS processed_events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(255) UNIQUE NOT NULL,
            topic VARCHAR(255) NOT NULL,
            source VARCHAR(255),
            payload JSONB,
            timestamp TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        yield cursor
    except Exception as e:
        pytest.fail(f"❌ Gagal koneksi/init DB: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@pytest.fixture(scope="module")
def redis_client():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    yield r
    r.close()

# ===========================================================================
# 4. TEST SCENARIO
# ===========================================================================

def test_end_to_end_flow(db_cursor, redis_client):
    """
    Test Alur: API -> Redis -> [Worker Simulator] -> DB
    """
    
    # 🔥 1. START WORKER SIMULATOR DI BACKGROUND
    stop_event = threading.Event()
    worker_thread = threading.Thread(target=run_worker_simulator, args=(stop_event,))
    worker_thread.daemon = True
    worker_thread.start()
    
    # Tunggu sebentar agar worker siap subscribe
    time.sleep(1)

    # 🔥 2. PATCH API
    src.main.redis_client = redis_client
    
    # 3. SETUP DATA
    unique_id = str(uuid.uuid4())
    test_event_id = f"test-{unique_id}"
    test_topic = "integration_test_topic"
    
    # 4. ACTION: Kirim data via API
    client = TestClient(app) 
    payload = {
        "topic": test_topic,
        "event_id": test_event_id,
        "timestamp": datetime.now().isoformat(),
        "source": "pytest-integrated",
        "payload": {"status": "simulator_active"}
    }
    
    print(f"\n[1] Mengirim payload ke API: {test_event_id}")
    response = client.post("/publish", json=payload)
    assert response.status_code == 200
    
    # 5. WAITING
    print("[2] Menunggu worker simulator memproses (3 detik)...")
    time.sleep(3) 
    
    # 6. VERIFICATION
    print("[3] Cek Database...")
    db_cursor.execute(
        "SELECT topic, event_id FROM processed_events WHERE event_id = %s", 
        (test_event_id,)
    )
    result = db_cursor.fetchone()
    
    # 7. CLEANUP WORKER
    stop_event.set()
    worker_thread.join(timeout=1)

    if result is None:
        # Debugging
        db_cursor.execute("SELECT * FROM processed_events LIMIT 5")
        print(f"⚠️ Isi tabel: {db_cursor.fetchall()}")
        pytest.fail(f"❌ Data GAGAL masuk DB. Event ID: {test_event_id}")
    
    assert result[0] == test_topic
    assert result[1] == test_event_id
    
    print(f"✅ SUKSES! Data {test_event_id} ditemukan di tabel 'processed_events'.")