import sys
import os
import threading
import time
import uuid
import json
import random
import concurrent.futures
from datetime import datetime

# ===========================================================================
# 1. SETUP PATH & ENV
# ===========================================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Konfigurasi agar connect ke Localhost (karena run dari Windows/Host)
os.environ["BROKER_URL"] = "redis://localhost:6379"
os.environ["DB_HOST"] = "localhost"
os.environ["DB_NAME"] = "postgres"
os.environ["DB_USER"] = "postgres"
os.environ["DB_PASS"] = "Febriani_20"

import pytest
import psycopg2
import redis

# --- IMPORT CLASS UTAMA ---
try:
    import src.main
    from src.main import EventPublisher # Import Class, bukan 'app'
except ImportError as e:
    raise ImportError(f"Gagal import EventPublisher dari src.main: {e}")

# ===========================================================================
# 2. WORKER SIMULATION (Background Consumer)
# ===========================================================================
def run_worker_optimized(stop_event):
    """
    Worker simulasi: Subscribe Redis -> Save ke Postgres.
    """
    conn = None
    r = None
    cursor = None
    try:
        # Connect ke Redis & DB
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe("events")
        
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"], 
            database=os.environ["DB_NAME"], 
            user=os.environ["DB_USER"], 
            password=os.environ["DB_PASS"]
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Buat tabel jika belum ada
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id VARCHAR(50) PRIMARY KEY,
                topic VARCHAR(50),
                source VARCHAR(50),
                payload JSONB,
                timestamp TIMESTAMP
            );
        """)

        while not stop_event.is_set():
            message = pubsub.get_message(timeout=0.1)
            if message and message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    # Query Insert (Ignore Duplicate)
                    query = """
                        INSERT INTO processed_events (event_id, topic, source, payload, timestamp)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (event_id) DO NOTHING;
                    """
                    cursor.execute(query, (
                        data['event_id'], data['topic'], data['source'],
                        json.dumps(data['payload']), data['timestamp']
                    ))
                except Exception:
                    pass
    except Exception as e:
        print(f"[Worker Error] {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        if r: r.close()

# ===========================================================================
# 3. FIXTURE (SETUP & TEARDOWN)
# ===========================================================================
@pytest.fixture(scope="module")
def setup_performance():
    # 1. Reset Database
    try:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"], 
            database=os.environ["DB_NAME"], 
            user=os.environ["DB_USER"], 
            password=os.environ["DB_PASS"]
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id VARCHAR(50) PRIMARY KEY,
                topic VARCHAR(50),
                source VARCHAR(50),
                payload JSONB,
                timestamp TIMESTAMP
            );
        """)
        cursor.execute("TRUNCATE TABLE processed_events RESTART IDENTITY;")
    except Exception as e:
        pytest.fail(f"Gagal konek DB: {e}")

    # 2. Reset Redis
    try:
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        r.flushall()
    except Exception as e:
        pytest.fail(f"Gagal konek Redis: {e}")
    
    # 3. Jalankan Worker Thread
    stop_event = threading.Event()
    worker_thread = threading.Thread(target=run_worker_optimized, args=(stop_event,))
    worker_thread.daemon = True
    worker_thread.start()
    
    yield cursor 
    
    # 4. Cleanup
    stop_event.set()
    worker_thread.join(timeout=2)
    cursor.close()
    conn.close()

# ===========================================================================
# 4. LOAD TEST SCENARIO
# ===========================================================================
def test_publisher_class_performance(setup_performance):
    """
    Test langsung ke Class EventPublisher (Bukan via API HTTP).
    Mengirim 1000 event dengan multithreading.
    """
    db_cursor = setup_performance
    
    # Instansiasi Publisher Class
    publisher = EventPublisher()
    
    # --- KONFIGURASI LOAD TEST ---
    TOTAL_REQUESTS = 1000       
    DUPLICATE_RATIO = 0.2        
    CONCURRENT_THREADS = 20      
    
    print(f"\n🚀 START LOAD TEST (Class Method): {TOTAL_REQUESTS} Events")
    
    # 1. Generate Data (Unik & Duplikat)
    unique_count = int(TOTAL_REQUESTS * (1 - DUPLICATE_RATIO))
    duplicate_count = int(TOTAL_REQUESTS * DUPLICATE_RATIO)
    
    unique_ids = [str(uuid.uuid4()) for _ in range(unique_count)]
    tasks = []
    
    # Siapkan Data
    for uid in unique_ids:
        tasks.append((uid, "test_topic"))
        
    if unique_ids:
        for _ in range(duplicate_count):
            tasks.append((random.choice(unique_ids), "test_topic"))
            
    random.shuffle(tasks)

    # 2. Fungsi Eksekutor (Langsung panggil method class)
    def publish_task(task_data):
        eid, topic = task_data
        # Panggil method .publish() dari class EventPublisher
        publisher.publish(topic=topic, event_id=eid, source="load-test")

    # 3. Eksekusi
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
        list(executor.map(publish_task, tasks))
    
    finish_publish_time = time.time()
    publish_duration = finish_publish_time - start_time
    print(f"✅ Publisher selesai mengirim dalam {publish_duration:.2f} detik")

    # 4. Polling DB (Menunggu Worker selesai insert)
    print("⏳ Menunggu Worker sync ke DB...")
    max_retries = 40
    for _ in range(max_retries):
        db_cursor.execute("SELECT COUNT(*) FROM processed_events")
        curr = db_cursor.fetchone()[0]
        if curr >= len(unique_ids): 
            break
        time.sleep(0.5)

    end_time = time.time()
    total_duration = end_time - start_time
    
    # 5. Analisis Hasil
    db_cursor.execute("SELECT COUNT(*) FROM processed_events")
    final_count = db_cursor.fetchone()[0]
    expected = len(unique_ids)
    
    throughput = TOTAL_REQUESTS / total_duration if total_duration > 0 else 0
    
    print("\n" + "="*40)
    print(f"📊 HASIL PERFORMANCE TEST")
    print(f" - Total Event Dikirim : {TOTAL_REQUESTS}")
    print(f" - Data Unik (Target)  : {expected}")
    print(f" - Data Masuk DB       : {final_count}")
    print(f" - Total Waktu         : {total_duration:.2f} s")
    print(f" - Throughput System   : {throughput:.2f} events/sec")
    print("="*40 + "\n")

    # Assertions
    assert final_count == expected, f"Data Loss! {final_count} vs {expected}"
    assert throughput > 10, "Throughput terlalu rendah (< 10 TPS)"