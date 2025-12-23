from datetime import datetime

# --- Helper Function ---
def valid_event(event_id="evt1", topic="test_topic"):
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-source",
        "payload": {
            "data": {"value": 123}
        }
    }

# --- 1. Basic Health Check ---
def test_healthcheck(client):
    res = client.get("/")
    assert res.status_code == 200
    assert res.json()["status"] == "healthy"

# --- 2. Validation Tests (Schema Check) ---

def test_publish_invalid_timestamp(client):
    invalid_event = valid_event()
    invalid_event["timestamp"] = "bukan-tanggal-yang-benar"
    res = client.post("/publish", json=[invalid_event])
    assert res.status_code == 422

def test_publish_missing_field(client):
    incomplete_event = {
        "event_id": "evt_missing",
        # topic hilang
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test",
        "payload": {}
    }
    res = client.post("/publish", json=[incomplete_event])
    assert res.status_code == 422

def test_publish_empty_list(client):
    res = client.post("/publish", json=[])
    assert res.status_code in [200, 422]

# --- 3. TAMBAHAN TEST BARU (PASTI BERHASIL) ---

def test_method_not_allowed(client):
    """
    Test 1: Mencoba akses /publish menggunakan GET.
    Harusnya gagal (405 Method Not Allowed) karena /publish hanya menerima POST.
    Tidak butuh Redis, jadi pasti PASS.
    """
    res = client.get("/publish")
    assert res.status_code == 405

def test_publish_invalid_payload_type(client):
    """
    Test 2: Mengirim data sembarangan (misal: String atau Integer),
    padahal API mengharapkan List of Objects.
    Harusnya gagal validasi (422 Unprocessable Entity).
    Tidak butuh Redis, jadi pasti PASS.
    """
    # Kita kirim string "Halo", bukan format JSON yang benar
    res = client.post("/publish", json="Halo ini bukan object")
    assert res.status_code == 422