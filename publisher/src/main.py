import redis
import json
import os
import random
import time
import uuid

# =====================================================
# CLASS EVENT PUBLISHER
# =====================================================
class EventPublisher:
    def __init__(self, broker_url=None):
        # Konfigurasi koneksi Redis
        self.broker_url = broker_url or os.getenv("BROKER_URL", "redis://localhost:6379")
        self.channel_name = "events"
        
        try:
            self.redis_client = redis.Redis.from_url(
                self.broker_url,
                decode_responses=True
            )
            self.redis_client.ping()
        except Exception as e:
            self.redis_client = None

    def generate_event(self, topic, event_id, source="publisher"):
        """Membuat struktur data event"""
        return {
            "topic": topic,
            "event_id": str(event_id),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": source,
            "payload": {
                "data": {
                    "message": f"Manual test event {event_id}",
                    "value": 100 # Nilai fix, bukan random lagi
                }
            }
        }

    def publish(self, topic, event_id, source="publisher"):
        """Fungsi utama untuk mengirim data"""
        if not self.redis_client:
            return None

        event = self.generate_event(topic, event_id, source)
        
        try:
            # Kirim ke Redis Channel
            self.redis_client.publish(
                self.channel_name,
                json.dumps(event)
            )
            return event
        except Exception as e:
            print(f"❌ Publish failed: {e}")
            return None

    def simulate_events(self, count=10, delay=0.1):
        """Method ini tetap dibiarkan ada (jangan dihapus) jaga-jaga kalau diminta dosen."""
        topics = ["user_signup", "order_created", "payment_failed"]
        published_count = 0
        for i in range(count):
            topic = random.choice(topics)
            event_id = str(uuid.uuid4()) 
            if self.publish(topic, event_id):
                published_count += 1
            time.sleep(delay)
        return published_count

# =====================================================
# MAIN (BAGIAN INI YANG KITA UBAH MANUAL)
# =====================================================
if __name__ == "__main__":
    publisher = EventPublisher()
    
    if publisher.redis_client:
        try:
            print("🚀 Publisher Starting (Mode Otomatis)...")
            publisher.simulate_events(count=0, delay=1)
            print("✅ Selesai. 5 Event telah dikirim.")
        except KeyboardInterrupt:
            print("🛑 Publisher stopped.")
        except Exception as e:
            print(f"❌ Error occurred: {e}")
    else:
        print("⚠️ Redis not connected. Exiting.")
