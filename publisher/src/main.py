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
        # FIX: Default ke localhost agar jalan di Windows/Test.
        # Saat di Docker, env BROKER_URL akan menimpa ini menjadi 'redis://broker:6379'
        self.broker_url = broker_url or os.getenv("BROKER_URL", "redis://localhost:6379")
        self.channel_name = "events"
        
        # Inisialisasi koneksi Redis
        try:
            self.redis_client = redis.Redis.from_url(
                self.broker_url,
                decode_responses=True
            )
            self.redis_client.ping() # Cek koneksi
        except Exception as e:
            # Jangan print error berisik jika hanya testing unit tanpa redis
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
                    "message": f"Sample event {event_id}",
                    "value": random.randint(1, 100)
                }
            }
        }

    def publish(self, topic, event_id, source="publisher"):
        """Publish single event ke Redis"""
        if not self.redis_client:
            return None

        event = self.generate_event(topic, event_id, source)
        
        try:
            self.redis_client.publish(
                self.channel_name,
                json.dumps(event)
            )
            # print(f"📤 Published: {topic} - {event_id}") # Optional debug
            return event
        except Exception as e:
            print(f"❌ Publish failed: {e}")
            return None

    def simulate_events(self, count=10, delay=0.1):
        """
        METHOD INI WAJIB ADA UNTUK LOLOS TESTING.
        Method ini mensimulasikan pengiriman banyak event sekaligus.
        """
        topics = ["user_signup", "order_created", "payment_failed"]
        published_count = 0
        
        for i in range(count):
            # Generate random data
            topic = random.choice(topics)
            # Gunakan UUID agar unik, atau random int untuk tes duplikat
            event_id = str(uuid.uuid4()) 
            
            result = self.publish(topic, event_id)
            if result:
                published_count += 1
            
            time.sleep(delay)
        
        return published_count

# =====================================================
# MAIN (Entrypoint untuk Docker)
# =====================================================

if __name__ == "__main__":
    print("🚀 Publisher started...")
    
    # Tunggu sebentar agar container lain siap (hanya efek di Docker)
    time.sleep(3) 
    
    publisher = EventPublisher()
    
    # Jalankan simulasi terus menerus jika dijalankan sebagai script utama
    if publisher.redis_client:
        try:
            while True:
                publisher.simulate_events(count=1, delay=2.0)
                print("ping...")
        except KeyboardInterrupt:
            print("🛑 Publisher stopped.")
    else:
        print("⚠️ Redis not connected. Exiting.")