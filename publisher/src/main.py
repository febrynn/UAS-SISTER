import requests
import random
import time

class EventPublisher:
    def __init__(self, target_url):
        self.target_url = target_url

    def publish_event(self, event, max_retries=5):
        """Mengirim satu event dengan dibungkus List [event]"""
        for i in range(max_retries):
            try:
                # PERBAIKAN: json=[event] (Aggregator wajib terima List)
                response = requests.post(self.target_url, json=[event]) 
                response.raise_for_status() 
                return response.status_code, response.json()
            except requests.exceptions.HTTPError as e:
                # Cetak pesan error detail dari FastAPI jika 422
                print(f"❌ Detail error dari Aggregator: {e.response.text}")
                return e.response.status_code, {"error": e.response.text}
            except Exception as e:
                if i < max_retries - 1:
                    time.sleep(2)
                else:
                    return 500, {"error": str(e)}

    def generate_event(self, topic, event_id, source, payload):
        return {
            "topic": topic,
            "event_id": str(event_id),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source": source,
            "payload": payload
        }

    def simulate_events(self, topic, source, num_events=5):
        events = []
        for _ in range(num_events):
            event_id = str(random.randint(1, 10000))
            # PERBAIKAN: payload['data'] harus DICT, bukan string
            payload = {
                "data": {
                    "message": f"Sample data for event {event_id}",
                    "value": random.randint(20, 30)
                }
            }
            event = self.generate_event(topic, event_id, source, payload)
            events.append(event)
        return events

if __name__ == "__main__":
    publisher = EventPublisher(target_url="http://uas-aggregator:8080/publish")
    print("🚀 Memulai publisher...")
    time.sleep(5) 

    events = publisher.simulate_events(topic="sensor_suhu", source="device_01")
    for event in events:
        status, resp = publisher.publish_event(event)
        print(f"📡 Event {event['event_id']} | Status {status}")