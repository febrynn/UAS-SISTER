def send_event(event):
    # Logic to send a single event to the aggregator
    pass

def send_batch_events(events):
    # Logic to send a batch of events to the aggregator
    for event in events:
        send_event(event)

def simulate_event_generation(num_events):
    # Logic to simulate event generation, including duplicates
    events = []
    for i in range(num_events):
        event = {
            "topic": "example_topic",
            "event_id": f"event_{i}",
            "timestamp": "2023-10-01T12:00:00Z",
            "source": "simulator",
            "payload": {"data": f"sample_data_{i}"}
        }
        events.append(event)
        # Optionally add duplicates
        if i % 3 == 0:  # Example condition for duplication
            events.append(event)
    return events

if __name__ == "__main__":
    # Example usage
    batch = simulate_event_generation(10)
    send_batch_events(batch)