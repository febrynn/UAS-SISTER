import os
import sys

# =====================================================
# FIX PATH AGAR PUBLISHER BISA DIIMPORT
# =====================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from publisher.src.main import EventPublisher

def test_generate_event_format():
    pub = EventPublisher("http://test")
    event = pub.generate_event(
        topic="unit",
        event_id="1",
        source="tester"
    )
    assert "topic" in event
    assert event["topic"] == "unit"

def test_payload_contains_data_dict():
    pub = EventPublisher("http://test")
    event = pub.generate_event(
        topic="unit",
        event_id="2",
        source="tester"
    )
    assert isinstance(event["payload"]["data"], dict)

