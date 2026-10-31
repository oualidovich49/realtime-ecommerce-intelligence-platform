import unittest
import uuid
from datetime import datetime, timezone

from ingestion.src.streaming import EVENT_TYPES, make_event, validate_event


class StreamingEventTests(unittest.TestCase):
    def test_generated_event_has_supported_shape_and_source(self):
        event = make_event([1], [2])
        clean = validate_event(event)
        self.assertIn(clean["event_type"], EVENT_TYPES)
        self.assertEqual("simulated", clean["source_system"])
        self.assertTrue(clean["payload"]["simulated"])
        self.assertIsInstance(clean["event_timestamp"], datetime)

    def test_rejects_invalid_event_id(self):
        event = {"event_id": "invalid", "event_type": EVENT_TYPES[0],
                 "event_timestamp": datetime.now(timezone.utc).isoformat(), "payload": {}}
        with self.assertRaisesRegex(ValueError, "event_id"):
            validate_event(event)

    def test_rejects_timezone_free_timestamp(self):
        event = {"event_id": str(uuid.uuid4()), "event_type": EVENT_TYPES[0],
                 "event_timestamp": "2026-10-31T12:00:00", "payload": {}}
        with self.assertRaisesRegex(ValueError, "timezone"):
            validate_event(event)


if __name__ == "__main__":
    unittest.main()
