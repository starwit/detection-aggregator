import pytest

from aggregator.aggregator import Aggregator
from aggregator.aggregator import AggregatorConfig
from visionapi.sae_pb2 import SaeMessage

@pytest.fixture
def config():
    return AggregatorConfig()

@pytest.fixture
def agg(config):
    return Aggregator(config)

@pytest.fixture
def agg2(config):
    return Aggregator(config)
   
def test_aggregate_msg(agg):
    
    with open('tests/sae_message.txt', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    
    for detection in sae_msg.detections:
        print(detection)
    
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    counts = agg._timeslot_buffer.get(sae_msg.frame.timestamp_utc_ms, {})
    pass

    # Everything is in one chunk
    result = len(counts)
    expected = 1
    assert result == expected, f"Expected {expected}, but got {result}"
    
    # All detections are in the chunk
    result = list(counts.values())[0] 
    expected = len(sae_msg.detections)
    assert result == expected, f"Expected {expected}, but got {result}"
    
def test_aggregate2_msg(agg2):
    with open('tests/sae_message.txt', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)

    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    
    for detection in sae_msg.detections:
        detection.class_id = 1
    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    
    for detection in sae_msg.detections:
        detection.class_id = 3
    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    
    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms + agg2.config.chunk.time_in_ms + 1, sae_msg.detections)
    pass

    # Two different times
    result = len(agg2._timeslot_buffer)
    expected = 2
    assert result == expected, f"Expected {expected}, but got {result}"
    
    counts = agg2._timeslot_buffer.get(sae_msg.frame.timestamp_utc_ms, {})
    result = len(counts)
    expected = 3
    assert result == expected, f"Expected {expected}, but got {result}"
    
    # All detections are in the chunk
    result = list(counts.values())[0]
    expected = len(sae_msg.detections)
    assert result == expected, f"Expected {expected}, but got {result}"
    
def test_write_to_buffer(agg):
    with open('tests/sae_message.txt', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    
    # Write to buffer and check if it is stored correctly
    agg._write_to_buffer(sae_msg)
    counts = agg._timeslot_buffer.get(sae_msg.frame.timestamp_utc_ms, {})
    
    result = len(counts)
    expected = 1
    assert result == expected, f"Expected {expected}, but got {result}"
    
    # Check if the count of detections is correct
    result = list(counts.values())[0]
    expected = len(sae_msg.detections)
    assert result == expected, f"Expected {expected}, but got {result}"
    
    slot = agg.config.chunk.time_in_ms
    sae_msg.frame.timestamp_utc_ms = sae_msg.frame.timestamp_utc_ms + slot + 1
    detectionCountMessage = agg._write_to_buffer(sae_msg)
    assert detectionCountMessage is None, "Expected NO DetectionCountMessage to be returned"
    assert len(agg._timeslot_buffer) == 2, "Buffer should contain two timeslots after writing a new message"
    
    sae_msg.frame.timestamp_utc_ms = sae_msg.frame.timestamp_utc_ms + slot + 1
    detectionCountMessage = agg._write_to_buffer(sae_msg)
    assert detectionCountMessage is not None, "Expected a DetectionCountMessage to be returned"
    assert len(agg._timeslot_buffer) == 2, "Buffer should contain two timeslots after writing another message"
    
    sae_msg.frame.timestamp_utc_ms = sae_msg.frame.timestamp_utc_ms + slot + 1
    detectionCountMessage = agg._write_to_buffer(sae_msg)
    assert detectionCountMessage is not None, "Expected a DetectionCountMessage to be returned"
    assert len(agg._timeslot_buffer) == 2, "Buffer should contain two timeslots after writing another message"
    

def test_unpacked_proto(agg):
    with open('tests/sae_message.txt', 'rb') as f:
        sae_message_bytes = f.read()

    sae_msg = agg._unpack_proto(sae_message_bytes)
    
    assert isinstance(sae_msg, SaeMessage), "Unpacked message is not of type SaeMessage"
    assert len(sae_msg.detections) > 0, "No detections found in unpacked message"

def test_packed_proto(agg):
    with open('tests/sae_message.txt', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    
    agg._write_to_buffer(sae_msg)
    counts = agg._timeslot_buffer.get(sae_msg.frame.timestamp_utc_ms, {})
    
    detection_count_msg = agg._create_detectioncount_msg(sae_msg.frame.timestamp_utc_ms, counts)
    packed_bytes = agg._pack_proto(detection_count_msg)
    
    assert isinstance(packed_bytes, bytes), "Packed message is not of type bytes"
    assert len(packed_bytes) > 0, "Packed message is empty"

def test_aggregate_multiple_timeslots(agg):
    with open('tests/sae_message.txt', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    
    # Aggregate in different timeslots
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms + 1000, sae_msg.detections)
    
    result = len(agg._timeslot_buffer)
    expected = 2
    assert result == expected, f"Expected {expected}, but got {result}"