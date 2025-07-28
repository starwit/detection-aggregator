import pytest

from aggregator.aggregator import Aggregator
from aggregator.aggregator import AggregatorConfig
from visionapi.sae_pb2 import SaeMessage
from visionapi.analytics_pb2 import DetectionCountMessage, DetectionCount

@pytest.fixture
def config():
    return AggregatorConfig()

@pytest.fixture
def agg(config):
    config.chunk.use_camera_coordinates = False
    return Aggregator(config)

@pytest.fixture
def agg2(config):
    config.chunk.use_camera_coordinates = False
    return Aggregator(config)
   
def test_aggregate_msg(agg):
    
    with open('tests/sae_message.bin', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    
    for detection in sae_msg.detections:
        print(detection)
    
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections, None)
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
    with open('tests/sae_message.bin', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)

    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections, None)
    
    for detection in sae_msg.detections:
        detection.class_id = 1
    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections, None)
    
    for detection in sae_msg.detections:
        detection.class_id = 3
    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections, None)
    
    agg2._aggregate_msg(sae_msg.frame.timestamp_utc_ms + agg2.config.chunk.time_in_ms + 1, sae_msg.detections, None)
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
    with open('tests/sae_message.bin', 'rb') as f:
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
    with open('tests/sae_message.bin', 'rb') as f:
        sae_message_bytes = f.read()

    sae_msg = agg._unpack_proto(sae_message_bytes)
    
    assert isinstance(sae_msg, SaeMessage), "Unpacked message is not of type SaeMessage"
    assert len(sae_msg.detections) > 0, "No detections found in unpacked message"

def test_packed_proto(agg):
    with open('tests/sae_message.bin', 'rb') as f:
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
    with open('tests/sae_message.bin', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    
    # Aggregate in different timeslots
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections, None)
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms + 1000, sae_msg.detections, None)
    
    result = len(agg._timeslot_buffer)
    expected = 2
    assert result == expected, f"Expected {expected}, but got {result}"

def test_get_method(agg):
    with open('tests/sae_message.bin', 'rb') as f:
        sae_message_bytes = f.read()
    
    # Call the get method with the SAE message bytes
    result = agg.get(sae_message_bytes)
    assert result is None, "Expected result to be None, but got a value"
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    first_timeslot = sae_msg.frame.timestamp_utc_ms
    
    sae_msg.frame.timestamp_utc_ms = sae_msg.frame.timestamp_utc_ms + agg.config.chunk.time_in_ms + 1
    result = agg.get(sae_msg.SerializeToString())
    assert result is None, "Expected result to be None, but got a value"
    
    sae_msg.frame.timestamp_utc_ms = sae_msg.frame.timestamp_utc_ms + agg.config.chunk.time_in_ms + 1
    result = agg.get(sae_msg.SerializeToString())
    # Check if the result is of type bytes
    assert isinstance(result, bytes), "Expected result to be of type bytes"
    
    detection_count_msg: DetectionCountMessage = DetectionCountMessage()
    detection_count_msg.ParseFromString(result)
    assert isinstance(detection_count_msg, DetectionCountMessage), "Expected result to be a DetectionCountMessage"
    
    # Check if the timestamp in the DetectionCountMessage matches the input message
    assert detection_count_msg.timestamp_utc_ms == first_timeslot, "Timestamps do not match"
    
    # Check if the detection counts are aggregated correctly
    dc = detection_count_msg.detection_counts[0]
    count = dc.count
    expected_count = len(sae_msg.detections)
    assert count == expected_count, f"Expected total count {expected_count}, but got {sum(counts.values())}"