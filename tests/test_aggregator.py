import json
import pytest

from aggregator.aggregator import Aggregator
from aggregator.aggregator import AggregatorConfig
from visionapi.sae_pb2 import SaeMessage
from google.protobuf.json_format import Parse
from visionapi.analytics_pb2 import DetectionCountMessage

@pytest.fixture
def config():
    cfg = AggregatorConfig()
    cfg.chunk.buffer_size = 3
    cfg.chunk.time_in_ms = 20000
    cfg.skip_empty_detections = False
    cfg.chunk.geo_coordinate.latitude = 10
    cfg.chunk.geo_coordinate.longitude = 10
    return cfg

@pytest.fixture
def config_no_agg():
    cfg = AggregatorConfig()
    cfg.chunk.buffer_size = 1
    cfg.chunk.time_in_ms = 1
    cfg.skip_empty_detections = False
    cfg.chunk.geo_coordinate.latitude = 0
    cfg.chunk.geo_coordinate.longitude = 0
    return cfg

@pytest.fixture
def agg(config):
    return Aggregator(config)

@pytest.fixture
def agg_no_agg(config_no_agg):
    return Aggregator(config_no_agg)
   
def test_aggregate_msg(agg):
    
    with open('tests/sae_message.bin', 'rb') as f:
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
    
def test_aggregate2_msg(agg):
    with open('tests/sae_message.bin', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)

    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    
    for detection in sae_msg.detections:
        detection.class_id = 1
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    
    for detection in sae_msg.detections:
        detection.class_id = 3
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms + agg.config.chunk.time_in_ms + 1, sae_msg.detections)
    pass

    # Two different times
    result = len(agg._timeslot_buffer)
    expected = 2
    assert result == expected, f"Expected {expected}, but got {result}"
    
    counts = agg._timeslot_buffer.get(sae_msg.frame.timestamp_utc_ms, {})
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
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms, sae_msg.detections)
    agg._aggregate_msg(sae_msg.frame.timestamp_utc_ms + 1000, sae_msg.detections)
    
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
    assert count == expected_count, f"Expected total count {expected_count}, but got {count}"
    
def test_sae_message_detections(agg_no_agg):
    with open('tests/sae_message_detections.json', 'rb') as f:
        sae_message = f.read()   
        
    json_obj = json.loads(sae_message)
    sae_msg: SaeMessage = Parse(sae_message, SaeMessage())
    assert isinstance(sae_msg, SaeMessage), "Parsed message is not of type SaeMessage"
    assert len(sae_msg.detections) > 0, "No detections found in parsed message"
    for detection in sae_msg.detections:
        print(detection)
    msg: DetectionCountMessage = DetectionCountMessage()
    msg.ParseFromString(agg_no_agg._write_to_buffer(sae_msg))
    assert msg is not None, "Expected DetectionCountMessage to be returned"
    
    count = msg.detection_counts[0].count
    expected_count = len(sae_msg.detections)
    assert count == expected_count, f"Expected total count {expected_count}, but got {count}"
    