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
    counts = agg._chunkBuffer.get(sae_msg.frame.timestamp_utc_ms, {})
    pass

    # Everything is in one chunk
    result = len(counts) # Replace with actual function to test
    expected = 1    # Replace with actual expected result
    assert result == expected, f"Expected {expected}, but got {result}"
    
    # All detections are in the chunk
    result = list(counts.values())[0] # Replace with actual function to test
    expected = len(sae_msg.detections)    # Replace with actual expected result
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
    result = len(agg2._chunkBuffer) # Replace with actual function to test
    expected = 2    # Replace with actual expected result
    assert result == expected, f"Expected {expected}, but got {result}"
    
    counts = agg2._chunkBuffer.get(sae_msg.frame.timestamp_utc_ms, {})
    result = len(counts) # Replace with actual function to test
    expected = 3    # Replace with actual expected result
    assert result == expected, f"Expected {expected}, but got {result}"
    
    # All detections are in the chunk
    result = list(counts.values())[0] # Replace with actual function to test
    expected = len(sae_msg.detections)    # Replace with actual expected result
    assert result == expected, f"Expected {expected}, but got {result}"