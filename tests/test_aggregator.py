import pytest
import json
import sys

from aggregator.aggregator import Aggregator
from aggregator.aggregator import AggregatorConfig
from visionapi.sae_pb2 import SaeMessage

@pytest.fixture
def config():
    return AggregatorConfig()

@pytest.fixture
def agg(config):
    return Aggregator(config)
   
def test_getSomething(agg):
    # Add your test logic here

    with open('tests/test.json') as json_file:
        json_data = json.load(json_file)
        
    json_str = json.dumps(json_data)
    #print(json_str)
    
    with open('tests/sae_message.txt', 'rb') as f:
        sae_message_bytes = f.read()
    
    sae_msg: SaeMessage = SaeMessage()
    sae_msg.ParseFromString(sae_message_bytes)
    for detection in sae_msg.detections:
        print(detection)
    agg.aggregate_msg(sae_msg)
    pass
    # Example test logic
    result = "expected_result"  # Replace with actual function to test
    expected = "expected_result"    # Replace with actual expected result
    assert result == expected, f"Expected {expected}, but got {result}"