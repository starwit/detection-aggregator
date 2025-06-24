import pytest

from aggregator.aggregator import Aggregator
from aggregator.aggregator import AggregatorConfig


@pytest.fixture
def config():
    return AggregatorConfig()

@pytest.fixture
def agg(config):
    return Aggregator(config)
   
def test_getSomething(agg):
    # Add your test logic here
    pass
    # Example test logic
    result = "expected_result"  # Replace with actual function to test
    expected = "expected_result"    # Replace with actual expected result
    assert result == expected, f"Expected {expected}, but got {result}"
    expected2 = "test"
    result2 = agg.getSomething()
    assert result2 == expected2, f"Expected {expected2}, but got {result2}"