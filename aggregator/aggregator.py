import logging
from typing import Any

from prometheus_client import Counter, Histogram, Summary
from visionapi.sae_pb2 import SaeMessage
from visionapi.analytics_pb2 import DetectionCountMessage
from .chunk import Chunk
from .chunkHandler import ChunkHandler

from .config import AggregatorConfig

logging.basicConfig(format='%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s')
logger = logging.getLogger(__name__)

GET_DURATION = Histogram('aggregator_get_duration', 'The time it takes to deserialize the proto until returning the tranformed result as a serialized proto',
                         buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
OBJECT_COUNTER = Counter('aggregator_object_counter', 'How many detections have been transformed')
PROTO_SERIALIZATION_DURATION = Summary('aggregator_proto_serialization_duration', 'The time it takes to create a serialized output proto')
PROTO_DESERIALIZATION_DURATION = Summary('aggregator_proto_deserialization_duration', 'The time it takes to deserialize an input proto')

"""
Aggregator class for processing and aggregating detection messages.

This class is responsible for handling incoming SAE messages, 
aggregating detections over specified time slots. It provides methods to 
unpack incoming messages, write to a buffer, and create 
detection count messages for further processing.

Attributes:
    config (AggregatorConfig): Configuration settings for the aggregator.
    _chunk_handler (ChunkHandler): Handler for managing chunk operations.
    _timeslot_buffer (dict[int, dict[Chunk, int]]): Buffer for storing 
        chunks of detections indexed by time slots.
    _buffer_size (int): Maximum size of the timeslot buffer.
"""
    # ... rest of the class code ...
class Aggregator:
    def __init__(self, config: AggregatorConfig) -> None:
        self.config = config
        self._chunk_handler = ChunkHandler(config.chunk)
        self._timeslot_buffer = dict[int, dict[Chunk, int]]()
        self._buffer_size = config.chunk.buffer_size
        logger.setLevel(self.config.log_level.value)

    def __call__(self, input_proto: bytes) -> Any:
        return self.get(input_proto)
    
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes) -> bytes:
        sae_msg = self._unpack_proto(input_proto)
        logger.debug('Received SAE message from pipeline')
        if (sae_msg is None or 
            sae_msg.detections is None or 
            len(sae_msg.detections) == 0):
            logger.debug('No detections in SAE message, skipping')
            return None
        return self._write_to_buffer(sae_msg)
    
    
    """
    Writes the given SAE message to a buffer, aggregates detections, 
    and returns the earliest chunk as a DetectionCountMessage if the 
    buffer size limit is reached.

    Args:
        sae_msg (SaeMessage): The SAE message containing detection data 
                                and a timestamp.

    Returns:
        DetectionCountMessage or None: Returns the earliest chunk as a 
                                        DetectionCountMessage if the 
                                        buffer size limit is reached, 
                                        otherwise returns None.
    """
    def _write_to_buffer(self, sae_msg: SaeMessage) -> DetectionCountMessage | None:
        # determine appropriate timeslot
        ts_keys = list(self._timeslot_buffer)
        start_ts = None
        if len(ts_keys) > 0:
            start_ts = sorted(ts_keys)[-1]
        key = self._chunk_handler.get_ts_period_start(start_ts, sae_msg.frame.timestamp_utc_ms)
        
        # aggregate detections to chunks
        self._aggregate_msg(key, sae_msg.detections)
        
        if len(self._timeslot_buffer) >= self._buffer_size:
            logger.debug(f'Buffer size {len(self._timeslot_buffer)} reached, writing to redis')
            sorted_dict = sorted(self._timeslot_buffer.items())
            
            # get earliest chunk
            first_timeslot, first_chunk_counts = sorted_dict[0]
            
            # remove from buffer
            self._timeslot_buffer.pop(first_timeslot, None)
            dcm = self._create_detectioncount_msg(first_timeslot, first_chunk_counts)
            
            # return earliest chunk as DetectionCountMessage
            return self._pack_proto(dcm)
        return None        

    def _create_detectioncount_msg(self, timeslot, first_chunk_counts):
        dcm = DetectionCountMessage()
        dcm.timestamp_utc_ms = timeslot

        for chunk in first_chunk_counts.keys():
            detection_count = dcm.detection_counts.add()
            detection_count.class_id = chunk.class_id
            detection_count.count = first_chunk_counts.get(chunk, 1)
            if chunk.geo_coordinate is not None:
                detection_count.location.latitude = chunk.geo_coordinate.latitude
                detection_count.location.longitude = chunk.geo_coordinate.longitude
            #if chunk.x is not None and chunk.y is not None:
            #    detection_count.location.latitude = chunk.x
            #    detection_count.location.longitude = chunk.y
        return dcm
        
    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes: bytes) -> SaeMessage:
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)
        #logger.debug(f'Unpacked SAE message: {sae_msg}')
        return sae_msg
    
    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, output_msg: DetectionCountMessage) -> bytes:
        return output_msg.SerializeToString()
    
    """
    Aggregates detection messages for a given timeslot.

    This method processes a list of detection messages and updates the 
    internal timeslot buffer with the aggregated counts of each detection 
    chunk. If a chunk already exists for the given timeslot, its count 
    is incremented; otherwise, a new chunk is created and added to the 
    buffer.

    Args:
        ts_in_ms (int): The timeslot in milliseconds for which the 
                        detections are being aggregated.
        detections: A list of detection messages to be aggregated in chunks.

    Returns:
        None: This method updates the internal state and does not return 
                any value.
    """
    def _aggregate_msg(self, ts_in_ms: int, detections) -> None:
            counts = self._timeslot_buffer.get(ts_in_ms, {})
            chunks = counts.keys()
            for detection in detections:
                newChunk = Chunk(ts_in_ms, detection)
                added = False
                for chunk in chunks:
                    newChunk = self._chunk_handler.aggregateChunk(chunk, newChunk)
                    if (chunk == newChunk):
                        counts[chunk] = counts.get(chunk, 0) + 1
                        added = True
                        break
                if (not added):
                    counts[newChunk] = 1              
            self._timeslot_buffer.update({ts_in_ms: counts})
