import logging
from typing import Any, Dict, NamedTuple

from prometheus_client import Counter, Histogram, Summary
from visionapi.sae_pb2 import SaeMessage, VideoFrame, Detection
from visionapi.analytics_pb2 import DetectionCountMessage, DetectionCount
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

class Aggregator:
    def __init__(self, config: AggregatorConfig) -> None:
        self.config = config
        self.__chunkHandler = ChunkHandler(config.chunk)
        self._chunkBuffer = dict[int, dict[Chunk, int]]()
        self.__buffer_size = config.chunk.buffer_size
        logger.setLevel(self.config.log_level.value)

    def __call__(self, input_proto: bytes) -> Any:
        return self.get(input_proto)
    
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes, publish) -> bytes:
        sae_msg = self._unpack_proto(input_proto)
        logger.warning('Received SAE message from pipeline')
        self._write_to_buffer(sae_msg, publish)
        return self._pack_proto(sae_msg)
    
    def _write_to_buffer(self, sae_msg: SaeMessage, publish) -> None:
        # determine appropriate timeslot
        ts_keys = list(self._chunkBuffer)
        start_ts = None
        if len(ts_keys) > 0:
            start_ts = sorted(ts_keys)[-1]
        key = self.__chunkHandler.get_ts_period_start(start_ts, sae_msg.frame.timestamp_utc_ms)
        
        # aggregate detections to chunks
        self._aggregate_msg(key, sae_msg.detections)
        
        if len(self._chunkBuffer) >= self.__buffer_size:
            logger.debug(f'Buffer size {len(self._chunkBuffer)} reached, writing to redis')
            sorted_dict = sorted(self._chunkBuffer.items())
            
            # get earliest chunk
            first_key, first_chunk_counts = sorted_dict[0]
            
            # remove from buffer
            self._chunkBuffer = sorted_dict[1:]
            dcm = self._create_detectioncount_msg(first_key, first_chunk_counts)
            
            # return earliest chunk as DetectionCountMessage
            return self._pack_proto(dcm)
        return None        

    def _create_detectioncount_msg(self, first_key, first_chunk_counts):
        dcm = DetectionCountMessage()
        dcm.timestamp_utc_ms.CopyFrom(first_key)
        dcm.detection_counts = [DetectionCount]

        for chunk in first_chunk_counts.keys():
            detection_count = dcm.detection_counts.add()
            detection_count.class_id = chunk.class_id
            detection_count.count = first_chunk_counts.get(chunk, 1)
            if chunk.geo_coordinate is not None:
                dcm.location.latitude = chunk.geo_coordinate.latitude
                dcm.location.longitude = chunk.geo_coordinate.longitude
            if chunk.x is not None and chunk.y is not None:
                dcm.location.x = chunk.x
                dcm.location.y = chunk.y
        return dcm
        
    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes: bytes) -> SaeMessage:
        with open("sae_message.bin", "wb") as f:
            f.write(sae_message_bytes)
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)
        sae_msg2 = SaeMessage()
        sae_msg2.CopyFrom(sae_msg)
        sae_msg2.frame.CopyFrom(VideoFrame())
        sae_msg2.frame.timestamp_utc_ms.CopyFrom(sae_msg.frame.timestamp_utc_ms)
        #for detection in sae_msg2.detections:
            #logger.debug(f'objectId: {detection.object_id.decode()}')
        logger.debug(f'Unpacked SAE message: {sae_msg2}')
        return sae_msg
    
    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, output_msg: DetectionCountMessage) -> bytes:
        return output_msg.SerializeToString()
    
    def _aggregate_msg(self, ts_in_ms: int, detections) -> None:
        counts = self._chunkBuffer.get(ts_in_ms, {})
        chunks = counts.keys()
        for detection in detections:
            newChunk = Chunk(ts_in_ms, detection)
            added = False
            for chunk in chunks:
                newChunk = self.__chunkHandler.aggregateChunk(chunk, newChunk)
                if (chunk == newChunk):
                    counts[chunk] = counts.get(chunk, 0) + 1
                    added = True
                    break
            if (not added):
                counts[newChunk] = 1              
        self._chunkBuffer.update({ts_in_ms: counts})
