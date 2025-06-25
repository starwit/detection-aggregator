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
        self.__chunkBuffer = dict[int, dict[Chunk, int]]()
        self.__buffer_size = self.config.chunk.buffer_size
        logger.setLevel(self.config.log_level.value)

    def __call__(self, input_proto: bytes) -> Any:
        return self.get(input_proto)
    
    def __write_to_buffer(self, sae_msg: SaeMessage) -> None:
        ts_keys = list(self.__chunkBuffer)
        if len(ts_keys) > 0:
            start_ts = ts_keys[0]  
        key = self.__chunkHandler.get_ts_period_start(start_ts, sae_msg.frame.timestamp_utc_ms)
        if (key in self.__chunkBuffer):
            self.aggregate_msg(key, sae_msg.detections)
        else :
            if len(self.__chunkBuffer) >= self.__buffer_size:
                logger.debug(f'Buffer size {len(self.__chunkBuffer)} reached, writing to redis')
                # Here you would write the buffer to Redis or any other storage
                # send first message and remove from buffer
                # create new buffer entry
                self.__chunkBuffer.clear()
    
    def getSomething(self):
        return "test"
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes):
        sae_msg = self._unpack_proto(input_proto)

        # Your implementation goes (mostly) here
        logger.warning('Received SAE message from pipeline')

        return self._pack_proto(sae_msg)
        
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
    def _pack_proto(self, sae_msg: SaeMessage) -> bytes:
        output_msg = self._create_decision_msg(sae_msg)
        return output_msg.SerializeToString()
    
    def aggregate_msg(self, ts_in_ms: int, detections: Any[Detection]) -> dict[Chunk, int]:
        counts = self.__chunkBuffer.get(ts_in_ms, {})
        chunks = counts.values()
        detcount = len(detections)
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
                chunks.add(newChunk)
                counts[newChunk] = 1              
            print(counts)
        self.__chunkBuffer.update({ts_in_ms: counts})
        return counts
    
    def _create_decision_msg(self, sae_msg: SaeMessage) -> DetectionCountMessage:
        output_msg = DetectionCountMessage()
        #output_msg.timestamp_utc_ms.CopyFrom(sae_msg.timestamp_utc_ms)

        return output_msg