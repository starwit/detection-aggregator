from .chunk import Chunk
from .config import ChunkConfig

class ChunkHandler:
    
    chunk_diff: ChunkConfig
    
    def __init__(self, config: ChunkConfig) -> None:
        self.chunk_diff = config
        return
            
    def aggregateChunk(self, current: Chunk, other: Chunk) -> Chunk:
        same = True
        same = same and current.class_id == other.class_id
        same = same and self.equals_time(current, other)     
        same = same and self._compareNone_(current, other)
        same = same and self._compareNone_(current.geo_coordinate, other.geo_coordinate)
        same = same and self._compareNone_(current.x, other.x)
        same = same and self._compareNone_(current.y, other.y)
        
        if (same and current.geo_coordinate is not None and other.geo_coordinate is not None):
            same = same and current.geo_coordinate.latitude <= other.geo_coordinate.latitude < current.geo_coordinate.latitude + self.chunk_diff.geo_coordinate.latitude
            same = same and current.geo_coordinate.longitude <= other.geo_coordinate.longitude < current.geo_coordinate.longitude + self.chunk_diff.geo_coordinate.longitude
        
        if (same and self.chunk_diff.x is not None and self.chunk_diff.y is not None):
            same = same and current.x <= other.x < current.x + self.chunk_diff.x
            same = same and current.y <= other.y < current.y + self.chunk_diff.y
       
        if (same):
            return current
        else:
            return other

    def _compareNone_(self, current, other):
        if (current is None and other is None):
            return True       
        elif (current is None and other is not None):
            return False
        return True
    
    def equals_time(self, current: Chunk, other: Chunk) -> bool:
        return current.time_in_ms <= other.time_in_ms < current.time_in_ms + self.chunk_diff.time_in_ms
    
    def get_ts_period_start(self, start_ts: int, other_ts: int) -> int:
        if start_ts is None or start_ts == 0:
            return other_ts
        if (start_ts.time_in_ms <= other_ts.time_in_ms < start_ts.time_in_ms + self.chunk_diff.time_in_ms):
            return start_ts
        return self.get_ts_period_start(start_ts.time_in_ms + self.chunk_diff.time_in_ms, other_ts)
