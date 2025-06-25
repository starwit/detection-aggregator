from visionapi.sae_pb2 import Detection
from visionapi.common_pb2 import GeoCoordinate

class Chunk:
    geo_coordinate: GeoCoordinate
    x: float = 0.0
    y: float = 0.0
    time_in_ms: int = 0
    class_id: int = 0
    
    def __init__(self, time_in_ms, detection: Detection) -> None:
        self.time_in_ms = time_in_ms
        self.geo_coordinate = GeoCoordinate()
            
        if detection is not None:
            self.geo_coordinate.CopyFrom(detection.geo_coordinate)
            self.class_id = detection.class_id
            if (detection.bounding_box is not None):
                self.x = detection.bounding_box.min_x
                self.y = detection.bounding_box.max_y

