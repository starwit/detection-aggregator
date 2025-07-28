from typing import List

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Annotated
from visionlib.pipeline.settings import LogLevel, YamlConfigSettingsSource


class RedisConfig(BaseModel):
    host: str = 'localhost'
    port: Annotated[int, Field(ge=1, le=65536)] = 6379
    stream_id: str = 'stream1'
    input_stream_prefix: str = 'objecttracker'
    output_stream_prefix: str = 'mystage'

class Coordinates(BaseModel):
    latitude: float = 10
    longitude: float = 10    
class ChunkConfig(BaseModel):
    buffer_size: int = 3
    time_in_ms: int = 1000
    use_camera_coordinates: bool = False 
    geo_coordinate: Coordinates = Coordinates()
    x: float = None
    y: float = None

class AggregatorConfig(BaseSettings):
    log_level: LogLevel = LogLevel.WARNING
    redis: RedisConfig = RedisConfig()
    chunk: ChunkConfig = ChunkConfig()
    prometheus_port: Annotated[int, Field(ge=1024, le=65536)] = 8000

    model_config = SettingsConfigDict(env_nested_delimiter='__')

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        return (init_settings, env_settings, YamlConfigSettingsSource(settings_cls), file_secret_settings)