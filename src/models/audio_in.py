import asyncio
import numpy as np
import time
from typing import Any, ClassVar, Dict, Mapping, Optional, Sequence, Tuple

from typing_extensions import Self
from viam.components.audio_in import *
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import Geometry, GetPropertiesResponse, ResourceName
from viam.proto.component.audioin import AudioChunk, GetAudioResponse
from viam.resource.base import ResourceBase
from viam.resource.easy_resource import EasyResource
from viam.resource.types import Model, ModelFamily
from viam.streams import Stream, StreamWithIterator
from viam.utils import ValueTypes


class AudioIn(AudioIn, EasyResource):
    # To enable debug-level logging, either run viam-server with the --debug option,
    # or configure your resource/machine to display debug logs.
    MODEL: ClassVar[Model] = Model(ModelFamily("jnj", "rtsp"), "audio_in")

    rtsp_url: str
    sample_rate: int
    channels: int
    volume_boost: float
    sound_threshold: float

    _monitor_task: Optional[asyncio.Task] = None
    _ffmpeg_process: Optional[asyncio.subprocess.Process] = None
    _sound_detected: bool = False
    _current_rms: float = 0.0
    _peak: float = 0.0
    _last_sound_time: float = 0.0
    _monitoring: bool = False

    @classmethod
    def new(
        cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ) -> Self:
        """This method creates a new instance of this AudioIn component.
        The default implementation sets the name from the `config` parameter.

        Args:
            config (ComponentConfig): The configuration for this resource
            dependencies (Mapping[ResourceName, ResourceBase]): The dependencies (both required and optional)

        Returns:
            Self: The resource
        """
        instance = super().new(config, dependencies)
        attrs = config.attributes.fields
        instance.rtsp_url = attrs["rtsp_url"].string_value
        instance.sample_rate = (
            int(attrs["sample_rate"].number_value) if "sample_rate" in attrs else 16000
        )
        instance.channels = (
            int(attrs["channels"].number_value) if "channels" in attrs else 1
        )
        instance.volume_boost = (
            attrs["volume_boost"].number_value if "volume_boost" in attrs else 10.0
        )
        instance.sound_threshold = (
            attrs["sound_threshold"].number_value
            if "sound_threshold" in attrs
            else 500.0
        )

        instance._monitor_task = asyncio.ensure_future(instance._start_monitoring())
        return instance

    @classmethod
    def validate_config(
        cls, config: ComponentConfig
    ) -> Tuple[Sequence[str], Sequence[str]]:
        """This method allows you to validate the configuration object received from the machine,
        as well as to return any required dependencies or optional dependencies based on that `config`.

        Args:
            config (ComponentConfig): The configuration for this resource

        Returns:
            Tuple[Sequence[str], Sequence[str]]: A tuple where the
                first element is a list of required dependencies and the
                second element is a list of optional dependencies
        """
        attrs = config.attributes.fields
        if "rtsp_url" not in attrs or not attrs["rtsp_url"].string_value:
            raise ValueError(
                "'rtsp_url' is required as a string in the component config attributes"
            )
        return [], []

    async def _start_monitoring(self):
        """Background task: continuously stream audio from RTSP and analyze volume."""
        self._monitoring = True
        self.logger.info(f"Starting continuous audio monitoring from {self.rtsp_url}")

        while self._monitoring:
            try:
                cmd = [
                    "ffmpeg",
                    "-rtsp_transport",
                    "tcp",
                    "-i",
                    self.rtsp_url,
                    "-vn",
                    "-acodec",
                    "pcm_s16le",
                    "-ar",
                    str(self.sample_rate),
                    "-ac",
                    str(self.channels),
                    "-filter:a",
                    f"volume={self.volume_boost}",
                    "-f",
                    "s16le",
                    "pipe:1",
                ]

                self._ffmpeg_process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                chunk_size = self.sample_rate * self.channels * 2

                self.logger.info("FFmpeg connected. Reading audio chunks...")

                while self._monitoring:
                    if self._ffmpeg_process.stdout is None:
                        self.logger.error("Ffmpeg stdout is None")
                        break
                    audio_data = await self._ffmpeg_process.stdout.read(chunk_size)

                    if not audio_data:
                        self.logger.warning("FFmpeg stream ended. Reconnecting...")
                        break

                    samples = np.frombuffer(audio_data, dtype=np.int16)
                    if len(samples) == 0:
                        continue

                    rms = float(np.sqrt(np.mean(samples.astype(np.float64) ** 2)))
                    peak = float(np.max(np.abs(samples)))

                    self._current_rms = rms
                    self._peak = peak

                    was_detected = self._sound_detected
                    self._sound_detected = rms > self.sound_threshold

                    if self._sound_detected:
                        self._last_sound_time = time.time()
                        if not was_detected:
                            self.logger.info(
                                f"Sound detected! RMS={rms:.1f}, peak={peak:.1f}"
                            )
                    elif was_detected:
                        self.logger.info(f"Sound stopped. RMS={rms:.1f}")

            except asyncio.CancelledError:
                self.logger.info("Audio monitoring cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Audio monitoring error: {e}")
                await asyncio.sleep(5)

        await self._stop_ffmpeg()
        self.logger.info("Audio monitoring stopped.")

    async def _stop_ffmpeg(self):
        """Kill the FFmpeg process if running."""
        if self._ffmpeg_process and self._ffmpeg_process.returncode is None:
            self._ffmpeg_process.kill()
            await self._ffmpeg_process.wait()
            self._ffmpeg_process = None

    async def close(self):
        """Called when the component is shutting down."""
        self._monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        await self._stop_ffmpeg()

    async def get_audio(
        self,
        codec: str,
        duration_seconds: float,
        # previous_timestamp_ns is purely only for timestamp continuity. It deson't call past audio
        previous_timestamp_ns: int,
        *,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> "AudioIn.AudioStream":
        # Map requested codec to FFmpeg encoder and output format
        # Currently only PCM16 is supported; other codecs could be added here
        ffmpeg_codec = "pcm_s16le"
        output_format = "s16le"
        codec_name = codec if codec else "pcm16"

        cmd = [
            "ffmpeg",
            "-rtsp_transport",
            "tcp",
            "-i",
            self.rtsp_url,
            "-vn",
            "-acodec",
            ffmpeg_codec,
            "-ar",
            str(self.sample_rate),
            "-ac",
            str(self.channels),
            "-filter:a",
            f"volume={self.volume_boost}",
            "-f",
            output_format,
            "pipe:1",
        ]

        # Only add -t if a finite duration is requested; 0 means stream indefinitely
        if duration_seconds > 0:
            cmd.insert(-2, "-t")
            cmd.insert(-2, str(duration_seconds))

        self.logger.info(
            f"Starting audio stream: codec={codec_name}, duration={duration_seconds}s "
            f"from {self.rtsp_url}"
        )

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # 1 second of audio = sample_rate * channels * 2 bytes (16-bit PCM)
        chunk_size = self.sample_rate * self.channels * 2
        # Use previous_timestamp_ns for recording continuity; fall back to current time
        start_time_ns = (
            previous_timestamp_ns if previous_timestamp_ns > 0 else time.time_ns()
        )
        sample_rate = self.sample_rate
        channels = self.channels

        async def _audio_stream_generator():
            sequence = 0
            try:
                while True:
                    if process.stdout is None:
                        break

                    audio_data = await process.stdout.read(chunk_size)
                    if not audio_data:
                        break  # FFmpeg finished or stream ended

                    # Calculate timestamps based on actual bytes read
                    bytes_per_second = sample_rate * channels * 2
                    chunk_duration_ns = int(
                        (len(audio_data) / bytes_per_second) * 1_000_000_000
                    )
                    chunk_start_ns = start_time_ns + (sequence * 1_000_000_000)
                    chunk_end_ns = chunk_start_ns + chunk_duration_ns

                    chunk = AudioChunk(
                        audio_data=audio_data,
                        audio_info=AudioInfo(
                            codec=codec_name,
                            sample_rate_hz=sample_rate,
                            num_channels=channels,
                        ),
                        start_timestamp_nanoseconds=chunk_start_ns,
                        end_timestamp_nanoseconds=chunk_end_ns,
                        sequence=sequence,
                    )
                    yield GetAudioResponse(audio=chunk)
                    sequence += 1

            finally:
                # Clean up FFmpeg when the stream ends or client disconnects
                if process.returncode is None:
                    process.kill()
                    await process.wait()

        return StreamWithIterator(_audio_stream_generator())

    async def get_properties(
        self, *, timeout: Optional[float] = None, **kwargs
    ) -> AudioIn.Properties:
        return GetPropertiesResponse()

    async def get_status(
        self, *, timeout: Optional[float] = None, **kwargs
    ) -> Mapping[str, ValueTypes]:
        seconds_since_sound = (
            time.time() - self._last_sound_time if self._last_sound_time > 0 else -1
        )
        return {
            "sound_detected": self._sound_detected,
            "rms": self._current_rms,
            "peak": self._peak,
            "threshold": self.sound_threshold,
            "last_sound_seconds_ago": round(seconds_since_sound, 1),
            "monitoring": self._monitoring,
        }

    async def do_command(
        self,
        command: Mapping[str, ValueTypes],
        *,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> Mapping[str, ValueTypes]:
        cmd_name = command.get("command", "")

        if cmd_name == "get_status":
            return await self.get_status(timeout=timeout)

        # While a user can set the threshold in the config, this allows changing it during run time.
        # Don't forget to update the config if a better threshold is determined.
        if cmd_name == "set_threshold":
            new_threshold = command.get("threshold", self.sound_threshold)
            self.sound_threshold = float(str(new_threshold))
            return {"threshold": self.sound_threshold}

        return {
            "error": f"Unknown command: {cmd_name}. Available: get_status, set_threshold"
        }

    async def get_geometries(
        self, *, extra: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
    ) -> Sequence[Geometry]:
        self.logger.error("`get_geometries` is not implemented")
        raise NotImplementedError()
