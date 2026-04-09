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
from viam.streams import StreamWithIterator
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
        previous_timestamp_ns: int,
        *,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> "AudioIn.AudioStream":
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
            "-t",
            str(duration_seconds) if duration_seconds > 0 else "5",
            "-f",
            "wav",
            "pipe:1",
        ]
        self.logger.info(
            f"Starting audio capture: {duration_seconds}s from {self.rtsp_url}"
        )

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        audio_data, stderr = await process.communicate()
        if process.returncode != 0:
            error_msg = stderr.decode()
            self.logger.error(f"FFmpeg error: {error_msg}")
            raise RuntimeError(f"Failed to capture audio: {error_msg}")

        self.logger.info(f"Captured {len(audio_data)} bytes of audio")

        chunk = AudioChunk(
            audio_data=audio_data,
            audio_info=AudioInfo(),
            sequence=0,
        )
        response = GetAudioResponse(audio=chunk)

        async def _audio_generator():
            yield response

        return StreamWithIterator(_audio_generator())

    async def get_properties(
        self, *, timeout: Optional[float] = None, **kwargs
    ) -> AudioIn.Properties:
        return GetPropertiesResponse()

    async def do_command(
        self,
        command: Mapping[str, ValueTypes],
        *,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> Mapping[str, ValueTypes]:
        cmd_name = command.get("command", "")

        # While a user can set the threshold in the config, it allows for changing the threshold doing run time.
        # Don't forget to update the config if a better threshold is determined.
        if cmd_name == "set_threshold":
            new_threshold = command.get("threshold", self.sound_threshold)
            self.sound_threshold = float(str(new_threshold))
            return {"threshold": self.sound_threshold}

        return {"error": f"Unknown command: {cmd_name}. Available: set_threshold"}

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

    async def get_geometries(
        self, *, extra: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
    ) -> Sequence[Geometry]:
        self.logger.error("`get_geometries` is not implemented")
        raise NotImplementedError()
