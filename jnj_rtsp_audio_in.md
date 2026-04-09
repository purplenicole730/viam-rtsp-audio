# Model jnj:rtsp:audio_in

This model is a Viam AudioIn component that connects to an RTSP camera's audio stream via FFMpeg over TCP. It continuously monitors the audio every second and reports sound detection when it goes over a configurable threshold.

## Configuration

The following attribute template can be used to configure this model:

```json
{
"rtsp_url": <string>,
}
```

### Attributes

The following attributes are available for this model:

| Name              | Type   | Inclusion    | Description                                                                                                       |
| ----------------- | ------ | ------------ | ----------------------------------------------------------------------------------------------------------------- |
| `rtsp_url`        | string | **Required** | The RTSP URL of the camera to capture audio from. Example: `rtsp://admin:password@192.168.1.100:554/stream1`      |
| `sample_rate`     | int    | Optional     | Audio sample rate in Hz (samples per second). Higher = better quality but more data. Defaults to 16000.           |
| `channels`        | int    | Optional     | Number of audio channels. 1 = mono, 2 = stereo. Defaults to 1                                                     |
| `volume_boost`    | float  | Optional     | Multiplier for audio volume. Increase if audio is too quiet, decrease if it's clipping/distorted. Default to 10.0 |
| `sound_threshold` | float  | Optional     | The RMS (volume level) above which sound will be detected. Default to 500.0                                       |

### Example Configuration

```json
{
  "rtsp_url": "rtsp://admin:password@192.168.1.100:554/stream1",
  "volume_boost": 5.0,
  "sound_threshold": 200.0
}
```

## DoCommand

Adjust the sound detection threshold at runtime without restarting the module. The changes are temporary and will revert to the configured value on restart. If a better threshold was found, then make sure to update the configuration.

### Example DoCommand

```json
{
  "command": "set_threshold",
  "threshold": 800.0
}
```

Response:

```json
{
  "threshold": 800.0
}
```
