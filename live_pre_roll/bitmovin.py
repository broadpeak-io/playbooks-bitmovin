from os import path
from time import sleep
from typing import List, Tuple

import bitmovin_api_sdk as bm
import config as cfg

max_minutes_to_wait_for_live_encoding_details = 5
max_minutes_to_wait_for_encoding_status = 5

# Automatically shutdown the live stream
# if there is no input anymore for a predefined number of seconds.
bytes_read_timeout_seconds = 3600
# Automatically shutdown the live stream after a predefined runtime in minutes.
stream_timeout_minutes = 1 * 60
# How far behind real time the live edge is. Longer for more stable streams,
# lower for lower latency streams
live_edge_offset = 30
# How long the timeshift window is, ie. how far back from the live edge
# the user can rewind
timeshift_window = 300


class BitmovinController:
    def __init__(self) -> None:
        self.bitmovin_api = bm.BitmovinApi(
            api_key=cfg.BITMOVIN_API_KEY,
            tenant_org_id=getattr(cfg, "BITMOVIN_TENANT_ORG_ID", ""),
            # logger=bm.BitmovinApiLogger(),
        )

        self.encoding_api = self.bitmovin_api.encoding
        self.hls_api = self.bitmovin_api.encoding.manifests.hls

        if hasattr(cfg, "S3_OUTPUT_ID"):
            self.output = self._get_s3_output(output_id=cfg.S3_OUTPUT_ID)
        else:
            self.output = self._create_s3_output(
                bucket_name=getattr(cfg, "S3_OUTPUT_BUCKET_NAME"),
                access_key=getattr(cfg, "S3_OUTPUT_ACCESS_KEY"),
                secret_key=getattr(cfg, "S3_OUTPUT_SECRET_KEY"),
            )
            print(f"Created S3 output with id {self.output.id}")

    def encode_and_package(
        self,
        name: str,
        output_sub_path: str,
    ) -> Tuple[bm.Encoding, bm.LiveEncoding, List[bm.HlsManifest | bm.DashManifest]]:
        rtmp_input = self._get_rtmp_input()

        encoding = self._create_encoding(name=name, description="")

        # Manifest
        hls_manifest = self._generate_hls_manifest(
            output=self.output, output_path=output_sub_path
        )

        # ABR Ladder
        video_configurations = [
            self._create_h264_video_configuration(
                height=r.height,
                bitrate=r.bitrate,
                profile=bm.ProfileH264(r.profile.upper()),
                level=bm.LevelH264(r.level),
                rate=cfg.FRAME_RATE
            )
            for r in cfg.VIDEO_LADDER
        ]

        audio_configurations = [
            self._create_aac_audio_configuration(bitrate=r.bitrate)
            for r in cfg.AUDIO_LADDER
        ]

        # create video streams, muxings, dash representations and hls variant playlists
        for i, video_config in enumerate(video_configurations):
            h264_video_stream = self._create_stream(
                encoding=encoding,
                input=rtmp_input,
                input_path="live",
                codec_configuration=video_config,
            )

            relative_path_ts = f"video/{video_config.bitrate}/ts"
            ts_muxing = self._create_ts_muxing(
                encoding=encoding,
                output=self.output,
                output_path=f"{output_sub_path}/{relative_path_ts}",
                stream=h264_video_stream,
            )

            self._add_hls_variant(
                encoding=encoding,
                hls_manifest=hls_manifest,
                stream=h264_video_stream,
                ts_muxing=ts_muxing,
                relative_path=relative_path_ts,
                filename_suffix=f"{video_config.height}p_{video_config.bitrate}",
            )

        # create audio streams and muxings
        for i, audio_config in enumerate(audio_configurations):
            audio_stream = self._create_stream(
                encoding=encoding,
                input=rtmp_input,
                input_path="live",
                codec_configuration=audio_config,
            )

            relative_path_ts = f"audio/{audio_config.bitrate}/ts"
            ts_muxing = self._create_ts_muxing(
                encoding=encoding,
                output=self.output,
                output_path=f"{output_sub_path}/{relative_path_ts}",
                stream=audio_stream,
            )

            self._add_hls_media(
                encoding=encoding,
                hls_manifest=hls_manifest,
                stream=audio_stream,
                ts_muxing=ts_muxing,
                relative_path=relative_path_ts,
                filename_suffix=f"{audio_config.bitrate}",
            )

        # Setting the auto_shutdown_configuration is optional;
        # if omitted the live encoding will not shut down automatically.
        auto_shutdown_configuration = bm.LiveAutoShutdownConfiguration(
            bytes_read_timeout_seconds=bytes_read_timeout_seconds,
            stream_timeout_minutes=stream_timeout_minutes,
        )

        start_live_encoding_request = bm.StartLiveEncodingRequest(
            stream_key=cfg.RTMP_STREAM_KEY,
            auto_shutdown_configuration=auto_shutdown_configuration,
        )

        start_live_encoding_request.hls_manifests = [
            bm.LiveHlsManifest(
                manifest_id=hls_manifest.id,
                timeshift=timeshift_window,
                live_edge_offset=live_edge_offset,
                insert_program_date_time=True,
            )
        ]

        self._start_live_encoding_and_wait_until_running(
            encoding=encoding, request=start_live_encoding_request
        )

        live_encoding = self._wait_for_live_encoding_details(encoding=encoding)
        return (encoding, live_encoding, [hls_manifest])

    def determine_origin_urls(self, manifests: List[bm.HlsManifest]):
        baseurl = f"https://{self.output.bucket_name}.s3.amazonaws.com/"
        manifest_urls = []

        for manifest in manifests:
            manifest_url = path.join(
                baseurl + manifest.outputs[0].output_path + "/" + manifest.manifest_name
            )

            manifest_urls.append(manifest_url)

        return manifest_urls

    def stop_encoding(self, encoding: bm.Encoding):
        self.encoding_api.encodings.live.stop(encoding_id=encoding.id)
        self._wait_until_encoding_is_in_state(
            encoding=encoding, expected_status=bm.Status.FINISHED
        )

    def _wait_until_encoding_is_in_state(
        self, encoding: bm.Encoding, expected_status: bm.Status
    ):
        check_interval_in_seconds = 15
        max_attempts = int(
            max_minutes_to_wait_for_encoding_status * (60 / check_interval_in_seconds)
        )
        attempt = 0

        while attempt < max_attempts:
            task = self.encoding_api.encodings.status(encoding_id=encoding.id)
            if task.status is expected_status:
                return
            if task.status is bm.Status.ERROR:
                self._log_task_errors(task=task)
                raise Exception("Encoding failed")

            print(
                "Encoding status is {0}. Waiting for status {1} ({2} / {3})".format(
                    task.status.value, expected_status.value, attempt, max_attempts
                )
            )

            sleep(check_interval_in_seconds)

            attempt += 1

        raise Exception(
            "Encoding did not switch to state {0} within {1} minutes. Aborting.".format(
                expected_status.value, max_minutes_to_wait_for_encoding_status
            )
        )

    def _wait_for_live_encoding_details(self, encoding: bm.Encoding):
        timeout_interval_seconds = 5
        retries = 0
        max_retries = int(
            (60 / timeout_interval_seconds)
            * max_minutes_to_wait_for_live_encoding_details
        )

        while retries < max_retries:
            try:
                return self.encoding_api.encodings.live.get(encoding_id=encoding.id)
            except bm.BitmovinError:
                print(
                    "Failed to fetch live encoding details. "
                    "Retrying... {0} / {1}".format(retries, max_retries)
                )
                retries += 1
                sleep(timeout_interval_seconds)

        raise Exception(
            "Live encoding details could not be fetched after {0} minutes".format(
                max_minutes_to_wait_for_live_encoding_details
            )
        )

    def _start_live_encoding_and_wait_until_running(
        self, encoding: bm.Encoding, request: bm.StartLiveEncodingRequest
    ):
        self.encoding_api.encodings.live.start(
            encoding_id=encoding.id, start_live_encoding_request=request
        )
        self._wait_until_encoding_is_in_state(
            encoding=encoding, expected_status=bm.Status.RUNNING
        )

    def _create_encoding(self, name: str, description: str) -> bm.Encoding:
        encoding = bm.Encoding(name=name, description=description)

        return self.encoding_api.encodings.create(encoding=encoding)

    def _get_rtmp_input(self) -> bm.RtmpInput:
        rtmp_inputs = self.encoding_api.inputs.rtmp.list()
        return rtmp_inputs.items[0]

    def _get_s3_output(self, output_id: str) -> bm.S3Output:
        return self.encoding_api.outputs.s3.get(output_id=output_id)

    def _create_s3_output(
        self, access_key: str, secret_key: str, bucket_name: str
    ) -> bm.S3Output:
        s3_output = bm.S3Output(
            name=bucket_name,
            access_key=access_key,
            secret_key=secret_key,
            bucket_name=bucket_name,
        )

        return self.encoding_api.outputs.s3.create(s3_output=s3_output)

    def _create_h264_video_configuration(
        self,
        height: int,
        bitrate: int,
        profile: bm.ProfileH264,
        level: bm.LevelH264,
        rate: float,
    ) -> bm.H264VideoConfiguration:
        config = bm.H264VideoConfiguration(
            name="H.264 {0} {1} Mbit/s".format(height, bitrate / (1000 * 1000)),
            preset_configuration=bm.PresetConfiguration.LIVE_STANDARD,
            height=height,
            bitrate=bitrate,
            rate=rate,
            profile=profile,
            level=level,
        )

        # For correct alignment between content and ads, it's critical that the Bitmovin
        # encoding abides by the selected profile. The following code makes sure of it,
        # see https://developer.bitmovin.com/encoding/docs/h264-presets#conformance-with-h264-profiles,
        # written by the author when he was working at Bitmovin ;)

        if profile is bm.ProfileH264.BASELINE:
            config.adaptive_spatial_transform = False
            config.bframes = 0
            config.cabac = False
            config.weighted_prediction_p_frames = bm.WeightedPredictionPFrames.DISABLED

        if profile is bm.ProfileH264.MAIN:
            config.adaptive_spatial_transform = False

        if profile is bm.ProfileH264.HIGH:
            config.adaptive_spatial_transform = True

        return self.encoding_api.configurations.video.h264.create(
            h264_video_configuration=config
        )

    def _create_stream(
        self,
        encoding: bm.Encoding,
        input: bm.Input,
        input_path: str,
        codec_configuration: bm.CodecConfiguration,
    ) -> bm.Stream:
        stream_input = bm.StreamInput(
            input_id=input.id,
            input_path=input_path,
            selection_mode=bm.StreamSelectionMode.AUTO,
        )

        stream = bm.Stream(
            input_streams=[stream_input], codec_config_id=codec_configuration.id
        )

        return self.encoding_api.encodings.streams.create(
            encoding_id=encoding.id, stream=stream
        )

    def _create_ts_muxing(
        self,
        encoding: bm.Encoding,
        output: bm.Output,
        output_path: str,
        stream: bm.Stream,
    ) -> bm.TsMuxing:
        muxing = bm.TsMuxing(
            outputs=[
                self._build_encoding_output(output=output, output_path=output_path)
            ],
            segment_length=cfg.SEGMENT_DURATION,
            streams=[bm.MuxingStream(stream_id=stream.id)],
        )

        return self.encoding_api.encodings.muxings.ts.create(
            encoding_id=encoding.id, ts_muxing=muxing
        )

    def _create_aac_audio_configuration(self, bitrate: int) -> bm.AacAudioConfiguration:
        config = bm.AacAudioConfiguration(
            name="AAC {0} kbit/s".format(bitrate / 1000), bitrate=bitrate
        )

        return self.encoding_api.configurations.audio.aac.create(
            aac_audio_configuration=config
        )

    def _generate_hls_manifest(
        self, output: bm.Output, output_path: str
    ) -> bm.HlsManifest:
        hls_manifest = bm.HlsManifest(
            outputs=[self._build_encoding_output(output, output_path)],
            name="HLS/ts Manifest",
            hls_master_playlist_version=bm.HlsVersion.HLS_V6,
            hls_media_playlist_version=bm.HlsVersion.HLS_V6,
            manifest_name="stream.m3u8",
        )

        return self.hls_api.create(hls_manifest=hls_manifest)

    def _add_hls_variant(
        self,
        encoding: bm.Encoding,
        hls_manifest: bm.HlsManifest,
        stream: bm.Stream,
        ts_muxing: bm.TsMuxing,
        relative_path: str,
        filename_suffix: str,
    ) -> bm.StreamInfo:
        stream_info = bm.StreamInfo(
            audio="AUDIO",
            segment_path=relative_path,
            uri=f"video_{filename_suffix}.m3u8",
            encoding_id=encoding.id,
            stream_id=stream.id,
            muxing_id=ts_muxing.id,
            force_frame_rate_attribute=True,
            force_video_range_attribute=True,
        )

        return self.hls_api.streams.create(
            manifest_id=hls_manifest.id, stream_info=stream_info
        )

    def _add_hls_media(
        self,
        encoding: bm.Encoding,
        hls_manifest: bm.HlsManifest,
        stream: bm.Stream,
        ts_muxing: bm.TsMuxing,
        relative_path: str,
        filename_suffix: str,
    ) -> bm.AudioMediaInfo:
        media_info = bm.AudioMediaInfo(
            name=filename_suffix,
            group_id="AUDIO",
            language="en",
            segment_path=relative_path,
            uri=f"audio_{filename_suffix}.m3u8",
            encoding_id=encoding.id,
            stream_id=stream.id,
            muxing_id=ts_muxing.id,
        )

        return self.hls_api.media.audio.create(
            manifest_id=hls_manifest.id, audio_media_info=media_info
        )

    def _build_encoding_output(
        self, output: bm.Output, output_path: str
    ) -> bm.EncodingOutput:
        acl_entry = bm.AclEntry(permission=bm.AclPermission.PUBLIC_READ)

        return bm.EncodingOutput(
            output_path=self._build_absolute_path(relative_path=output_path),
            output_id=output.id,
            acl=[acl_entry],
        )

    def _build_absolute_path(self, relative_path: str) -> str:
        return path.join(cfg.S3_OUTPUT_BASE_PATH, relative_path)

    def _log_task_errors(self, task: bm.Task) -> None:
        if task is None:
            return

        filtered = [x for x in task.messages if x.type is bm.MessageType.ERROR]

        for message in filtered:
            print(message.text)
